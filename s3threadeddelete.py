#!/usr/bin/env python3
"""
This module bulk deletes of set of documents from s3 using multiple threads
for performance.
"""

import time
import argparse
import logging
import random
import sys
import multiprocessing
import queue
import threading
import boto3
import tqdm

__version__ = '0.1'
_LOG_LEVEL_STRINGS = ['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG']


def _log_level_string_to_int(log_level_string):
    """Convert log-level arg to logging.level."""
    if not log_level_string in _LOG_LEVEL_STRINGS:
        message = 'invalid choice: {0} (choose from {1})'.format(
            log_level_string, _LOG_LEVEL_STRINGS)
        raise argparse.ArgumentTypeError(message)

    log_level_int = getattr(logging, log_level_string, logging.INFO)
    # check the logging log_level_choices have not changed from our expected values
    assert isinstance(log_level_int, int)

    return log_level_int


def get_args():
    """Fetch our command line arguments"""

    parser = argparse.ArgumentParser(
        description='''Delete a set of objects from an s3 bucket.
                        Objects to be deleted are supplied in a line delimited file''',
        formatter_class=lambda prog: argparse.HelpFormatter(prog, max_help_position=27))
    parser.add_argument('--filepath',
                        help='Path to text file containing line-delimited set of object keys to delete',
                        required=True)
    parser.add_argument('--s3bucket',
                        help='S3 bucket name to delete from', required=True)
    parser.add_argument('--dryrun',
                        help="Don't delete.  Print what we would have deleted",
                        action='store_true')
    parser.add_argument('--loglevel',
                        default='INFO',
                        dest='log_level',
                        type=_log_level_string_to_int,
                        nargs='?',
                        help='Set the logging output level. {0}'.format(_LOG_LEVEL_STRINGS))
    parser.add_argument('--batchsize',
                        help='# of keys to batch delete (default 1000)',
                        type=int, default=1000)
    parser.add_argument('--maxqueue',
                        help='Max size of deletion queue (default 10k)',
                        type=int, default=10000)
    parser.add_argument('--deleterthreads',
                        help='Number of deleter threads (default 10)',
                        dest='deleter_threads',
                        type=int, default=10)

    return parser.parse_args()


# Set up our logging object
def logger_setup(log_level):
    """Configures logger and sets loglevels."""

    # Log configuration
    logging.basicConfig(
        filename='log.txt',
        level=log_level,
        format='%(asctime)s %(levelname)s [%(thread)d] %(name)s: %(message)s',
        datefmt='%Y-%m-%d@%H:%M:%S',
    )

    # Create logger and point it at our log file
    global logger
    logger = logging.getLogger()

    # Make the logger emit all unhandled exceptions
    sys.excepthook = lambda t, v, x: logger.error(
        'Uncaught exception', exc_info=(t, v, x))

    # Supress boto debug logging, since it is very chatty
    logging.getLogger('boto3').setLevel(logging.WARNING)
    logging.getLogger('botocore').setLevel(logging.WARNING)


def deleter(args, to_delete_queue, deleted_queue, cancellation_token):
    """
    Worker function that takes keys from the to_delete_queue and constructs
    batch requests to s3 to delete those keys.  All deleted keys are added to the
    deleted_queue so they can be logged as processed.

    Note: This function is intended to be the target of 1 or more Threads.
    """
    logger.debug('%s started', threading.current_thread().name)

    # Set up per-thread boto objects
    session = boto3.session.Session()
    s3client = session.client('s3')

    keys_to_delete = []

    while not cancellation_token.is_set():
        # Snatch a key off our deletion queue and add it
        # to our local deletion list
        # if no item within a short time then loop again.  This ensures that our thread stays responsive to cancellation_token
        try:
            key = to_delete_queue.get(timeout=1)
            # check for our sentinel value indicating we have reached the end of the queue
            if key is None:
                logger.info('Reached end of queue.  Finishing processing items.')
                break
        except queue.Empty:
            continue

        keys_to_delete.append({'Key': key})

        # remove item from queue
        to_delete_queue.task_done()

        # if we have accumulated enough keys in this
        # thread's delete list to justify a batch delete
        if len(keys_to_delete) >= args.batchsize:
            _delete_batch(args, keys_to_delete, s3client, deleted_queue)

            # reset the key list
            keys_to_delete = []

    # Delete any remaining batches of items
    if keys_to_delete:
        _delete_batch(args, keys_to_delete, s3client, deleted_queue)

    logger.debug('%s finished', threading.current_thread().name)


def _delete_batch(args, keys_to_delete, s3client, deleted_queue):
    """Deletes a batch of keys from s3"""
    if args.dryrun:
        time.sleep(random.random())

        if logger.isEnabledFor(logging.DEBUG):
            for key in keys_to_delete:
                logger.debug("Would have deleted '%s'", key)
    else:
        logger.debug('Deleting %s keys from s3', len(keys_to_delete))
        try:
            response = s3client.delete_objects(
                Bucket=args.s3bucket,
                Delete={
                    'Objects': keys_to_delete
                },
            )

            deleted_count = 0
            if 'Deleted' in response:
                deleted_count = len(response['Deleted'])

            errored_count = 0
            if 'Errors' in response:
                errored_count = len(response['Errors'])
                logger.warning(response['Errors'])

            logger.debug('Deleted: %i, Errored: %i', deleted_count, errored_count)
  
        except:
            logger.exception('Error deleting objects')
            return

    # update the count of deleted keys
    with deleted_keys.get_lock():
        deleted_keys.value += len(keys_to_delete)

    # Track which keys have been successfully deleted
    for key in keys_to_delete:
        deleted_queue.put(key['Key'])

    # log some progress info
    if random.randint(0, args.deleter_threads) == args.deleter_threads:
        logger.info('Deleted %i out of %i keys found thus far.',
                    deleted_keys.value, queued_keys.value)


def key_loader(filepath, to_delete_queue, sentinel_count):
    """
    Worker function to iterate through object key file and append items to queue.

    Note: this is intended to be called from a Thread.
    """

    """
    Note:
    This should prevent us from loading the entire key set into memory at once as
    there may millions of keys.
    """

    logger.debug('%s started', threading.current_thread().name)
    with open(filepath, 'r') as object_key_file:
        for line in object_key_file:
            to_delete_queue.put(line.rstrip())
            with queued_keys.get_lock():
                queued_keys.value += 1

    # Add sentinel item to queue to indicate we are finished adding items
    #  create 1 sentinel for every deleter thread
    for _ in range(0, sentinel_count):
        to_delete_queue.put(None)

    logger.info('Loaded %i keys', queued_keys.value)
    logger.debug('%s finished', threading.current_thread().name)


def processed_writer(dest_filename, deleted_queue, cancellation_token):
    """
    Worker function to write each processed key to an output file.

    Note: this is intended to be called from a Thread.
    """

    logger.debug('%s started', threading.current_thread().name)

    with open(dest_filename, 'w') as dest_file:
        while not cancellation_token.is_set():
            try:
                key = deleted_queue.get(timeout=1)
            except queue.Empty:
                # if no item within 10s then loop again.  This ensures that our thread stays responsive to cancellation_token
                continue

            dest_file.write(key+'\n')
            deleted_queue.task_done()

    logger.debug('%s finished', threading.current_thread().name)


def progress_updater(key_count, cancellation_token):
    """
    Worker function to update the progress bar.

    Note: this is intended to be called from a Thread.
    """

    logger.debug('%s started', threading.current_thread().name)
    last_value = 0
    with tqdm.tqdm(total=key_count, unit='keys') as pbar:
        while not cancellation_token.is_set():
            pbar.update(deleted_keys.value-last_value)
            last_value = deleted_keys.value
            time.sleep(.1)

        pbar.close()

    logger.debug({'key_count': key_count,
                  'deleted_keys': deleted_keys.value, 'last_value': last_value})
    logger.debug('%s finished', threading.current_thread().name)


def key_file_len(filepath):
    """Quickly iterates the input file to get a count of keys to be processed."""

    with open(filepath) as key_file:
        i = None
        for i, _ in enumerate(key_file):
            pass
    return i + 1


# Our main function
def main():
    """Our main function"""

    # Parse arguments
    args = get_args()

    # Set up the logging object
    logger_setup(args.log_level)

    # print parsed args
    logger.info('#'*25)
    start_msg = 'Starting batch_delete'
    logger.info('#'*len(start_msg))
    logger.info(start_msg)
    logger.info('program args:')
    for arg in vars(args):
        arg_msg = '{0}: {1}'.format(arg, getattr(args, arg))
        print(arg_msg)
        logger.info(arg_msg)
    logger.info('#'*len(start_msg))

    key_count = key_file_len(args.filepath)

    to_delete_queue = multiprocessing.JoinableQueue(maxsize=args.maxqueue)
    deleted_queue = multiprocessing.JoinableQueue(maxsize=args.maxqueue)

    # Our thread-safe variables, used for progress tracking
    global queued_keys, deleted_keys
    queued_keys = multiprocessing.Value('i', 0)
    deleted_keys = multiprocessing.Value('i', 0)

    # Start the progress_updater thread
    progress_cancellation_token = threading.Event()
    progress_updater_thread = threading.Thread(target=progress_updater,
                                               name="progress_updater_thread",
                                               args=(key_count, progress_cancellation_token))
    progress_updater_thread.daemon = True
    progress_updater_thread.start()

    # Now start all of our delete threads
    deleter_cancellation_token = threading.Event()
    deleter_threads = []
    for i in range(args.deleter_threads):
        t = threading.Thread(target=deleter,
                             name="deleter_thread_{0}".format(i),
                             args=(args, to_delete_queue, deleted_queue, deleter_cancellation_token))
        t.daemon = True
        t.start()
        deleter_threads.append(t)

    # Start List thread
    list_thread = threading.Thread(target=key_loader,
                                   name="lister_thread",
                                   args=(args.filepath, to_delete_queue, args.deleter_threads))
    list_thread.daemon = True
    list_thread.start()

    # Start our output processed writer thread
    processed_cancellation_token = threading.Event()
    processed_writer_thread = threading.Thread(target=processed_writer,
                                               name="processed_writer_thread",
                                               args=('processed.txt', deleted_queue, processed_cancellation_token))
    processed_writer_thread.daemon = True
    processed_writer_thread.start()
    time.sleep(5)

    # wait for all records to be processed
    try:
        # Ensure all keys have been listed into the queue
        list_thread.join()

        # Wait for threads to finish
        for thread in deleter_threads:
            thread.join()

        # Ensure we've finished writing all processed keys to our file
        deleted_queue.join()

        # Give the progress updater time to finish getting to 100%
        time.sleep(1)

        # Kill the progress updater thread
        progress_cancellation_token.set()
        progress_updater_thread.join()

        # Kill the process worker thread
        processed_cancellation_token.set()
        processed_writer_thread.join()

        time.sleep(5)

        delete_msg = 'Finished deleting keys: {0}'.format(queued_keys.value)
        logger.info(delete_msg)
        print('\n'+delete_msg)

    except KeyboardInterrupt:
        print('\nCtrl-c pressed ...\n')
        processed_cancellation_token.set()
        deleter_cancellation_token.set()
        progress_cancellation_token.set()


if __name__ == '__main__':
    main()
    