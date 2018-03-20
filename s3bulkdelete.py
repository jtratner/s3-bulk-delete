#!/usr/bin/env python3
"""This module deletes a batch of documents from s3"""

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

version = '0.1'
_LOG_LEVEL_STRINGS = ['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG']

def _log_level_string_to_int(log_level_string):
    """Convert log-level arg to logging.level"""
    if not log_level_string in _LOG_LEVEL_STRINGS:
        message = 'invalid choice: {0} (choose from {1})'.format(log_level_string, _LOG_LEVEL_STRINGS)
        raise argparse.ArgumentTypeError(message)

    log_level_int = getattr(logging, log_level_string, logging.INFO)
    # check the logging log_level_choices have not changed from our expected values
    assert isinstance(log_level_int, int)

    return log_level_int


def get_args():
    """Fetch our command line arguments"""
    parser = argparse.ArgumentParser(
        description='Delete a set of objects from an s3 bucket.  Objects to be deleted are supplied in a line delimited file',
        formatter_class=lambda prog: 
            argparse.HelpFormatter(prog,max_help_position=27))
    parser.add_argument(
        '--filepath', 
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
def logger_setup(args):
                                                                              
    # Log configuration                                                       
    logging.basicConfig( 
        filename='log.txt',                                             
        level=args.log_level,                                               
        format='%(asctime)s %(levelname)s [%(thread)d] %(name)s: %(message)s',              
        datefmt='%Y-%m-%d@%H:%M:%S',                              
    )                                                                 
                                                                              
    # Create logger and point it at our log file                              
    global logger                                                             
    logger = logging.getLogger()                                     
                                                                              
    # Make the logger emit all unhandled exceptions                           
    sys.excepthook = lambda t, v, x: logger.error(
        'Uncaught exception', exc_info=(t,v,x))
                                                             
    # Supress boto debug logging, since it is very chatty    
    logging.getLogger('boto3').setLevel(logging.WARN)     
    logging.getLogger('botocore').setLevel(logging.WARN)     


def deleter(args, to_delete_queue, deleted_queue, cancellation_token):
    logger.debug('{0} started'.format(threading.current_thread().name))

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
        except queue.Empty:
            continue

        keys_to_delete.append({'Key': key})

        # Poll our deletion queue until it is empty or
        # until we have accumulated enough keys in this
        # thread's delete list to justify a batch delete
        if len(keys_to_delete) >= args.batchsize or to_delete_queue.empty():
            delete_batch(args, keys_to_delete, s3client, deleted_queue)

        to_delete_queue.task_done()
    
    # Delete any remaining batches of items
    if(len(keys_to_delete)):
        delete_batch(args, keys_to_delete, s3client, deleted_queue)

    logger.debug('{0} finished'.format(threading.current_thread().name))


def delete_batch(args, keys_to_delete, s3client, deleted_queue):
    if args.dryrun:
        time.sleep(random.random())

        if logger.isEnabledFor(logging.DEBUG):
            for key in keys_to_delete:
                logger.debug("Would have deleted '{0}'".format(key))
    else:
        logger.debug('Deleting {0} keys from s3'.format(len(keys_to_delete)))
        try:   
            response = s3client.delete_objects(
                Bucket=args.s3bucket,
                Delete={
                    'Objects': keys_to_delete
                },
            )

            deleted_count = len(response['Deleted']) if 'Deleted' in response else 0
            error_count = len(response['Errors']) if 'Errors' in response else 0
            logger.info('Deleted: {0}, Errored: {1}'.format(deleted_count, error_count))
            if 'Errors' in response:
                logger.warn(response['Errors'])
        except:
            logger.exception('Error deleting objects')
            return

    with deleted_keys.get_lock():
        deleted_keys.value += len(keys_to_delete)
    
    # Track which keys have been successfully deleted
    for key in keys_to_delete:
        deleted_queue.put(key['Key'])

    keys_to_delete = []

    # Print some progress info
    if random.randint(0,args.deleter_threads) == args.deleter_threads:
        logger.info('Deleted {0} out of {1} keys found thus far.'.format(deleted_keys.value, queued_keys.value))


def key_loader(filepath, to_delete_queue):
    """Iterate through object key file and append items to queue"""
    logger.debug('{0} started'.format(threading.current_thread().name))
    with open(filepath, 'r') as object_key_file:
        for line in object_key_file:
            to_delete_queue.put(line.rstrip())
            with queued_keys.get_lock():
                queued_keys.value += 1
    
    logger.info('Loaded {0} keys'.format(queued_keys.value))
    logger.debug('{0} finished'.format(threading.current_thread().name))


def processed_writer(dest_filename, deleted_queue, cancellation_token):
    """write each processed key to an output file"""
    logger.debug('{0} started'.format(threading.current_thread().name))    
    
    with open(dest_filename, 'w') as dest_file:
        while not cancellation_token.is_set(): 
            try:
                key = deleted_queue.get(timeout=1)
            except queue.Empty:
                # if no item within 10s then loop again.  This ensures that our thread stays responsive to cancellation_token
                continue
            dest_file.write(key+'\n')
            deleted_queue.task_done()
    logger.debug('{0} finished'.format(threading.current_thread().name))


def progress_updater(key_count):
    logger.debug('{0} started'.format(threading.current_thread().name))
    last_value = 0
    with tqdm.tqdm(total=key_count, unit='keys') as pbar:
        while deleted_keys.value<=key_count:
            pbar.update(deleted_keys.value-last_value)
            last_value=deleted_keys.value
            time.sleep(.1)
        
        pbar.close()


def key_file_len(filepath):
    with open(filepath) as f:
        for i, l in enumerate(f):
            pass
    return i + 1


# Our main function
def main():
    """Runs s3bulkdelete
    """

    # Parse arguments
    args = get_args()

    # Set up the logging object
    logger_setup(args)

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
    print('Found {0} keys in file {1}'.format(key_count, args.filepath))

    deleter_cancellation_token = threading.Event()
    processed_cancellation_token = threading.Event()
    to_delete_queue = multiprocessing.JoinableQueue(maxsize=args.maxqueue)
    deleted_queue = multiprocessing.JoinableQueue(maxsize=args.maxqueue)

    # Our thread-safe variables, used for progress tracking
    global queued_keys, deleted_keys
    queued_keys = multiprocessing.Value('i',0)
    deleted_keys = multiprocessing.Value('i',0)

    progress_updater_thread = threading.Thread(target=progress_updater,
        name="progress_updater_thread",
        args=(key_count,))
    progress_updater_thread.daemon=True
    progress_updater_thread.start()

    # Now start all of our delete & list threads
    deleter_threads = []
    for i in range(args.deleter_threads):
        t = threading.Thread(target=deleter,
            name="deleter_thread_{0}".format(i),
            args=(args, to_delete_queue, deleted_queue, deleter_cancellation_token))
        t.daemon = True
        t.start()
        deleter_threads.append(t)

    list_thread = threading.Thread(target=key_loader,
        name="lister_thread",
        args=(args.filepath, to_delete_queue))
    list_thread.daemon=True
    list_thread.start()

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

        # Ensure all keys have been processed out of the queue
        to_delete_queue.join()

        # kill all deleter threads
        deleter_cancellation_token.set()

        # Wait for threads to finish
        for thread in deleter_threads:
            thread._stop
            thread.join()

        # Ensure we've finished writing all processed keys to our file
        deleted_queue.join()
        
        # Kill the process worker thread
        processed_cancellation_token.set()
        processed_writer_thread.join()

        progress_updater_thread.join()

        time.sleep(5)

        delete_msg = 'Finished deleting keys: {0}'.format(queued_keys.value)
        logger.info(delete_msg)
        print('\n'+delete_msg)
        
    except KeyboardInterrupt:
        print('\nCtrl-c pressed ...\n')
        processed_cancellation_token.set()
        deleter_cancellation_token.set()
    

if __name__ == '__main__':
    main()
