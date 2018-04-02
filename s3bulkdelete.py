#!/usr/bin/env python
"""
This module bulk deletes of set of documents from s3 
"""
import itertools
import argparse
import logging
import boto3
import tqdm
import sys
import asyncio
import aiobotocore

__version__ = '1.0.0'
_LOG_LEVEL_STRINGS = ['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG']
_SENTINEL = None

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
                        help='# of keys to batch delete (default 500)',
                        type=int, default=500)
    parser.add_argument('--concurrency',
                        help='# of concurrent actions (default 1000)',
                        type=int, default=1000)

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
    # sys.excepthook = lambda t, v, x: logger.error(
    #     'Uncaught exception', exc_info=(t, v, x))

    # Supress boto debug logging, since it is very chatty
    logging.getLogger('boto3').setLevel(logging.WARNING)
    logging.getLogger('botocore').setLevel(logging.WARNING)


def key_file_len(filepath):
    """Quickly iterates the input file to get a count of keys to be processed.

    Arguments:
        filepath {str} -- path to the file to count

    Returns:
        {int} -- number of lines in file
    """

    with open(filepath) as key_file:
        i = None
        for i, _ in enumerate(key_file):
            pass
    return i + 1


async def get_versions(sem, queue, s3client, s3bucket_name, total_keys, input_filepath):
    """
    Loads all objects from the input file, queries s3 for all object versions
    of each object and queues the object versions on the the queue for processing by the deleter.
    
    Arguments:
        sem {semaphore} -- semaphore to control concurrency
        queue {queue} -- queue for sharing work with the deleter
        s3client {aiobotocore s3client} -- s3 client
        s3bucket_name {str} -- bucket to delete from
        total_keys {int} -- the total keys to delete.  note: this is used to initialize the progress bar
        input_filepath {str} -- path of the file holding keys to delete
    """
    with tqdm.tqdm(total=total_keys, desc='Getting object versions', unit='keys',position=0) as progress:
        for key in open(input_filepath, 'r'):
            logger.debug("Discovering key '{}'".format(key.rstrip()))
            
            async with sem:
                object_versions = []

                # list all versions for this object so we can delete all of them
                response = await s3client.list_object_versions(
                    Bucket=s3bucket_name,
                    Prefix=key.rstrip()
                )

                progress.update(1)
                
                # batch up all versions into a single delete call
                versions = response['Versions'] if 'Versions' in response else []
                for v in versions:
                    object_versions.append(
                        {'VersionId': v['VersionId'], 'Key': v['Key']})

                delete_markers = response['DeleteMarkers'] if 'DeleteMarkers' in response else []
                for dm in delete_markers:
                    object_versions.append(
                        {'VersionId': dm['VersionId'], 'Key': dm['Key']})

                # put the item in the queue
                await queue.put(object_versions)

                # update the progressbar
                logger.debug("Discovered key '{}'".format(key.rstrip()))

        # indicate the producer is done
        logger.debug('Done discovering key versions')
        # push sentinel value onto queue
        await queue.put(_SENTINEL)


def _unique_items_by_keyname(list_of_dict, keyname):
    """
    Convert a list of dictionaries to a set by extracting 
    the values of 'keyname' from each item in the list.
    
    Arguments:
        list_of_dict {list of dict} -- list of dictionaries to extract from
        keyname {str} -- key to extract from each dictionary item in the list
    """
    return {i[keyname] for i in list_of_dict}


async def delete_versions(sem, queue, s3client, s3bucket_name, total_keys, batch_size):
    """
    Deleter process that groups up batches of object versions for deletion.
    It loops until it recieves the sentinel value from the queue.

    Arguments:
        sem {[semaphore]} -- semaphore to control concurrency
        queue {queue} -- queue for sharing work with the deleter
        s3client {aiobotocore s3client} -- s3 client
        s3bucket_name {str} -- bucket to delete from
        total_keys {int} -- the total keys to delete.  note: this is used to initialize the progress bar
        batch_size {int} -- the desired number of object versions to batch together into a single delete_objects
            request
    """
    with open('deleted.txt', mode='w') as deleted_file, \
            open('errored.txt', mode='w') as errors_file, \
            tqdm.tqdm(total=total_keys, desc='Deleting object versions', unit='keys',position=1) as progress:
        object_versions = []

        # We will break out of loop when we find 'None' on the queue
        while True:
            # wait for an item from the producer
            item = await queue.get()
            
            # the producer emits a sentinel value to indicate that it is done
            if item is _SENTINEL:

                # delete the last batch
                if len(object_versions):
                    # update progress bar with the number of keys processed.
                    # Note: there might be multiple object versions for each key.
                    # This is just a count of the unique keys
                    keys_processed = _unique_items_by_keyname(
                        object_versions, 'Key')

                    logger.debug('Deleting batch of {} unique keys with {} versions'.format(len(keys_processed), len(object_versions)))

                    # delete the set of object versions
                    await delete_batch(sem, s3client, s3bucket_name,
                                 object_versions, deleted_file, errors_file)

                    logger.debug("Deleted keys: {}".format(keys_processed))
                    
                    progress.update(len(keys_processed))
                break

            object_versions += item

            # if we have accumulated enough keys in this
            # thread's delete list to justify a batch delete
            if len(object_versions) >= batch_size:
                # update progress bar with the number of keys processed.
                # Note: there might be multiple object versions for each key.
                # This is just a count of the unique keys
                keys_processed = _unique_items_by_keyname(
                    object_versions, 'Key')
                
                logger.debug('Deleting batch of {} unique keys with {} versions'.format(len(keys_processed), len(object_versions)))
                
                # delete the set of object versions
                await delete_batch(sem, s3client, s3bucket_name,
                             object_versions, deleted_file, errors_file)

                logger.debug("Deleted keys: {}".format(keys_processed))
 
                progress.update(len(keys_processed))

                # reset the key list
                object_versions = []


async def delete_batch(sem, s3client, s3bucket_name, object_versions, deleted_file, errors_file):
    """Delete a batch of records from s3
    
    Arguments:
        sem {[semaphore]} -- semaphore to control concurrency
        s3client {aiobotocore s3client} -- s3 client
        s3bucket_name {str} -- bucket to delete from
        object_versions {list of dict} -- list of object versions to delete
        deleted_file {file} -- file object for the file to record deleted keys
        errors_file {file} -- file object for the file to record errors
    """
    async with sem:
        status = {'deleted': [], 'errors': []}

        resp = await s3client.delete_objects(
            Bucket=s3bucket_name, Delete={'Objects': object_versions})

        if 'Deleted' in resp:
            for deleted in resp['Deleted']:
                status['deleted'].append(deleted['Key'])

        if 'Errors' in resp:
            for error in resp['Errors']:
                status['errors'].append('{0} failed because: {1}'.format(
                    error['Key'], error['Message']))

        for item in status['deleted']:
            deleted_file.write("{0}\n".format(item))
        for item in status['errors']:
            errors_file.write("{0}\n".format(item))


async def run(loop, input_filepath, s3bucket_name, batch_size, concurrency):
    """
    Top level async task loop that coordinates with the get_versions (producer)
    and delete_versions (consumer) coroutines.


    Arguments:
        loop {asyncio.AbstractEventLoop} -- asyncio event loop
        input_filepath {str} -- path of the file holding keys to delete
        s3bucket_name {str} -- bucket to delete from
        batch_size {int} -- the desired number of object versions to batch together into a single delete_objects
            request
        concurrency {int} -- max number of concurrent asyncio actions
    """
    # Setup asyncio objects
    queue = asyncio.Queue(loop=loop)
    sem = asyncio.Semaphore(concurrency)
    session = aiobotocore.get_session(loop=loop)

    # quickly find the total keys we expect to delete to setup the progress bar
    total_keys = key_file_len(input_filepath)

    async with session.create_client('s3') as s3client:
        # wait until we have completed:
        # - the extraction of all object versions
        # - the deletions of all those object versions
        await asyncio.gather(
            get_versions(sem, queue, s3client, s3bucket_name, total_keys, input_filepath), 
            delete_versions(sem, queue, s3client, s3bucket_name, total_keys, batch_size=batch_size)
        )


def main():
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

    # Run asyncronous event loop
    loop = asyncio.get_event_loop()

    # Runs the event loop until the 'run' method completes
    loop.run_until_complete(
        run(loop, args.filepath, args.s3bucket, args.batchsize, args.concurrency))
    loop.close()


if __name__ == '__main__':
    main()