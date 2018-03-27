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

async def delete(sem, key, s3client, s3bucket_name):
    async with sem:
        status = {'deleted':[], 'errors': []}

        # list all versions for this object so we can delete all of them
        object_versions_response = await s3client.list_object_versions(
            Bucket=s3bucket_name,
            Prefix=key.rstrip()
        )

        # batch up all versions into a single delete call
        if 'Versions' not in object_versions_response:
            return status

        versions = object_versions_response['Versions']
        objects = []
        for v in versions:
            objects.append(
                {'VersionId': v['VersionId'], 'Key': v['Key']})
        
        delete_response = await s3client.delete_objects(
            Bucket=s3bucket_name, Delete={'Objects': objects})

        
        if 'Deleted' in delete_response:
            for deleted in delete_response['Deleted']:
                status['deleted'].append(deleted['Key'])

        if 'Errors' in delete_response:
            for error in delete_response['Errors']:
                status['errors'].append('{0} failed because: {1}'.format(
                    error['Key'], error['Message']))

        return status


async def do_delete(loop, input_filepath, s3bucket_name, batch_size=1000):
    """
    delete all objects with keys in input_filepath from 's3bucket_name' 

    Arguments:
        input_filepath {str} -- filepath to the file containing line-delimited set of keys
        s3bucket_name {str} -- name of s3 bucket to delete objects out of

    Keyword Arguments:
        batch_size {int} -- batch size of delete requests against s3 (default: {1000})
    """

    # quickly find the total keys we expect to delete to setup the progress bar
    total_keys = key_file_len(input_filepath)

    sem = asyncio.Semaphore(1000)
    session = aiobotocore.get_session(loop=loop)
    async with session.create_client('s3') as s3client:

        with open(input_filepath, 'r') as object_key_file, open('deleted.txt', mode='w') as deleted_file, open('errored.txt', mode='w') as errors_file:
            
            delete_tasks = [delete(sem, key, s3client, s3bucket_name) for key in object_key_file]
            for status_task in tqdm.tqdm(asyncio.as_completed(delete_tasks), total=total_keys, unit='keys',):
                status = await status_task
                for item in status['deleted']:
                    deleted_file.write("{0}\n".format(item))
                for item in status['errors']:
                    errors_file.write("{0}\n".format(item))



if __name__ == '__main__':
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
   
    loop = asyncio.get_event_loop()
    loop.run_until_complete( do_delete(loop, args.filepath, args.s3bucket))