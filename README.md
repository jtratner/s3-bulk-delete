# s3-bulk-delete
Utility for bulk deleting objects from s3.

There are 2 main scripts in this repo:
- [s3bulkdelete.py](#advanced-usage---s3bulkdeletepy) - (recommended) single threaded bulk delete
- [s3threadeddelete.py](#advanced-usage---s3threadeddeletepy) - multithreaded threaded bulk delete

## Installation

1. Clone from repo
1. (Optional) `virtualenv venv && source ./venv/bin/activate`
1. `pip install -r requirements.txt`
1. `python ./s3bulkdelete.py`


## Basic Usage

The simplest use case is to supply an s3bucket and a filepath.  
```sh
 python .\s3bulkdelete.py --s3bucket docstorage-delete-test1 --filepath .\input.txt
```

**Note:** The file should be a line-delimited set of keys that should be deleted from the s3bucket

### Advanced Usage - s3bulkdelete.py

```text
 python .\s3bulkdelete.py -h
usage: s3bulkdelete.py [-h] --filepath FILEPATH --s3bucket S3BUCKET [--dryrun]
                       [--loglevel [LOG_LEVEL]] [--batchsize BATCHSIZE]

Delete a set of objects from an s3 bucket. Objects to be deleted are supplied
in a line delimited file

optional arguments:
  -h, --help              show this help message and exit
  --filepath FILEPATH     Path to text file containing line-delimited set of
                          object keys to delete
  --s3bucket S3BUCKET     S3 bucket name to delete from
  --dryrun                Don't delete. Print what we would have deleted
  --loglevel [LOG_LEVEL]  Set the logging output level. ['CRITICAL', 'ERROR',
                          'WARNING', 'INFO', 'DEBUG']
  --batchsize BATCHSIZE   # of keys to batch delete (default 1000)
```

### Advanced Usage - s3threadeddelete.py

Run the script with the --help option to view usage and available options:

```text
python .\s3threadeddelete.py -h
usage: s3threadeddelete.py [-h] --filepath FILEPATH --s3bucket S3BUCKET
                           [--dryrun] [--loglevel [LOG_LEVEL]]
                           [--batchsize BATCHSIZE] [--maxqueue MAXQUEUE]
                           [--deleterthreads DELETER_THREADS]

Delete a set of objects from an s3 bucket. Objects to be deleted are supplied
in a line delimited file

optional arguments:
  -h, --help               show this help message and exit
  --filepath FILEPATH      Path to text file containing line-delimited set of
                           object keys to delete
  --s3bucket S3BUCKET      S3 bucket name to delete from
  --dryrun                 Don't delete. Print what we would have deleted
  --loglevel [LOG_LEVEL]   Set the logging output level. ['CRITICAL', 'ERROR',
                           'WARNING', 'INFO', 'DEBUG']
  --batchsize BATCHSIZE    # of keys to batch delete (default 1000)
  --maxqueue MAXQUEUE      Max size of deletion queue (default 10k)
  --deleterthreads DELETER_THREADS
                           Number of deleter threads (default 10)
```