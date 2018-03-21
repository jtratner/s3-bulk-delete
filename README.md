# s3-bulk-delete
Utility for bulk deleting objects from s3

## Install

Install using pip directly from git

``` sh
# Install latest release
$ pip3 install git+https://github.com/DiceHoldingsInc/s3-bulk-delete.git

# Install specific version
$ pip3 install git+https://github.com/DiceHoldingsInc/s3-bulk-delete.git@0.1.0
```

## Basic Usage

The simplest use case is to supply an s3bucket and a filepath.  
```sh
 python .\s3bulkdelete.py --s3bucket docstorage-delete-test1 --filepath .\input.txt
```

**Note:** The file should be a line-delimited set of keys that should be deleted from the s3bucket

### Advanced Usage

Run the script with the --help option to view usage and available options:

```text
python .\s3bulkdelete.py -h
usage: s3bulkdelete.py [-h] --filepath FILEPATH --s3bucket S3BUCKET [--dryrun]
                       [--loglevel [LOG_LEVEL]] [--batchsize BATCHSIZE]
                       [--maxqueue MAXQUEUE]
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