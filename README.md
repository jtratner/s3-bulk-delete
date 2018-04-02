# s3-bulk-delete

Utility for bulk deleting objects from s3.

## Installation

1. Clone from repo
1. (Optional) `virtualenv venv && source ./venv/bin/activate`
1. `pip install -r requirements.txt`
1. `python ./s3bulkdelete.py`

## Basic Usage

The simplest use case is to supply an s3bucket and a filepath.

```sh
 python ./s3bulkdelete.py --s3bucket bucketxyz --filepath ./input.txt
```

**Note:** The file should be a line-delimited set of keys that should be deleted from the s3bucket

### Advanced Usage - s3bulkdelete.py

```text
python .\s3bulkdelete.py -h
usage: s3bulkdelete.py [-h] --filepath FILEPATH --s3bucket S3BUCKET [--dryrun]
                       [--loglevel [LOG_LEVEL]] [--batchsize BATCHSIZE]
                       [--concurrency CONCURRENCY]

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
  --batchsize BATCHSIZE    # of keys to batch delete (default 500)
  --concurrency CONCURRENCY
                           # of concurrent actions (default 1000)
```

## Notes

### Python Asyncio (aiobotocore)

This project uses the awesome [aiobotocore](https://github.com/aio-libs/aiobotocore) library instead of boto3 directly.
This library supports python 3's [asyncio](https://docs.python.org/3.5/library/asyncio.html) functionality for asyncronous
IO.

### S3 Bucket Versioning

If versioning is enabled then you can't just issue a delete_object(key) command.
Deleting an object by simply specifying a key will just create a new blank version of that object
and mark it with a DeleteMarker.  The effect of this is that GET requests for that key will now
return no object as the DeleteMarker is the active version.

To delete an object with versioning enabled you have to issue a delete_object request with both the key and the versionid
parameters.

In order to do that effectively for a bulk set of keys, we need to first query all versions for each key using the ```list_object_versions``` method
and then issue a ```delete_objects``` call with all of the key/versions.