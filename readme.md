Lakefs Garbage Collection
==========================

# Issue:
We have many small files in lakeFS, which is not efficient for querying.
We want to collect all the small files into a single parquet file.

## Solution:
This is a simple garbage collection tool for lakeFS.
It is intended to be run as a cron job.

Collects all the files in a branch from a lakeFS repository with size less than a given threshold
and collects them into a duckdb table.
If the table size exceeds a given threshold, make a new parquet file and upload it to the lakeFS repository.
After uploading the parquet file success, delete the old files from the lakeFS repository.

## Usage
```
yarn start 
```

## Configuration
The following environment variables are required:
* `LAKEFS_HOST` - The lakeFS host to connect to
* `LAKEFS_ACCESS_KEY_ID` - The lakeFS access key id to use
* `LAKEFS_SECRET_ACCESS_KEY` - The lakeFS secret access key to use
* `LAKEFS_REPOSITORY` - The lakeFS repository to use
* `LAKEFS_BRANCH` - The lakeFS branch to use
* `FILE_PREFIX` - The prefix of the files to save (e.g. `blocks`)
* `TABLE_NAME` - The name of the duckdub table to use (e.g. `blocks`)
* `MAX_SIZE` - The maximum size of the table in bytes (e.g. `500` for 500 Mb)
* `INTERVAL_TIME` - The interval time in milliseconds (e.g. `5` for 5 minutes)
