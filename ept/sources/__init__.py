from ept.sources.httpsource import HTTPSource
from ept.sources.s3 import S3Source
from ept.sources.syncsources import SyncHTTPSource, SyncFSSource, SyncS3Source


def get_source(uri: str):
    if uri.startswith("s3://"):
        splits = uri.split('/')
        bucket = splits[2]
        key = '/'.join(splits[3:])
        return S3Source(bucket, key)
    elif uri.startswith("https://"):
        return HTTPSource(uri)
    else:
        raise ValueError("Unknown source type")


def get_sync_source(uri: str):
    if uri.startswith("s3://"):
        splits = uri.split('/')
        bucket = splits[2]
        key = '/'.join(splits[3:])
        return SyncS3Source(bucket, key)
    elif uri.startswith("https://"):
        return SyncHTTPSource(uri)
    else:
        raise ValueError("Unknown source type")
