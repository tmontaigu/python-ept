import json

import boto3
import fs
import requests


class SyncS3Source:
    def __init__(self, bucket: str, key: str):
        self.bucket = bucket
        self.key = key

    def get_client(self):
        return SyncS3Client(self.bucket, self.key)

    def get_entwine_json(self):
        return self.get_client().fetch_json('entwine.json')


class SyncS3Client:
    def __init__(self, bucket: str, key: str):
        self.bucket = bucket
        self.key = key
        self.s3 = boto3.resource('s3')

    def fetch_json(self, key):
        return json.loads(self.fetch_bin(key))

    def fetch_bin(self, key):
        obj = self.s3.Object(self.bucket, self.key + "/" + key)
        response = obj.get()
        return response['Body'].read()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class SyncFSSource:
    def __init__(self, path):
        self.root_path = path

    def get_entwine_json(self):
        with self.get_client() as client:
            return client.fetch_json('entwine.json')

    def get_client(self):
        return SyncFSClient(self.root_path)


class SyncFSClient:
    def __init__(self, root_path):
        self.file_system = fs.open_fs(root_path)

    def fetch_json(self, key):
        with self.file_system.open("/" + key, mode='r') as f:
            return json.loads(f.read())

    def fetch_bin(self, key):
        with self.file_system.open(key, mode='rb') as f:
            return f.read()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.file_system.close()


class SyncHTTPSource:
    def __init__(self, root_url):
        self.root_url = root_url
        self.client = self.get_client()

    def get_client(self):
        return SyncHttpClient(self.root_url)

    def get_entwine_json(self):
        return self.client.fetch_json('entwine.json')


class SyncHttpClient:
    def __init__(self, root_url):
        self.root_url = root_url

    def fetch_json(self, key):
        response = requests.get(self.root_url + '/' + key)
        response.raise_for_status()
        return response.json()

    def fetch_bin(self, key):
        response = requests.get(self.root_url + '/' + key)
        response.raise_for_status()
        return response.content

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
