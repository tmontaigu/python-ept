import json

import aiobotocore


class S3Source:
    def __init__(self, bucket: str, key: str):
        self.bucket = bucket
        self.key = key

    async def get_entwine_json(self):
        async with self.get_client() as client:
            return await client.fetch_json("entwine.json")

    def get_client(self):
        return S3Client(self.bucket, self.key)


class S3Client:
    def __init__(self, bucket: str, key: str):
        self.session = aiobotocore.get_session()
        self.client = self.session.create_client('s3')
        self.bucket = bucket
        self.key = key

    async def fetch_bin(self, path):
        response = await self.client.get_object(Bucket=self.bucket, Key=self.key + "/" + path)
        async with response['Body'] as stream:
            return await stream.read()

    async def fetch_json(self, path):
        response = await self.client.get_object(Bucket=self.bucket, Key=self.key + "/" + path)
        async with response['Body'] as stream:
            return json.loads(await stream.read())

    async def fetch_hierarchy(self, key: str):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.client.__aexit__(exc_type, exc_val, exc_tb)
