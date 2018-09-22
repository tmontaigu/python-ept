import aiohttp


class HTTPSource:
    def __init__(self, root_url):
        self.root_url = root_url

    def get_client(self):
        return HTTPClient(self.root_url)

    async def get_entwine_json(self):
        async with self.get_client() as client:
            return await client.fetch_json("entwine.json")


class HTTPClient:
    def __init__(self, root_url):
        self.root_url = root_url
        self.session = aiohttp.ClientSession()

    async def fetch_json(self, key):
        async with self.session.get(self.root_url + "/" + key) as response:
            return await response.json()

    async def fetch_bin(self, key):
        async with self.session.get(self.root_url + "/" + key) as response:
            return await response.read()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.session.__aexit__(exc_type, exc_val, exc_tb)
