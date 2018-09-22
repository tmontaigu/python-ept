import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor


async def get_hierarchies(source, step):
    async with source.get_client() as client:
        roots = ['0-0-0-0']

        while roots:
            responses = []
            for root in roots:
                task = asyncio.ensure_future(client.fetch_json("h/" + root + '.json'))
                responses.append(task)

            responses = await asyncio.gather(*responses)
            new_roots = []
            for root, json in zip(roots, responses):
                root_depth = int(root[0])

                for item in json.items():
                    yield item

                    depth = int(item[0][0])
                    if step and depth > root_depth and (depth % step) == 0:
                        new_roots.append(item[0])
            roots = new_roots


async def load_hierarchy(source, hierarchy_step):
    keys = {}
    async for key, count in get_hierarchies(source, hierarchy_step):
        keys[key] = count
    return keys


class SyncHierarchyLoader:
    def __init__(self, source, step, n_threads=16):
        self.source = source
        self.keys = {}
        self.step = step
        self.n_threads = n_threads

    def load(self, root='0-0-0-0'):
        with self.source.get_client() as client:
            json = client.fetch_json("h/" + root + ".json")

            with ThreadPoolExecutor(max_workers=self.n_threads) as pool:
                root_depth = int(root[0])
                for dxyz, count in json.items():

                    with threading.Lock():
                        self.keys[dxyz] = count

                    depth = int(dxyz[0])
                    if self.step and depth > root_depth and (depth % self.step) == 0:
                        pool.submit(self.load, dxyz)
        return self.keys
