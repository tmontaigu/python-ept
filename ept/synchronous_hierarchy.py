import threading
from concurrent.futures import ThreadPoolExecutor

import requests


class Hierarchy:
    def __init__(self, address, step):
        self.address = address + "h/"
        self.keys = {}
        self.step = step

    def read(self, dxyz):
        r = requests.get(self.address + dxyz + ".json")
        r.raise_for_status()
        return r.json()

    def load(self, root='0-0-0-0'):
        json = self.read(root)

        with ThreadPoolExecutor(max_workers=16) as pool:
            root_depth = int(root[0])
            for dxyz, count in json.items():
                with threading.Lock():
                    self.keys[dxyz] = count
                depth = int(dxyz[0])
                if self.step and depth > root_depth and (depth % self.step) == 0:
                    pool.submit(self.load, dxyz)


if __name__ == '__main__':
    base_addr = "http://na-c.entwine.io/dk/"

    INFO = requests.get(base_addr + "entwine.json").json()
    hier_step = INFO.get('hierarchyStep', 0)
    hier = Hierarchy(base_addr, hier_step)
    hier.load()
    print(len(hier.keys))
