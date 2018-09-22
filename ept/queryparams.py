import asyncio
from multiprocessing.pool import ThreadPool
from typing import Dict, List

import pylas

from ept.boundingboxes import BoundingBox
from ept.key import Key


class DepthRange:
    def __init__(self, begin=0, end=None):
        self.depth_begin = begin
        self.depth_end = end

    def __contains__(self, depth):
        if self.depth_end is not None:
            return depth in range(self.depth_begin, self.depth_end)
        elif depth >= 0:
            return True
        else:
            raise ValueError('depth cannot be negative')

    def is_deeper(self, depth):
        if self.depth_end is not None:
            return depth > self.depth_end
        elif depth >= 0:
            return False
        else:
            raise ValueError('depth cannot be negative')


class QueryParams:
    def __init__(self, bounds, depth_range=DepthRange()):
        self.bounds: BoundingBox = bounds
        self.depth_range: DepthRange = depth_range


def overlaps(hierarchy: Dict[str, int], key: Key, params: QueryParams, overlaps_key: List):
    if not key.bounds.overlaps(params.bounds):
        return

    try:
        count = hierarchy[str(key)]
    except KeyError:
        return
    else:
        if count == 0:
            return

    overlaps_key.append(str(key))

    if params.depth_range.is_deeper(key.d):
        return

    for i in range(8):
        overlaps(hierarchy, key.bisect(i), params, overlaps_key)


async def download_laz(source, overlaps_key):
    async with source.get_client() as client:
        lases = []
        for key in overlaps_key:
            lases.append(asyncio.ensure_future(client.fetch_bin(key + ".laz")))

        return await asyncio.gather(*lases)


def sync_download_job(client, key):
    return client.fetch_bin(key)


def sync_download_laz(source, overlaps_key, n_threads=16):
    with source.get_client() as client, ThreadPool(n_threads) as pool:
        bin_datas = pool.map(client.fetch_bin, (key + '.laz' for key in overlaps_key))
    return bin_datas


def read_laz_files(laz_files, query_bounds):
    lases = [pylas.read(b) for b in laz_files]

    las = pylas.merge(lases)
    x = las.x
    las.points = las.points[(x >= query_bounds.xmin) & (x <= query_bounds.xmax)]
    y = las.y
    las.points = las.points[(y >= query_bounds.ymin) & (y <= query_bounds.ymax)]
    z = las.z
    las.points = las.points[(z >= query_bounds.zmin) & (z <= query_bounds.zmax)]
    return las
