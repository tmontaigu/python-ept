import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor

import pylas
from typing import Dict, List

from ept.boundingboxes import BoundingBox, BoundingBox2D, BoundingBox3D
from ept.key import Key

logger = logging.getLogger(__name__)


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

    def __repr__(self):
        return "<DepthRange({}, {})>".format(
            self.depth_begin, self.depth_end
        )


class QueryParams:
    def __init__(self, bounds, depth_range=DepthRange()):
        self.bounds: BoundingBox = bounds
        self.depth_range: DepthRange = depth_range

    def ensure_3d_bounds(self, reference_bounds):
        if isinstance(self.bounds, BoundingBox2D):
            xmin, ymin, xmax, ymax = self.bounds
            self.bounds = BoundingBox3D(xmin, ymin, reference_bounds[2], xmax, ymax, reference_bounds[5])


def sync_overlaps(hierarchy: Dict[str, int], key: Key, params: QueryParams, overlaps_key: List):
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
        sync_overlaps(hierarchy, key.bisect(i), params, overlaps_key)


def _overlaps(hierarchy: Dict[str, int], start_key: Key, params: QueryParams) -> List[str]:
    keys, overlaps_key = [start_key], []
    while keys:
        current_key = keys.pop()
        if not current_key.bounds.overlaps(params.bounds):
            continue

        try:
            count = hierarchy[str(current_key)]
        except KeyError:
            continue
        else:
            if count == 0:
                continue

        overlaps_key.append(str(current_key))

        if params.depth_range.is_deeper(current_key.d):
            continue

        for i in range(8):
            keys.append(current_key.bisect(i))
    return overlaps_key


async def overlaps(hierarchy: Dict[str, int], key: Key, params: QueryParams, loop=None):
    if loop is None:
        loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _overlaps, hierarchy, key, params)


def sync_download_laz(source, overlaps_key, n_threads=16):
    with source.get_client() as client, ThreadPoolExecutor(n_threads) as pool:
        bin_datas = pool.map(client.fetch_bin, (key + '.laz' for key in overlaps_key))
    return bin_datas


def sync_filter_las_points(las, query):
    x = las.x
    las.points = las.points[(x >= query.bounds.xmin) & (x <= query.bounds.xmax)]
    y = las.y
    las.points = las.points[(y >= query.bounds.ymin) & (y <= query.bounds.ymax)]
    z = las.z
    las.points = las.points[(z >= query.bounds.zmin) & (z <= query.bounds.zmax)]


def sync_read_laz_files(laz_files):
    lases = [pylas.read(b) for b in laz_files]
    las = pylas.merge(lases)
    return las


async def download_laz(source, keys):
    logger.debug("Starting download of {} keys".format(len(keys)))
    async with source.get_client() as client:
        futures = [client.fetch_bin(key + ".laz") for key in keys]
        return await asyncio.gather(*futures)


async def filter_las_points(las, query, loop=None, executor=None):
    if loop is None:
        loop = asyncio.get_event_loop()
    return await loop.run_in_executor(executor, sync_filter_las_points, las, query)


async def read_laz_files(laz_files, loop=None, executor=None):
    if loop is None:
        loop = asyncio.get_event_loop()
    return await loop.run_in_executor(executor, sync_read_laz_files, laz_files)
