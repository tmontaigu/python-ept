import asyncio
import logging
from concurrent.futures import ProcessPoolExecutor

import io
from aiohttp import web

from ept.boundingboxes import BoundingBox2D, BoundingBox3D
from ept.eptresource import EPTResource
from ept.key import Key
from ept.queryparams import QueryParams, overlaps, download_laz, sync_read_laz_files, sync_filter_las_points

logger = logging.getLogger(__name__)

RESOURCES = {}
pool = ProcessPoolExecutor(8)


async def get_info(request):
    name = request.match_info["resource_name"]
    address = "https://na-c.entwine.io/{}".format(name)
    ept = EPTResource(address)
    return web.json_response(await ept.info)


async def process(lazes_bytes, query):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(pool, _process, lazes_bytes, query)


def _process(lazes_bytes, query):
    las = sync_read_laz_files(lazes_bytes)
    sync_filter_las_points(las, query)
    return _las_to_bytes(las)


def _las_to_bytes(las):
    with io.BytesIO() as buffer:
        las.write(buffer, do_compress=True)
        return buffer.getvalue()


async def las_to_bytes(las):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _las_to_bytes, las)


async def read(request):
    name = request.match_info["resource_name"]
    address = "https://na-c.entwine.io/{}".format(name)
    xmin, ymin = request.match_info['xmin'], request.match_info['ymin']
    xmax, ymax = request.match_info['xmax'], request.match_info['ymax']

    text = " ".join(a for a in (name, xmin, ymin, xmax, ymax))
    logger.info("The Query: {}".format(text))
    try:
        ept = RESOURCES[address]
        logger.info("Found existing")
    except KeyError:
        ept = EPTResource(address)
        RESOURCES[address] = ept

    query_bounds = BoundingBox2D(int(xmin), int(ymin), int(xmax), int(ymax))
    params = QueryParams(query_bounds)

    info = await ept.info
    params.ensure_3d_bounds(info['bounds'])
    hierarchy = await ept.hierarchy

    logger.info("Computing overlap")
    key = Key(BoundingBox3D(*info['bounds']))
    overlaps_key = await overlaps(hierarchy, key, params)

    logger.info("Downloading")
    lases = await download_laz(ept.source, overlaps_key)
    logger.info("Processing")
    bytes = await process(lases, params)

    logger.info("Sending {} bytes".format(len(bytes)))
    return web.Response(body=b'')


async def prepare():
    ept = EPTResource("https://na-c.entwine.io/dk")
    await ept.hierarchy
    RESOURCES["https://na-c.entwine.io/dk"] = ept


if __name__ == '__main__':
    import logging

    logging.basicConfig(level=logging.INFO)

    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(prepare())
    loop.run_until_complete(future)

    app = web.Application()
    app.add_routes(
        [
            web.get("/info/{resource_name}", get_info),
            web.get("/read/{resource_name}/[{xmin},{ymin},{xmax},{ymax}]", read),
        ]
    )

    web.run_app(app)
