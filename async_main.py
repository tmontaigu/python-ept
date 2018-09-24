import asyncio

from ept.boundingboxes import BoundingBox3D, BoundingBox2D
from ept.eptresource import EPTResource
from ept.queryparams import QueryParams


async def main_s3():
    address = "https://na-c.entwine.io/dk"
    query_bounds = BoundingBox2D(1133208, 7588362, 1134607, 7589830)
    resource = EPTResource(address)

    params = QueryParams(query_bounds)
    las = await resource.query(params)
    las.write('downloaded_ept.laz')


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(main_s3())
    loop.run_until_complete(future)
