import io
from aiohttp import web

from ept.boundingboxes import BoundingBox3D, BoundingBox2D
from ept.eptresource import EPTResource
from ept.queryparams import QueryParams

RESOURCES = {}


async def get_info(request):
    name = request.match_info["resource_name"]
    address = "https://na-c.entwine.io/{}".format(name)
    ept = EPTResource(address)
    return web.json_response(await ept.info)


async def read(request):
    name = request.match_info["resource_name"]
    address = "https://na-c.entwine.io/{}".format(name)
    xmin, ymin = request.match_info['xmin'], request.match_info['ymin']
    xmax, ymax = request.match_info['xmax'], request.match_info['ymax']

    text = " ".join(a for a in (name, xmin, ymin, xmax, ymax))
    print(text)
    try:
        ept = RESOURCES[address]
    except KeyError:
        ept = EPTResource(address)
        RESOURCES[address] = ept

    query_bounds = BoundingBox2D(int(xmin), int(ymin), int(xmax), int(ymax))
    print("querying")
    params = QueryParams(query_bounds)
    las = await ept.query(params)
    with io.BytesIO() as buffer:
        las.write(buffer, do_compress=True)
        buffer.seek(0)
        bytes = buffer.read()
    return web.Response(body=bytes)


app = web.Application()
app.add_routes(
    [
        web.get("/info/{resource_name}", get_info),
        web.get("/read/{resource_name}/[{xmin},{ymin},{xmax},{ymax}]", read),
    ]
)

web.run_app(app)
