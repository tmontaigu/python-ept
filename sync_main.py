from ept.boundingboxes import BoundingBox3D
from ept.eptresource import SyncEPTResource
from ept.queryparams import QueryParams


def main():
    address = "https://na-c.entwine.io/dk"
    query_bounds = BoundingBox3D(1133208, 7588362, 0, 1134607, 7589830, 50)
    resource = SyncEPTResource(address)

    params = QueryParams(query_bounds)
    las = resource.query(params)
    las.write('downloaded_ept.laz')


if __name__ == '__main__':
    main()
