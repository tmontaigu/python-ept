import logging

from ept.boundingboxes import BoundingBox3D
from ept.hierarchy import load_hierarchy, SyncHierarchyLoader
from ept.key import Key
from ept.queryparams import sync_overlaps, download_laz, sync_read_laz_files, sync_download_laz, filter_las_points, \
    QueryParams, overlaps, read_laz_files
from ept.sources import get_source, get_sync_source

logger = logging.getLogger(__name__)


class EPTResource:
    def __init__(self, root_address):
        self.root_address = root_address
        self.source = get_source(root_address)
        self._info = None
        self._hierarchy = None

    @property
    async def info(self):
        if self._info is None:
            logger.info("Getting info")
            self._info = await self.source.get_entwine_json()
        return self._info

    @property
    async def hierarchy(self):
        if self._hierarchy is None:
            logger.info("Getting hierarchy")
            info = await self.info
            hierarchy_step = info.get('hierarchyStep', 0)
            self._hierarchy = await load_hierarchy(self.source, hierarchy_step)
        return self._hierarchy

    async def query_tile_bytes(self, params):
        info = await self.info
        params.ensure_3d_bounds(info['bounds'])
        hierarchy = await self.hierarchy

        logger.info("Computing overlap")
        key = Key(BoundingBox3D(*info['bounds']))
        overlaps_key = await overlaps(hierarchy, key, params)

        logger.info("Downloading")
        return await download_laz(self.source, overlaps_key)

    async def query(self, params):
        lases = self.query_tile_bytes(params)
        logger.info("Reading")
        las = await read_laz_files(lases)
        await filter_las_points(las, params)
        return las


class SyncEPTResource:
    def __init__(self, root_address, n_threads=16):
        self.root_address = root_address
        self.source = get_sync_source(root_address)
        self._info = None
        self._hierarchy = None
        self.n_threads = n_threads

    @property
    def info(self):
        if self._info is None:
            self._info = self.source.get_entwine_json()
        return self._info

    @property
    def hierarchy(self):
        if self._hierarchy is None:
            hierarchy_step = self.info.get('hierarchyStep', 0)
            hierarchy_loader = SyncHierarchyLoader(self.source, hierarchy_step, n_threads=self.n_threads)
            self._hierarchy = hierarchy_loader.load()
        return self._hierarchy

    def query(self, params: QueryParams):
        info = self.info
        params.ensure_3d_bounds(info['bounds'])
        hierarchy = self.hierarchy
        key = Key(BoundingBox3D(*info['bounds']))
        overlaps_key = []
        sync_overlaps(hierarchy, key, params, overlaps_key)

        las = sync_download_laz(self.source, overlaps_key, n_threads=self.n_threads)
        las = sync_read_laz_files(las)
        filter_las_points(las, params)
        return las
