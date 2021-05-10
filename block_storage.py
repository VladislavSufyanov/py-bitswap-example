from typing import Union, Dict
import os

from cid import CIDv0, CIDv1
import aiofiles
from aiofiles.os import remove as aio_remove
from aiofiles.os import stat as aio_stat

from bitswap.bitswap import BaseBlockStorage


class LocalBlockStorage(BaseBlockStorage):

    def __init__(self, storage_path: str) -> None:
        if not os.path.exists(storage_path):
            raise FileNotFoundError(storage_path)
        self._storage_path = storage_path

    async def get(self, cid: Union[CIDv0, CIDv1]) -> bytes:
        async with aiofiles.open(os.path.join(self._storage_path, str(cid)), 'rb') as b_f:
            return await b_f.read()

    async def put(self, cid: Union[CIDv0, CIDv1], block: bytes) -> None:
        async with aiofiles.open(os.path.join(self._storage_path, str(cid)), 'wb') as b_f:
            await b_f.write(block)

    async def delete(self, cid: Union[CIDv0, CIDv1]) -> None:
        await aio_remove(os.path.join(self._storage_path, str(cid)))

    async def put_many(self, blocks: Dict[Union[CIDv0, CIDv1], bytes]) -> None:
        for cid, block in blocks.items():
            await self.put(cid, block)

    def has(self, cid: Union[CIDv0, CIDv1]) -> bool:
        return os.path.exists(os.path.join(self._storage_path, str(cid)))

    async def size(self, cid: Union[CIDv0, CIDv1]) -> int:
        st = await aio_stat(os.path.join(self._storage_path, str(cid)))
        return st.st_size
