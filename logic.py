from typing import Union
from concurrent.futures import ThreadPoolExecutor
import asyncio
import os

from cid import CIDv0, CIDv1, make_cid
import multihash
from multicodec.constants import CODE_TABLE

from bitswap.bitswap.table import HASH_TABLE
from network import Network
from bitswap.bitswap import Bitswap
from block_storage import LocalBlockStorage


async def aio_input(prompt: str = '') -> str:
    with ThreadPoolExecutor(1, 'aio_input') as executor:
        return (await asyncio.get_running_loop().run_in_executor(executor, input, prompt)).rstrip()


def compute_cid(data: bytes, cid_version: int, hash_func_code: int, multi_codec: int) -> Union[CIDv0, CIDv1]:
    hash_func = HASH_TABLE[hash_func_code]
    multi_hash = multihash.encode(hash_func(data).digest(), hash_func_code)
    return make_cid(cid_version, CODE_TABLE[multi_codec], multi_hash)


def create_peer_cid(peer_name: str) -> Union[CIDv0, CIDv1]:
    return compute_cid(bytes(peer_name, 'utf-8'), 1, 18, 49)


async def public_peer_uri(network: Network) -> None:
    await network.public_uri()


async def put_data(bw: Bitswap) -> None:
    print('Enter the path to the file:')
    file_path = await aio_input('> ')
    if not os.path.exists(file_path) or not os.path.isfile(file_path):
        print('Bad file path')
        return
    try:
        with open(file_path, 'rb') as f_b:
            data = f_b.read()
    except OSError as e:
        print(f'Exception: {e}')
        return
    else:
        cid = compute_cid(data, 1, 18, 49)
        res_put = await bw.put(cid, data)
        if not res_put:
            print('The data is already in storage')
        else:
            print('Data saved')
        print(f'CID: {cid}')


async def public_cid(bl_storage: LocalBlockStorage, network: Network) -> None:
    print('Enter CID:')
    try:
        cid = make_cid(await aio_input('> '))
    except Exception as e:
        print(f'Exception, e: {e}')
        return
    else:
        if bl_storage.has(cid):
            await network.public(cid)


async def get_data(bw: Bitswap) -> None:
    print('Enter CID:')
    try:
        cid = make_cid(await aio_input('> '))
    except Exception as e:
        print(f'Exception, e: {e}')
        return
    else:
        data = await bw.get(cid)
        if data is None:
            print('Can not get the data')
        else:
            print('Data received')
            res_put = await bw.put(cid, data)
            if not res_put:
                print('The data is already in storage')
            else:
                print('Data saved')
