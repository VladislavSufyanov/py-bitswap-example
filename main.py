import sys
import argparse
import asyncio
from typing import List, Tuple, Optional, Sequence
from pathlib import Path
import logging
from functools import partial

from bitswap.bitswap import Bitswap
from block_storage import LocalBlockStorage
from network import Network
from logic import create_peer_cid, public_peer_uri, aio_input, put_data, public_cid, get_data


async def read_user_choice(prompt: str = '> ') -> Optional[int]:
    value = await aio_input(prompt)
    try:
        choice = int(value)
    except ValueError:
        print('Bad value')
        return
    else:
        return choice


def print_menu():
    print('1. Public peer CID', '2. Public CID', '3. Put data', '4. Get data', '0. Exit', sep='\n')


async def run(peer_name: str, storage_path: str, kad_port: int, kad_host: str,
              web_socket_port: int, web_socket_host: str, connect_port: int, connect_host: str,
              nodes_adr: List[Tuple[str, int]], log_level: str) -> int:
    int_log_level = getattr(logging, log_level, logging.INFO)
    bl_storage = LocalBlockStorage(storage_path)
    peer_cid = create_peer_cid(peer_name)
    print(f'Peer CID: {peer_cid}')
    async with Network(peer_cid, web_socket_host, web_socket_port, connect_host, connect_port,
                       kad_host, kad_port, nodes_adr, log_level=int_log_level) as network:
        async with Bitswap(network, bl_storage, int_log_level) as bw:
            do = {1: partial(public_peer_uri, network=network),
                  2: partial(public_cid, bl_storage=bl_storage, network=network),
                  3: partial(put_data, bw=bw),
                  4: partial(get_data, bw=bw)}
            choice = None
            while choice != 0:
                print_menu()
                choice = await read_user_choice()
                if choice is not None and choice in do:
                    await do[choice]()
    return 0


def main(args: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description='Bitswap node.')
    parser.add_argument('--peer-name', type=str, required=True, help='Used for generating peer_cid')
    parser.add_argument('--storage-path', type=Path, required=True, help='Path to local storage')
    parser.add_argument('--kad-port', type=int, default=21300, help='Kademlia port')
    parser.add_argument('--kad-host', type=str, default='0.0.0.0', help='Kademlia host')
    parser.add_argument('--web-socket-port', type=int, default=10100, help='WebSocket server port')
    parser.add_argument('--web-socket-host', type=str, default='0.0.0.0', help='WebSocket server host')
    parser.add_argument('--connect-port', type=int, default=10100,
                        help='WebSocket server port that is being published in Kademlia')
    parser.add_argument('--connect-host', type=str, default='127.0.0.1',
                        help='WebSocket server host that is being published in Kademlia')
    parser.add_argument('--nodes-adr', type=str, nargs='*', default=[],
                        help='Format: ip:port, ip:port ...')
    parser.add_argument('--log-level', type=str, default='INFO',
                        choices=['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG'], help='Log level')
    parsed_args = parser.parse_args(args)
    log_level = parsed_args.log_level.upper()
    nodes_adr = list(map(lambda adr: (adr[0], int(adr[1])), [adr.split(':') for adr in parsed_args.nodes_adr]))
    return asyncio.get_event_loop().run_until_complete(
        run(parsed_args.peer_name, parsed_args.storage_path, parsed_args.kad_port, parsed_args.kad_host,
            parsed_args.web_socket_port, parsed_args.web_socket_host, parsed_args.connect_port,
            parsed_args.connect_host, nodes_adr, log_level)
    )


if __name__ == '__main__':
    sys.exit(main())
