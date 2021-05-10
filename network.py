from typing import Union, List, AsyncGenerator, Tuple, Any, Optional
import json
from logging import INFO
from asyncio import Event
from asyncio.queues import Queue

from cid import CIDv0, CIDv1, make_cid
import websockets
from websockets import WebSocketClientProtocol, WebSocketServerProtocol
from kademlia.network import Server

from bitswap.bitswap import BaseNetwork
from bitswap.bitswap.logger import get_stream_logger_colored
from peer import NetworkPeer


class Network(BaseNetwork):

    def __init__(self, local_peer_cid: Union[CIDv0, CIDv1], web_socket_host: str, connect_port: int,
                 connect_host: str, web_socket_port: int, kademlia_host: str, kademlia_port: int,
                 known_nodes: List[Tuple[str, int]], uri_prefix: str = 'ws://', log_level: int = INFO) -> None:
        self._local_peer_cid = local_peer_cid
        self._web_socket_host = web_socket_host
        self._web_socket_port = web_socket_port
        self._connect_host = connect_host
        self._connect_port = connect_port
        self._kademlia_host = kademlia_host
        self._kademlia_port = kademlia_port
        self._known_kademlia_nodes = known_nodes
        self._web_sockets_server: Optional[websockets.serve] = None
        self._kademlia_server: Optional[Server] = None
        self._peer_prefix = 'peer_'
        self._block_prefix = 'block_'
        self._uri_prefix = uri_prefix
        self._peer_queue = Queue()
        self._logger = get_stream_logger_colored(__name__, log_level)

    async def __aenter__(self) -> 'Network':
        await self.run()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.stop()

    async def run(self):
        self._kademlia_server = Server()
        await self._kademlia_server.listen(self._kademlia_port, interface=self._kademlia_host)
        await self._kademlia_server.bootstrap(self._known_kademlia_nodes)
        self._web_sockets_server = websockets.serve(self._new_connection_request, self._web_socket_host,
                                                    self._web_socket_port)
        await self._web_sockets_server.__aenter__()
        await self.public_uri()

    async def stop(self):
        self._kademlia_server.stop()
        self._web_sockets_server.ws_server.close()
        await self._web_sockets_server.ws_server.wait_closed()
        self._kademlia_server = None
        self._web_sockets_server = None

    async def connect(self, peer_cid: Union[CIDv0, CIDv1]) -> NetworkPeer:
        kad_value = await self._kademlia_server.get(self._peer_prefix + str(peer_cid))
        if kad_value is None:
            raise Exception(f'Not found peer in network, peer_cid: {peer_cid}')
        uri = json.loads(kad_value)['uri']
        con_obj = websockets.connect(uri, max_size=None, extra_headers={'peer_cid': str(self._local_peer_cid)})
        web_socket: WebSocketClientProtocol = await con_obj.__aenter__()
        return NetworkPeer(web_socket, con_obj)

    async def public(self, block_cid: Union[CIDv0, CIDv1]) -> None:
        kad_value = await self._kademlia_server.get(self._block_prefix + str(block_cid))
        if kad_value is None:
            await self._kademlia_server.set(self._block_prefix + str(block_cid),
                                            json.dumps({'peers': [str(self._local_peer_cid)]}))
        else:
            dict_kad_value = json.loads(kad_value)
            peers = {str(self._local_peer_cid), *dict_kad_value['peers']}
            new_block_value = {'peers': list(peers)}
            await self._kademlia_server.set(self._block_prefix + str(block_cid), json.dumps(new_block_value))

    async def find_peers(self, block_cid: Union[CIDv0, CIDv1]) -> List[Union[CIDv0, CIDv1]]:
        res = []
        kad_value = await self._kademlia_server.get(self._block_prefix + str(block_cid))
        if kad_value is None:
            return res
        dict_kad_value = json.loads(kad_value)
        for str_p in dict_kad_value['peers']:
            if str_p != str(self._local_peer_cid):
                try:
                    res.append(make_cid(str_p))
                except Exception as e:
                    self._logger.warning(f'Bad peer_cid in kad, block_cid: {block_cid}, str_p: {str_p}, e: {e}')
        return res

    def new_connections(self) -> AsyncGenerator[Tuple[Union[CIDv0, CIDv1], NetworkPeer], None]:
        return self._peer_generator()

    async def public_uri(self) -> None:
        kad_value = json.dumps({'uri': f'{self._uri_prefix}{self._connect_host}:{self._connect_port}'})
        await self._kademlia_server.set(self._peer_prefix + str(self._local_peer_cid), kad_value)

    async def _peer_generator(self) -> AsyncGenerator[Tuple[Union[CIDv0, CIDv1], NetworkPeer], None]:
        while True:
            yield await self._peer_queue.get()

    async def _new_connection_request(self, web_socket: WebSocketServerProtocol, _: str) -> None:
        str_peer_cid = web_socket.request_headers.get('peer_cid')
        if str_peer_cid is None:
            self._logger.warning('Attempt to connect without peer_cid')
        else:
            peer_cid = make_cid(str_peer_cid)
            close_event = Event()
            peer = NetworkPeer(web_socket, close_event=close_event)
            await self._peer_queue.put((peer_cid, peer))
            await close_event.wait()
            self._logger.debug('Close internal connection')
