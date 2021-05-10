from typing import AsyncGenerator, Union, Optional
from asyncio import Event
from logging import INFO
from time import monotonic

from websockets import connect, WebSocketClientProtocol, WebSocketServerProtocol, exceptions

from bitswap.bitswap import BasePeer
from bitswap.bitswap.logger import get_stream_logger_colored


class NetworkPeer(BasePeer):

    def __init__(self, web_socket: Union[WebSocketClientProtocol, WebSocketServerProtocol],
                 con_obj: Optional[connect] = None, close_event: Optional[Event] = None,
                 log_level: int = INFO) -> None:
        self._web_socket = web_socket
        self._con_obj = con_obj
        self._close_event = close_event
        self._logger = get_stream_logger_colored(__name__, log_level)

    def __aiter__(self) -> AsyncGenerator[bytes, None]:
        return self._message_iter()

    async def send(self, message: bytes) -> None:
        await self._web_socket.send(message)

    async def close(self) -> None:
        if self._con_obj is not None:
            await self._con_obj.protocol.close()
        elif self._close_event is not None:
            self._close_event.set()
        else:
            raise Exception('Error close WebSocketProtocol')

    async def ping(self) -> Optional[float]:
        start = monotonic()
        try:
            future_ping = await self._web_socket.ping()
            await future_ping
        except exceptions.WebSocketException:
            return
        return monotonic() - start

    async def _message_iter(self) -> AsyncGenerator[bytes, None]:
        if self._con_obj is None:
            async for msg in self._web_socket:
                yield msg
        else:
            while True:
                yield await self._web_socket.recv()
