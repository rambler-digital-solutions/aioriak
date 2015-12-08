import os
import logging
os.environ['PYTHONASYNCIODEBUG'] = '1'
# logging.basicConfig(level=logging.DEBUG)

import asyncio
import struct
import riak_pb
from riak_pb import messages


MAX_CHUNK_SIZE = 65536
MAX_CHUNK_SIZE = 2

logger = logging.getLogger('aioriak')

# Debug
import sys
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler = logging.StreamHandler(stream=sys.stdout)
handler.setFormatter(formatter)
handler.setLevel(logging.DEBUG)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)


async def create_connection(host='localhost', port=8087, loop=None):
    reader, writer = await asyncio.open_connection(
        host, port, loop=loop)
    conn = RiakConnection(reader, writer, loop=loop)
    return conn


class RPBParser:
    """ Riak protobuf packet parser."""
    HEADER_LENGTH = 4

    def __init__(self, *args):
        self._data = bytearray(*args)
        self._writer = self._feed_data()
        self._header_parsed = False
        self._exception = None
        self._eof = False
        self._msglen = 0
        self._tail = bytearray()
        self._msg = bytearray()
        next(self._writer)

    @property
    def tail(self):
        return self._tail

    def at_eof(self):
        return self._eof

    def _feed_data(self):
        while True:
            chunk = yield
            if chunk:
                self._data.extend(chunk)
                if not self._header_parsed and \
                        len(self._data) >= self.HEADER_LENGTH:
                    self._msglen, = struct.unpack(
                        '!i', self._data[:self.HEADER_LENGTH])
                    self._header_parsed = True
                if self._header_parsed and \
                        len(self._data) >= self._msglen + self.HEADER_LENGTH:
                    self._eof = True
                    self._msg = self._data[
                        self.HEADER_LENGTH:self._msglen + self.HEADER_LENGTH]
                    self.msg_code, = struct.unpack("B", self._msg[:1])
                    if self.msg_code is messages.MSG_CODE_ERROR_RESP:
                        logger.error('Riak error message reciever')
                        raise Exception('Raik error', self._msg)
                    elif self.msg_code in messages.MESSAGE_CLASSES:
                        logger.debug('Normal message with code %d received',
                                     self.msg_code)
                        self.msg = self._parse_msg(self.msg_code,
                                                   self._msg[1:])
                    else:
                        logger.error('Unknown message received')

                    # tail is growing
                    if len(self._data) > self._msglen + self.HEADER_LENGTH:
                        self._tail = self._data[
                            self.HEADER_LENGTH + self._msglen]

            if self._exception:
                raise self._exception

    def feed_data(self, data):
        if not self._exception:
            self._writer.send(data)

    def _parse_msg(self, code, msg):
        try:
            pbclass = messages.MESSAGE_CLASSES[code]
        except KeyError:
            pbclass = None

        if pbclass is None:
            return None
        pbo = pbclass()
        pbo.ParseFromString(bytes(msg))
        return pbo


class RiakConnection:
    ParserClass = RPBParser

    def __init__(self, reader, writer, loop=None):
        self._loop = loop or asyncio.get_event_loop()
        self._writer = writer
        self._reader = reader
        self._parser = None

    def _encode_message(self, msg_code, msg=None):
        if msg is None:
            return struct.pack("!iB", 1, msg_code)
        msgstr = msg.SerializeToString()
        slen = len(msgstr)
        hdr = struct.pack("!iB", 1 + slen, msg_code)
        return hdr + msgstr

    @classmethod
    def _decode_pbo(cls, message):
        result = {}
        for key, value in message.ListFields():
            result[key.name] = value
        return result

    async def _request(self, msg_code, msg=None, expect=None):
        self._writer.write(self._encode_message(msg_code, msg))

        if self._parser:
            tail = self._parser.tail
            del self._parser
        else:
            tail = bytearray()
        self._parser = self.ParserClass(tail)

        code, response = await self._read_response()

        if expect is not None and code != expect:
            raise Exception('Unexpected response code ({})'.format(code))
        return code, response

    async def _read_response(self):
        while not self._reader.at_eof():
            try:
                data = await self._reader.read(MAX_CHUNK_SIZE)
            except asyncio.CancelledError:
                break
            except Exception:
                # XXX: for QUIT command connection error can be received
                #       before response
                # logger.error("Exception on data read %r", exc, exc_info=True)
                break
            self._parser.feed_data(data)
            if self._parser.at_eof():
                break
        return self._parser.msg_code, self._parser.msg

    async def ping(self, error=False):
        if error:
            _, response = await self._request(messages.MSG_CODE_PING_RESP)
        else:
            _, response = await self._request(
                messages.MSG_CODE_PING_REQ, expect=messages.MSG_CODE_PING_RESP)
        return response

    async def get_server_info(self):
        _, res = await self._request(
            messages.MSG_CODE_GET_SERVER_INFO_REQ,
            expect=messages.MSG_CODE_GET_SERVER_INFO_RESP)
        return self._decode_pbo(res)

    async def get_client_id(self):
        _, res = await self._request(
            messages.MSG_CODE_GET_CLIENT_ID_REQ,
            expect=messages.MSG_CODE_GET_CLIENT_ID_RESP)
        return self._decode_pbo(res)

    async def set_client_id(self, client_id):
        req = riak_pb.RpbSetClientIdReq()
        req.client_id = client_id

        code, res = await self._request(
            messages.MSG_CODE_SET_CLIENT_ID_REQ, req,
            expect=messages.MSG_CODE_SET_CLIENT_ID_RESP)
        if code == messages.MSG_CODE_SET_CLIENT_ID_RESP:
            return True
        else:
            return False


if __name__ == '__main__':
    loop = asyncio.get_event_loop()

    async def test():
        conn = await create_connection(loop=loop)
        await conn.ping()
        await conn.ping()
        server_info = await conn.get_server_info()
        print(server_info)
        res = await conn.get_client_id()
        print(res)
        res = await conn.set_client_id(b'test')
        print(res)
        res = await conn.get_client_id()
        print(res)

    loop.run_until_complete(test())
