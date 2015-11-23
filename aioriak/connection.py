import asyncio
import struct
from riak_pb import messages


MAX_CHUNK_SIZE = 65536


async def create_connection(host='localhost', port=8087, loop=None):
    reader, writer = await asyncio.open_connection(
        host, port, loop=loop)
    conn = RiakConnection(reader, writer, loop=loop)
    return conn


class RPBParser:
    def __init__(self, *args):
        self._data = bytearray(*args)
        self._writer = self._feed_data()
        self._header_parsed = False
        self._exception = None
        self._eof = False
        self._msglen = 0
        self._msg = bytearray()
        next(self._writer)

    def at_eof(self):
        return self._eof

    def _feed_data(self):
        while True:
            chunk = yield
            if chunk:
                self._data.extend(chunk)
                if not self._header_parsed and len(self._data) >= 4:
                    self._msglen, = struct.unpack('!i', self._data[:4])
                    self._header_parsed = True
                if self._header_parsed and \
                        len(self._data) >= self._msglen + 4:
                    self._eof = True
                    self._msg = self._data[4:self._msglen + 4]
                    self.msg_code, = struct.unpack("B", self._msg[:1])
                    if self.msg_code is messages.MSG_CODE_ERROR_RESP:
                        raise Exception('Raik error', self._msg)
                    elif self.msg_code in messages.MESSAGE_CLASSES:
                        print('Normal message')
                    else:
                        raise Exception('Unknown message code')
            if self._exception:
                raise self._exception

    def feed_data(self, data):
        if not self._exception:
            self._writer.send(data)


class RiakConnection:

    def __init__(self, reader, writer, loop=None):
        self._loop = loop or asyncio.get_event_loop()
        self._writer = writer
        self._reader = reader
        self._parser = RPBParser()

    def _encode_message(self, msg_code, msg=None):
        if msg is None:
            return struct.pack("!iB", 1, msg_code)
        # msgstr = msg.SerializeToString()
        # slen = len(msgstr)
        # hdr = struct.pack("!iB", 1 + slen, msg_code)
        # return hdr + msgstr

    def _request(self, msg_code, msg=None):
        self._writer.write(self._encode_message(msg_code, msg))
        print('+')
        response = self._read_response()
        print('self._request', response)
        self._parser = RPBParser()
        return response

    async def _read_response(self):
        while not self._reader.at_eof():
            print('at eof:', self._reader.at_eof())
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
            print('data:', data)
        return self._parser.msg_code

    async def ping(self, error=False):
        if error:
            response = await self._request(messages.MSG_CODE_PING_RESP)
        else:
            response = await self._request(messages.MSG_CODE_PING_REQ)
        return response


if __name__ == '__main__':
    loop = asyncio.get_event_loop()

    async def ping():
        conn = await create_connection(loop=loop)
        print('=' * 80)
        val = await conn.ping()
        print(val)
        print('=' * 80)
        val = await conn.ping(error=True)
        print(val)
        print('=' * 80)

    loop.run_until_complete(ping())
