import os
import logging
os.environ['PYTHONASYNCIODEBUG'] = '1'
# logging.basicConfig(level=logging.DEBUG)

import asyncio
import struct
import riak_pb
from riak_pb import messages
from riak.transports.pbc import codec


MAX_CHUNK_SIZE = 65536
MAX_CHUNK_SIZE = 2

logger = logging.getLogger('aioriak.transport')

# Debug
import sys
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler = logging.StreamHandler(stream=sys.stdout)
handler.setFormatter(formatter)
handler.setLevel(logging.DEBUG)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)


async def create_transport(host='localhost', port=8087, loop=None):
    reader, writer = await asyncio.open_connection(
        host, port, loop=loop)
    conn = RiakPbcAsyncTransport(reader, writer, loop=loop)
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


class RiakPbcAsyncTransport:
    ParserClass = RPBParser

    def __init__(self, reader, writer, loop=None):
        self._loop = loop or asyncio.get_event_loop()
        self._writer = writer
        self._reader = reader
        self._parser = None

    def _encode_bucket_props(self, props, msg):
        """
        Encodes a dict of bucket properties into the protobuf message.
        :param props: bucket properties
        :type props: dict
        :param msg: the protobuf message to fill
        :type msg: riak_pb.RpbSetBucketReq
        """
        for prop in codec.NORMAL_PROPS:
            if prop in props and props[prop] is not None:
                setattr(msg.props, prop, props[prop])
        for prop in codec.COMMIT_HOOK_PROPS:
            if prop in props:
                setattr(msg.props, 'has_' + prop, True)
                self._encode_hooklist(props[prop], getattr(msg.props, prop))
        for prop in codec.MODFUN_PROPS:
            if prop in props and props[prop] is not None:
                self._encode_modfun(props[prop], getattr(msg.props, prop))
        for prop in codec.QUORUM_PROPS:
            if prop in props and props[prop] not in (None, 'default'):
                value = self._encode_quorum(props[prop])
                if value is not None:
                    setattr(msg.props, prop, value)
        if 'repl' in props:
            msg.props.repl = codec.REPL_TO_PY[props['repl']]

        return msg

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

    async def get_bucket_type_props(self, bucket_type):
        '''
        Fetch bucket-type properties
        :param bucket_type: A :class:`BucketType <aioriak.bucket.BucketType>`
               instance
        :type bucket_type: :class:`BucketType <aioriak.bucket.BucketType>`
        '''
        req = riak_pb.RpbGetBucketTypeReq()
        req.type = bucket_type.name.encode()

        msg_code, resp = await self._request(
            messages.MSG_CODE_GET_BUCKET_TYPE_REQ, req,
            messages.MSG_CODE_GET_BUCKET_RESP)
        return resp

    async def set_bucket_type_props(self, bucket_type, props):
        '''
        Set bucket-type properties
        :param bucket_type: A :class:`BucketType <aioriak.bucket.BucketType>`
               instance
        :type bucket_type: :class:`BucketType <aioriak.bucket.BucketType>`
        '''
        req = riak_pb.RpbSetBucketTypeReq()
        req.type = bucket_type.name.encode()

        self._encode_bucket_props(props, req)

        msg_code, resp = await self._request(
            messages.MSG_CODE_SET_BUCKET_TYPE_REQ, req,
            messages.MSG_CODE_SET_BUCKET_RESP)
        return True

    def _add_bucket_type(self, req, bucket_type):
        if bucket_type and bucket_type != 'default':
            req.type = bucket_type

    async def get_buckets(self, bucket_type=None):
        req = riak_pb.RpbListBucketsReq()
        if bucket_type:
            self._add_bucket_type(req, bucket_type.name.encode())
        code, res = await self._request(messages.MSG_CODE_LIST_BUCKETS_REQ,
                                        req,
                                        messages.MSG_CODE_LIST_BUCKETS_RESP)
        return res.buckets

    async def get(self, bucket, key, bucket_type=b'default'):
        req = riak_pb.RpbGetReq()
        req.bucket = bucket
        req.key = key
        self._add_bucket_type(req, bucket_type)
        msg_code, resp = await self._request(messages.MSG_CODE_GET_REQ, req,
                                             messages.MSG_CODE_GET_RESP)
        if resp is not None:
            # self._decode_contents(resp.content)
            return str(resp.encoded_data)
