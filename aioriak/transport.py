import logging
import asyncio
import struct
import json
import warnings
from riak.pb import riak_pb2
from riak.pb import riak_dt_pb2
from riak.pb import riak_kv_pb2
from collections import ChainMap
from riak.pb import messages
from riak.codecs import pbuf as codec
from aioriak.content import RiakContent
from riak.riak_object import VClock
from riak.util import decode_index_value, bytes_to_str, str_to_bytes
from aioriak.error import RiakError


MAX_CHUNK_SIZE = 65536

logger = logging.getLogger('aioriak.transport')


def _validate_timeout(timeout):
    """
    Raises an exception if the given timeout is an invalid value.
    """
    if not (timeout is None or
            (type(timeout) == int and timeout > 0)):  # noqa
        raise ValueError("timeout must be a positive integer")


def _encode_link_field(value):
    """
    Encode the field of a `RiakObject.links` entry as bytes if necessary.
    """
    if isinstance(value, bytes):
        warnings.warn(
            "Passing `RiakObject.links` fields as bytes is deprecated "
            "and may be removed in a future release. Pass the fields as "
            "strings instead.",
            category=DeprecationWarning
        )
        return value

    return value.encode()


async def create_transport(host='localhost', port=8087, loop=None):
    reader, writer = await asyncio.open_connection(
        host, port)
    conn = RiakPbcAsyncTransport(reader, writer, loop=loop)
    return conn


class RPBPacketParser:
    ''' Riak protobuf packet parser.'''
    HEADER_LENGTH = 4

    def __init__(self, reader, initial_data=bytearray(), loop=None):
        self._loop = loop or asyncio.get_event_loop()
        self._data = initial_data
        self._reader = reader
        self._header_parsed = False
        self._tail = bytearray()
        self._eof = False

    @property
    def tail(self):
        return self._tail

    def _parse_header(self):
        ''' Parse protobuf message header '''
        if self._header_parsed:
            return True
        if len(self._data) >= self.HEADER_LENGTH:
            self._msglen, = struct.unpack(
                '!i', self._data[:self.HEADER_LENGTH])
            self._header_parsed = True
        else:
            self._header_parsed = False
        return self._header_parsed

    def _parse_msg(self):
        ''' Parse protobuf message'''
        self._msg = self._data[
            self.HEADER_LENGTH:self.HEADER_LENGTH + self._msglen]
        self.msg_code, = struct.unpack("B", self._msg[:1])
        if self.msg_code is messages.MSG_CODE_ERROR_RESP:
            error = self._get_pb_msg(self.msg_code, self._msg[1:])
            logger.error('Riak error message recieved: %s',
                         bytes_to_str(error.errmsg))
            raise RiakError(bytes_to_str(error.errmsg))
        elif self.msg_code in messages.MESSAGE_CLASSES:
            logger.debug('Normal message with code %d received', self.msg_code)
            self.msg = self._get_pb_msg(self.msg_code, self._msg[1:])
        else:
            logger.error('Unknown message received [%d]', self.msg_code)

    def _grow_tail(self):
        if len(self._data) > self._msglen + self.HEADER_LENGTH:
            self._tail = self._data[
                self.HEADER_LENGTH + self._msglen:]
        else:
            self._tail = bytearray()

    def _check_eof(self):
        if self._header_parsed and \
                len(self._data) >= self.HEADER_LENGTH + self._msglen:
            self._eof = True
        return self._eof

    def _get_pb_msg(self, code, msg):
        ''' Return protobuf object from raw mesage'''
        try:
            pbclass = messages.MESSAGE_CLASSES[code]
        except KeyError:
            pbclass = None

        if pbclass is None:
            return None
        pbo = pbclass()
        pbo.ParseFromString(bytes(msg))
        return pbo

    def at_eof(self):
        return self._eof

    async def get_pbo(self):
        if self._parse_header():
            if self._check_eof():
                self._parse_msg()
                self._grow_tail()
                return self.msg_code, self.msg

        while not self.at_eof():
            chunk = await self._reader.read(MAX_CHUNK_SIZE)
            self._data.extend(chunk)
            self._parse_header()
            if self._check_eof():
                self._parse_msg()
                self._grow_tail()
                return self.msg_code, self.msg


class RPBStreamParser:
    '''
    Riak protobuf stream packets parser
    This class is async generator with feed_data method
    and iterable packets on stream
    '''

    def __init__(self, reader, loop=None):
        self._loop = loop or asyncio.get_event_loop()
        self._reader = reader
        self._in_buf = bytearray()
        self.finished = False

    @property
    def tail(self):
        return self._in_buf

    async def __aiter__(self):
        return self

    async def __anext__(self):
        if self.finished:
            raise StopAsyncIteration
        msg_code, pbo = await self._fetch_pbo()
        if msg_code is not None:
            if pbo.done:
                self.finished = True
            return (msg_code, pbo)
        else:
            raise StopAsyncIteration

    async def _fetch_pbo(self):
        parser = RPBPacketParser(self._reader, self._in_buf, self._loop)
        code, pbo = await parser.get_pbo()
        self._in_buf = parser.tail
        return code, pbo


class MapRedStream:
    """
    Wrapper for returning streaming result of MapReduce operation to user
    """
    def __init__(self, stream_parser, expect=None):
        """
        :param stream_parser: instance of StreamParser
        :param expect: expected message code for response packet
        :type expect: int | None
        """
        self._stream_parser = stream_parser
        self._buf = iter([])  # initialize with empty iterator
        self._phase = None
        self._expect = expect

    async def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return self._phase, next(self._buf)
        except StopIteration:
            msg_code, pbo = await self._stream_parser.__anext__()
            if self._expect and self._expect != msg_code:
                raise Exception(
                    'Unexpected response code ({})'.format(msg_code))

            self._phase = pbo.phase
            try:
                self._buf = iter(json.loads(bytes_to_str(pbo.response)))
            except ValueError:
                raise StopAsyncIteration

            try:
                return self._phase, next(self._buf)
            except StopIteration:
                # usually we raise this on last part of the stream,
                # when pbo.response is empty
                raise StopAsyncIteration


class RiakPbcAsyncTransport:
    ParserClass = RPBPacketParser
    StreamParserClass = RPBStreamParser

    def __init__(self, reader, writer, loop=None):
        self._loop = loop or asyncio.get_event_loop()
        self._writer = writer
        self._reader = reader
        self._parser = None

    def _encode_content(self, robj, rpb_content):
        '''
        Fills an RpbContent message with the appropriate data and
        metadata from a RiakObject.

        :param robj: a RiakObject
        :type robj: RiakObject
        :param rpb_content: the protobuf message to fill
        :type rpb_content: riak_pb2.RpbContent
        '''
        if robj.content_type:
            rpb_content.content_type = robj.content_type.encode()
        if robj.charset:
            rpb_content.charset = robj.charset.encode()
        if robj.content_encoding:
            rpb_content.content_encoding = robj.content_encoding
        for uk in robj.usermeta:
            pair = rpb_content.usermeta.add()
            pair.key = uk.encode()
            pair.value = robj.usermeta[uk].encode()
        for link in robj.links:
            pb_link = rpb_content.links.add()
            try:
                bucket, key, tag = link
            except ValueError:
                raise RiakError("Invalid link tuple %s" % link)

            pb_link.bucket = _encode_link_field(bucket)
            pb_link.key = _encode_link_field(key)
            if tag:
                pb_link.tag = _encode_link_field(tag)
            else:
                pb_link.tag = b''

        for field, value in robj.indexes:
            if isinstance(value, int):
                value = str(value)

            pair = rpb_content.indexes.add()
            pair.key = str_to_bytes(field)
            pair.value = value.encode()

        rpb_content.value = robj.encoded_data

    def _encode_bucket_props(self, props, msg):
        '''
        Encodes a dict of bucket properties into the protobuf message.

        :param props: bucket properties
        :type props: dict
        :param msg: the protobuf message to fill
        :type msg: riak_pb2.RpbSetBucketReq
        '''
        for prop in codec.NORMAL_PROPS:
            if prop in props and props[prop] is not None:
                if isinstance(props[prop], str):
                    prop_value = props[prop].encode()
                else:
                    prop_value = props[prop]
                setattr(msg.props, prop, prop_value)
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

    def _encode_hooklist(self, hooklist, msg):
        '''
        Encodes a list of commit hooks into their protobuf equivalent.
        Used in bucket properties.

        :param hooklist: a list of commit hooks
        :type hooklist: list
        :param msg: a protobuf field that is a list of commit hooks
        '''
        for hook in hooklist:
            pbhook = msg.add()
            self._encode_hook(hook, pbhook)

    def _encode_hook(self, hook, msg):
        '''
        Encodes a commit hook dict into the protobuf message. Used in
        bucket properties.

        :param hook: the hook to encode
        :type hook: dict
        :param msg: the protobuf message to fill
        :type msg: riak_pb2.RpbCommitHook
        :rtype riak_pb2.RpbCommitHook
        '''
        if 'name' in hook:
            msg.name = hook['name']
        else:
            self._encode_modfun(hook, msg.modfun)
        return msg

    def _encode_modfun(self, props, msg=None):
        '''
        Encodes a dict with 'mod' and 'fun' keys into a protobuf
        modfun pair. Used in bucket properties.

        :param props: the module/function pair
        :type props: dict
        :param msg: the protobuf message to fill
        :type msg: riak_pb2.RpbModFun
        :rtype riak_pb2.RpbModFun
        '''
        if msg is None:
            msg = riak_pb2.RpbModFun()
        msg.module = props['mod'].encode()
        msg.function = props['fun'].encode()
        return msg

    def _encode_quorum(self, rw):
        '''
        Converts a symbolic quorum value into its on-the-wire
        equivalent.

        :param rw: the quorum
        :type rw: string, integer
        :rtype: integer
        '''
        if rw in codec.QUORUM_TO_PB:
            return codec.QUORUM_TO_PB[rw]
        elif type(rw) is int and rw >= 0:
            return rw
        else:
            return None

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

    def _encode_dt_op(self, dtype, req, op):
        if dtype == 'counter':
            req.op.counter_op.increment = op[1]
        elif dtype == 'set':
            self._encode_set_op(req.op, op)
        elif dtype == 'map':
            self._encode_map_op(req.op.map_op, op)
        else:
            raise TypeError("Cannot send operation on datatype {!r}".
                            format(dtype))

    def _encode_set_op(self, msg, op):
        if 'adds' in op:
            msg.set_op.adds.extend(str_to_bytes(op['adds']))
        if 'removes' in op:
            msg.set_op.removes.extend(str_to_bytes(op['removes']))

    def _encode_map_op(self, msg, ops):
        for op in ops:
            name, dtype = op[1]
            ftype = codec.MAP_FIELD_TYPES[dtype]
            if op[0] == 'add':
                add = msg.adds.add()
                add.name = str_to_bytes(name)
                add.type = ftype
            elif op[0] == 'remove':
                remove = msg.removes.add()
                remove.name = str_to_bytes(name)
                remove.type = ftype
            elif op[0] == 'update':
                update = msg.updates.add()
                update.field.name = str_to_bytes(name)
                update.field.type = ftype
                self._encode_map_update(dtype, update, op[2])

    def _encode_map_update(self, dtype, msg, op):
        if dtype == 'counter':
            # ('increment', some_int)
            msg.counter_op.increment = op[1]
        elif dtype == 'set':
            self._encode_set_op(msg, op)
        elif dtype == 'map':
            self._encode_map_op(msg.map_op, op)
        elif dtype == 'register':
            # ('assign', some_str)
            msg.register_op = str_to_bytes(op[1])
        elif dtype == 'flag':
            if op == 'enable':
                msg.flag_op = riak_dt_pb2.MapUpdate.ENABLE
            else:
                msg.flag_op = riak_dt_pb2.MapUpdate.DISABLE

    def _encode_dt_options(self, req, params):
        for q in ['r', 'pr', 'w', 'dw', 'pw']:
            if q in params and params[q] is not None:
                setattr(req, q, self._encode_quorum(params[q]))

        for o in ['basic_quorum', 'notfound_ok', 'timeout', 'return_body',
                  'include_context']:
            if o in params and params[o] is not None:
                setattr(req, o, params[o])

    def _encode_index_req(self, bucket, index, startkey, endkey=None,
                          return_terms=None, max_results=None,
                          continuation=None, timeout=None, term_regex=None,
                          streaming=False):
        """
        Encodes a secondary index request into the protobuf message.
        :param bucket: the bucket whose index to query
        :type bucket: :class:`~aioriak.bucket.Bucket`
        :param index: the index to query
        :type index: str
        :param startkey: the value or beginning of the range
        :type startkey: int, str
        :param endkey: the end of the range
        :type endkey: int, str
        :param return_terms: whether to return the index term with the key
        :type return_terms: bool
        :param max_results: the maximum number of results to return (page size)
        :type max_results: int
        :param continuation: the opaque continuation returned from a
            previous paginated request
        :type continuation: str
        :param timeout: a timeout value in milliseconds, or 'infinity'
        :type timeout: int
        :param term_regex: a regular expression used to filter index terms
        :type term_regex: str
        :param streaming: encode as streaming request
        :type streaming: bool
        :rtype: riak_kv_pb2.RpbIndexReq
        """
        req = riak_kv_pb2.RpbIndexReq()
        req.bucket = str_to_bytes(bucket.name)
        req.index = str_to_bytes(index)
        self._add_bucket_type(req, bucket.bucket_type)
        if endkey is not None:
            req.qtype = riak_kv_pb2.RpbIndexReq.range
            req.range_min = str_to_bytes(str(startkey))
            req.range_max = str_to_bytes(str(endkey))
        else:
            req.qtype = riak_kv_pb2.RpbIndexReq.eq
            req.key = str_to_bytes(str(startkey))
        if return_terms is not None:
            req.return_terms = return_terms
        if max_results:
            req.max_results = max_results
        if continuation:
            req.continuation = str_to_bytes(continuation)
        if timeout:
            if timeout == 'infinity':
                req.timeout = 0
            else:
                req.timeout = timeout
        if term_regex:
            req.term_regex = str_to_bytes(term_regex)
        req.stream = streaming

        return req

    def _decode_dt_fetch(self, resp):
        dtype = codec.DT_FETCH_TYPES.get(resp.type)
        if dtype is None:
            raise ValueError("Unknown datatype on wire: {}".format(resp.type))

        value = self._decode_dt_value(dtype, resp.value)

        if resp.HasField('context'):
            context = resp.context[:]
        else:
            context = None

        return dtype, value, context

    def _decode_dt_value(self, dtype, msg):
        if dtype == 'counter':
            return msg.counter_value
        elif dtype == 'set':
            return self._decode_set_value(msg.set_value)
        elif dtype == 'map':
            return self._decode_map_value(msg.map_value)

    def _decode_map_value(self, entries):
        out = {}
        for entry in entries:
            name = entry.field.name[:].decode()
            dtype = codec.MAP_FIELD_TYPES[entry.field.type]
            if dtype == 'counter':
                value = entry.counter_value
            elif dtype == 'set':
                value = self._decode_set_value(entry.set_value)
            elif dtype == 'register':
                value = entry.register_value[:].decode()
            elif dtype == 'flag':
                value = entry.flag_value
            elif dtype == 'map':
                value = self._decode_map_value(entry.map_value)
            out[(name, dtype)] = value
        return out

    def _decode_set_value(self, set_value):
        return [string[:].decode() for string in set_value]

    def _decode_bucket_props(self, msg):
        '''
        Decodes the protobuf bucket properties message into a dict.

        :param msg: the protobuf message to decode
        :type msg: riak_pb2.RpbBucketProps
        :rtype dict
        '''
        props = {}

        for prop_name in codec.NORMAL_PROPS:
            if msg.HasField(prop_name):
                prop = getattr(msg, prop_name)
                if isinstance(prop, bytes):
                    props[prop_name] = prop.decode()
                else:
                    props[prop_name] = prop
        for prop in codec.COMMIT_HOOK_PROPS:
            if getattr(msg, 'has_' + prop):
                props[prop] = self._decode_hooklist(getattr(msg, prop))
        for prop in codec.MODFUN_PROPS:
            if msg.HasField(prop):
                props[prop] = self._decode_modfun(getattr(msg, prop))
        for prop in codec.QUORUM_PROPS:
            if msg.HasField(prop):
                props[prop] = self._decode_quorum(getattr(msg, prop))
        if msg.HasField('repl'):
            props['repl'] = codec.REPL_TO_PY[msg.repl]

        return props

    def _decode_hooklist(self, hooklist):
        '''
        Decodes a list of protobuf commit hooks into their python
        equivalents. Used in bucket properties.

        :param hooklist: a list of protobuf commit hooks
        :type hooklist: list
        :rtype list
        '''
        return [self._decode_hook(hook) for hook in hooklist]

    def _decode_hook(self, hook):
        '''
        Decodes a protobuf commit hook message into a dict. Used in
        bucket properties.

        :param hook: the hook to decode
        :type hook: riak_pb2.RpbCommitHook
        :rtype dict
        '''
        if hook.HasField('modfun'):
            return self._decode_modfun(hook.modfun)
        else:
            return {'name': hook.name}

    def _decode_modfun(self, modfun):
        '''
        Decodes a protobuf modfun pair into a dict with 'mod' and
        'fun' keys. Used in bucket properties.

        :param modfun: the protobuf message to decode
        :type modfun: riak_pb2.RpbModFun
        :rtype dict
        '''
        return {'mod': modfun.module.decode(),
                'fun': modfun.function.decode()}

    def _decode_quorum(self, rw):
        '''
        Converts a protobuf quorum value to a symbolic value if
        necessary.

        :param rw: the quorum
        :type rw: int
        :rtype int or string
        '''
        if rw in codec.QUORUM_TO_PY:
            return codec.QUORUM_TO_PY[rw]
        else:
            return rw

    async def _stream(self, msg_code, msg=None, expect=None):
        self._writer.write(self._encode_message(msg_code, msg))
        self._parser = self.StreamParserClass(self._reader, loop=self._loop)
        responses = []
        async for code, pbo in self._parser:
            if expect is not None and code != expect:
                raise Exception('Unexpected response code ({})'.format(code))
            responses.append((code, pbo))
        return responses

    async def _request(self, msg_code, msg=None, expect=None):
        self._writer.write(self._encode_message(msg_code, msg))
        if self._parser:
            tail = self._parser.tail
            del self._parser
        else:
            tail = bytearray()
        self._parser = self.ParserClass(self._reader, tail, loop=self._loop)
        code, response = await self._parser.get_pbo()

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

    def close(self):
        if self._writer:
            self._writer.close()
            del self._writer

    async def ping(self):
        code, res = await self._request(messages.MSG_CODE_PING_REQ)
        if code == messages.MSG_CODE_PING_RESP:
            return True
        return False

    async def get_server_info(self):
        _, res = await self._request(
            messages.MSG_CODE_GET_SERVER_INFO_REQ,
            expect=messages.MSG_CODE_GET_SERVER_INFO_RESP)
        return self._decode_pbo(res)

    async def get_client_id(self):
        _, res = await self._request(
            messages.MSG_CODE_GET_CLIENT_ID_REQ,
            expect=messages.MSG_CODE_GET_CLIENT_ID_RESP)
        return self._decode_pbo(res)['client_id']

    async def set_client_id(self, client_id):
        req = riak_kv_pb2.RpbSetClientIdReq()
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
        req = riak_pb2.RpbGetBucketTypeReq()
        req.type = bucket_type.name.encode()

        msg_code, resp = await self._request(
            messages.MSG_CODE_GET_BUCKET_TYPE_REQ, req,
            messages.MSG_CODE_GET_BUCKET_RESP)
        return self._decode_bucket_props(resp.props)

    async def fetch_datatype(self, bucket, key):

        if bucket.bucket_type.is_default():
            raise NotImplementedError('Datatypes cannot be used in the default'
                                      ' bucket-type.')
        req = riak_dt_pb2.DtFetchReq()
        req.type = bucket.bucket_type.name.encode()
        req.bucket = bucket.name.encode()
        req.key = key.encode()

        msg_code, resp = await self._request(messages.MSG_CODE_DT_FETCH_REQ,
                                             req,
                                             messages.MSG_CODE_DT_FETCH_RESP)

        return self._decode_dt_fetch(resp)

    async def get_bucket_props(self, bucket):
        '''
        Serialize bucket property request and deserialize response
        '''
        req = riak_pb2.RpbGetBucketReq()
        req.bucket = str_to_bytes(bucket.name)
        self._add_bucket_type(req, bucket.bucket_type)

        msg_code, resp = await self._request(
            messages.MSG_CODE_GET_BUCKET_REQ, req,
            messages.MSG_CODE_GET_BUCKET_RESP)

        return self._decode_bucket_props(resp.props)

    async def set_bucket_type_props(self, bucket_type, props):
        '''
        Set bucket-type properties

        :param bucket_type: A :class:`BucketType <aioriak.bucket.BucketType>`
               instance
        :type bucket_type: :class:`BucketType <aioriak.bucket.BucketType>`
        '''
        req = riak_pb2.RpbSetBucketTypeReq()
        req.type = bucket_type.name.encode()

        self._encode_bucket_props(props, req)

        msg_code, resp = await self._request(
            messages.MSG_CODE_SET_BUCKET_TYPE_REQ, req,
            messages.MSG_CODE_SET_BUCKET_RESP)
        return True

    async def set_bucket_props(self, bucket, props):
        '''
        Serialize set bucket property request and deserialize response
        '''
        req = riak_pb2.RpbSetBucketReq()
        req.bucket = str_to_bytes(bucket.name)
        self._add_bucket_type(req, bucket.bucket_type)

        self._encode_bucket_props(props, req)

        msg_code, resp = await self._request(
            messages.MSG_CODE_SET_BUCKET_REQ, req,
            messages.MSG_CODE_SET_BUCKET_RESP)
        return True

    def _add_bucket_type(self, req, bucket_type):
        if bucket_type and not bucket_type.is_default():
            req.type = bucket_type.name.encode()

    async def get_buckets(self, bucket_type=None):
        req = riak_kv_pb2.RpbListBucketsReq()
        if bucket_type:
            self._add_bucket_type(req, bucket_type)
        code, res = await self._request(messages.MSG_CODE_LIST_BUCKETS_REQ,
                                        req,
                                        messages.MSG_CODE_LIST_BUCKETS_RESP)
        return res.buckets

    async def get_keys(self, bucket):
        '''
        Lists all keys within a bucket.
        '''
        req = riak_kv_pb2.RpbListKeysReq()
        req.bucket = bucket.name.encode()
        keys = []
        self._add_bucket_type(req, bucket.bucket_type)
        for code, res in await self._stream(messages.MSG_CODE_LIST_KEYS_REQ,
                                            req,
                                            messages.MSG_CODE_LIST_KEYS_RESP):
            for key in res.keys:
                keys.append(key.decode())
        return keys

    def _decode_contents(self, contents, obj):
        '''
        Decodes the list of siblings from the protobuf representation
        into the object.

        :param contents: a list of RpbContent messages
        :type contents: list
        :param obj: a RiakObject
        :type obj: RiakObject
        :rtype RiakObject
        '''
        obj.siblings = [self._decode_content(c, RiakContent(obj))
                        for c in contents]
        # Invoke sibling-resolution logic
        if len(obj.siblings) > 1 and obj.resolver is not None:
            obj.resolver(obj)
        return obj

    def _decode_content(self, rpb_content, sibling):
        '''
        Decodes a single sibling from the protobuf representation into
        a RiakObject.

        :param rpb_content: a single RpbContent message
        :type rpb_content: riak_pb2.RpbContent
        :param sibling: a RiakContent sibling container
        :type sibling: RiakContent
        :rtype: RiakContent
        '''

        if rpb_content.HasField("deleted") and rpb_content.deleted:
            sibling.exists = False
        else:
            sibling.exists = True
        if rpb_content.HasField("content_type"):
            sibling.content_type = rpb_content.content_type.decode()
        if rpb_content.HasField("charset"):
            sibling.charset = rpb_content.charset.decode()
        if rpb_content.HasField("content_encoding"):
            sibling.content_encoding = rpb_content.content_encoding.decode()
        if rpb_content.HasField("vtag"):
            sibling.etag = rpb_content.vtag.decode()

        sibling.links = [self._decode_link(link)
                         for link in rpb_content.links]
        if rpb_content.HasField("last_mod"):
            sibling.last_modified = float(rpb_content.last_mod)
            if rpb_content.HasField("last_mod_usecs"):
                sibling.last_modified += rpb_content.last_mod_usecs / 1000000.0

        sibling.usermeta = dict([(usermd.key.decode(), usermd.value.decode())
                                 for usermd in rpb_content.usermeta])
        sibling.indexes = set([(bytes_to_str(index.key),
                                decode_index_value(index.key, index.value))
                               for index in rpb_content.indexes])

        sibling.encoded_data = rpb_content.value

        return sibling

    def _decode_link(self, link):
        '''
        Decodes an RpbLink message into a tuple

        :param link: an RpbLink message
        :type link: riak_pb2.RpbLink
        :rtype tuple
        '''

        if link.HasField("bucket"):
            bucket = link.bucket.decode()
        else:
            bucket = None
        if link.HasField("key"):
            key = link.key.decode()
        else:
            key = None
        if link.HasField("tag"):
            tag = link.tag.decode()
        else:
            tag = None

        return (bucket, key, tag)

    async def get(self, robj, r=None, pr=None, timeout=None, basic_quorum=None,
                  notfound_ok=None):
        '''
        Serialize get request and deserialize response
        '''
        bucket = robj.bucket

        req = riak_kv_pb2.RpbGetReq()
        if r:
            req.r = self._encode_quorum(r)
        if pr:
            req.pr = self._encode_quorum(pr)
        if basic_quorum is not None:
            req.basic_quorum = basic_quorum
        if notfound_ok is not None:
            req.notfound_ok = notfound_ok
        if timeout:
            req.timeout = timeout
        req.deletedvclock = True

        req.bucket = bucket.name.encode()
        self._add_bucket_type(req, bucket.bucket_type)
        req.key = robj.key.encode()

        msg_code, resp = await self._request(messages.MSG_CODE_GET_REQ, req,
                                             messages.MSG_CODE_GET_RESP)
        if resp is not None:
            if resp.HasField('vclock'):
                robj.vclock = VClock(resp.vclock, 'binary')
            # We should do this even if there are no contents, i.e.
            # the object is tombstoned
            self._decode_contents(resp.content, robj)
        else:
            # "not found" returns an empty message,
            # so let's make sure to clear the siblings
            robj.siblings = []
        return robj

    async def get_index(self, bucket, index, startkey, endkey=None,
                        return_terms=None, max_results=None,
                        continuation=None, timeout=None, term_regex=None):

        req = self._encode_index_req(bucket, index, startkey, endkey,
                                     return_terms, max_results,
                                     continuation, timeout, term_regex,
                                     streaming=False)
        msg_code, resp = await self._request(messages.MSG_CODE_INDEX_REQ, req,
                                             messages.MSG_CODE_INDEX_RESP)
        if return_terms and resp.results:
            results = [(decode_index_value(index, pair.key),
                        bytes_to_str(pair.value))
                       for pair in resp.results]
        else:
            results = [bytes_to_str(key) for key in resp.keys]

        if max_results is not None and resp.HasField('continuation'):
            return results, bytes_to_str(resp.continuation)
        else:
            return results, None

    async def put(self, robj, w=None, dw=None, pw=None, return_body=True,
                  if_none_match=False, timeout=None):
        bucket = robj.bucket

        req = riak_kv_pb2.RpbPutReq()
        if w:
            req.w = self._encode_quorum(w)
        if dw:
            req.dw = self._encode_quorum(dw)
        if pw:
            req.pw = self._encode_quorum(pw)

        if return_body:
            req.return_body = 1
        if if_none_match:
            req.if_none_match = 1
        if timeout:
            req.timeout = timeout

        req.bucket = str_to_bytes(bucket.name)
        self._add_bucket_type(req, bucket.bucket_type)

        if robj.key:
            req.key = str_to_bytes(robj.key)
        if robj.vclock:
            req.vclock = robj.vclock.encode('binary')

        self._encode_content(robj, req.content)

        msg_code, resp = await self._request(messages.MSG_CODE_PUT_REQ, req,
                                             messages.MSG_CODE_PUT_RESP)

        if resp is not None:
            if resp.HasField('key'):
                robj.key = bytes_to_str(resp.key)
            if resp.HasField('vclock'):
                robj.vclock = VClock(resp.vclock, 'binary')
            if resp.content:
                self._decode_contents(resp.content, robj)
        elif not robj.key:
            raise RiakError("missing response object")

        return robj

    async def delete(self, robj):
        req = riak_kv_pb2.RpbDelReq()

        use_vclocks = (hasattr(robj, 'vclock') and robj.vclock)
        if use_vclocks:
            req.vclock = robj.vclock.encode('binary')

        bucket = robj.bucket
        req.bucket = str_to_bytes(bucket.name)
        self._add_bucket_type(req, bucket.bucket_type)
        req.key = str_to_bytes(robj.key)

        msg_code, resp = await self._request(
            messages.MSG_CODE_DEL_REQ, req,
            messages.MSG_CODE_DEL_RESP)
        return self

    def _encode_mapred_req(self, inputs, query, timeout):
        req = riak_kv_pb2.RpbMapRedReq()
        job = {'inputs': inputs, 'query': query}
        if timeout is not None:
            job['timeout'] = timeout

        req.request = str_to_bytes(json.dumps(job))
        req.content_type = b'application/json'
        return req

    async def mapred(self, inputs, query, timeout):
        """
        Send MR Job to Server.
        Retrieves and merge all parts of result

        :param inputs: map reduce source
        :type inputs: list | dict
        :param query: map reduce phases
        :type query: list[dict]
        :type timeout: int | None
        :return: list
        """
        req = self._encode_mapred_req(inputs, query, timeout)
        parts = await self._stream(messages.MSG_CODE_MAP_RED_REQ,
                                   req,
                                   messages.MSG_CODE_MAP_RED_RESP)
        result = [json.loads(
            bytes_to_str(part.response)) for _, part in parts if part.response]
        if result and isinstance(result[0], list):
            result = sum(result, [])
        if result and isinstance(result[0], dict):
            result = dict(ChainMap(*result))
        return result

    async def stream_mapred(self, inputs, query, timeout):
        """
        Send MR Job to Server.
        Returns stream (async iterator) with result

        :param inputs: map reduce source
        :type inputs: list | dict
        :param query: map reduce phases
        :type query: list[dict]
        :type timeout: int | None
        :return: async iterator
        """

        req = self._encode_mapred_req(inputs, query, timeout)

        self._writer.write(self._encode_message(
            messages.MSG_CODE_MAP_RED_REQ, req))
        self._parser = self.StreamParserClass(self._reader, loop=self._loop)
        return MapRedStream(self._parser,
                            expect=messages.MSG_CODE_MAP_RED_RESP)

    async def update_datatype(self, datatype, **options):

        if datatype.bucket.bucket_type.is_default():
            raise NotImplementedError("Datatypes cannot be used in the default"
                                      " bucket-type.")

        op = datatype.to_op()
        type_name = datatype.type_name
        if not op:
            raise ValueError("No operation to send on datatype {!r}".
                             format(datatype))

        req = riak_dt_pb2.DtUpdateReq()
        req.bucket = str_to_bytes(datatype.bucket.name)
        req.type = str_to_bytes(datatype.bucket.bucket_type.name)

        if datatype.key:
            req.key = str_to_bytes(datatype.key)
        if datatype._context:
            req.context = datatype._context

        self._encode_dt_options(req, options)
        self._encode_dt_op(type_name, req, op)

        msg_code, resp = await self._request(
            messages.MSG_CODE_DT_UPDATE_REQ, req,
            messages.MSG_CODE_DT_UPDATE_RESP)
        if resp.HasField('key'):
            datatype.key = resp.key[:]
        if resp.HasField('context'):
            datatype._context = resp.context[:]

        datatype._set_value(self._decode_dt_value(type_name, resp))

        return True
