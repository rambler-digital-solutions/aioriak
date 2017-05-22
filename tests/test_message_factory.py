# import pytest
from aioriak.pb import messages
from aioriak.pb.factory import Message


class TestMessageFactory:
    async def test_ping_encode(self):
        message = Message(messages.MSG_CODE_PING_REQ)
        assert None is message.encode()
        assert messages.MSG_CODE_PING_REQ == message.code
        assert messages.MSG_CODE_PING_RESP == message.resp_code
