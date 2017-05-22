from aioriak.pb import messages


RESP_CODES = {
    messages.MSG_CODE_PING_REQ: messages.MSG_CODE_PING_RESP,
}


class Message:

    def __init__(self, code, data=None, resp_code=None):
        if code not in messages.MESSAGE_CLASSES:
            raise ValueError('Unknown MSG_CODE == {}'.format(code))
        self.code = code
        self.data = data
        self.resp_code = resp_code or RESP_CODES.get(code)

        self.encoders = {
            messages.MSG_CODE_PING_REQ: self._encode_ping
        }

    def encode(self):
        encoder = self.encoders.get(self.code)
        if encoder:
            return encoder()
        else:
            raise ValueError('Unknown MSG_CODE == {}'.format(self.code))

    def _encode_ping(self):
        return self.data

    def __repr__(self):
        return 'Message({}, {})'.format(self.code, self.resp_code)
