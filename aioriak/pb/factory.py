from aioriak.pb import messages


class Message:

    def __init__(self, msg_code, data=None, resp_code=None):
        if msg_code not in messages.MESSAGE_CLASSES:
            raise ValueError('Unknown MSG_CODE == {}'.format(msg_code))
        self.msg_code = msg_code
        self.data = data
        self.resp_code = resp_code

        self.encoders = {
            messages.MSG_CODE_PING_REQ: self._encode_ping
        }

    def encode(self):
        encoder = self.encoders.get(self.msg_code)
        if encoder:
            return encoder()
        else:
            raise ValueError('Unknown MSG_CODE == {}'.format(self.msg_code))

    def _encode_ping(self):
        return self.data

    def __repr__(self):
        return 'Message({}, {})'.format(self.msg_code, self.resp_code)
