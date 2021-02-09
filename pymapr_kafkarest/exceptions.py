
class MKException(Exception):
    pass


class MKExistingInstanceException(MKException):
    pass


class MKSubscriptionException(MKException):
    pass


class MKConsumerException(MKException):
    pass
