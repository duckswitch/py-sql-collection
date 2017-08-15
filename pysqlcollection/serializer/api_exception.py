# coding: utf-8
"""
This file contains exceptions for the ApiSerializer Class
"""

class ApiSerializerException(Exception):
    """
    Base exception for ApiSerializer class.
    """
    def __init__(self, message, status_code=None, api_error_code=None, payload=None):
        Exception.__init__(self)
        self.message = message
        self.status_code = status_code if status_code else 400
        self.api_error_code = api_error_code or status_code
        self.payload = payload

    def to_dict(self):
        """
        Return dict representation.
        """
        rv = dict(self.payload or ())
        rv[u'message'] = self.message
        return rv

    def __str__(self):
        return repr(self.message)

class WrongParameter(ApiSerializerException):
    """
    Raise when the API meets a wrong parameter.
    """
    def __init__(self, message, api_error_code=u"WRONG_PARAMETER", payload=None):
        ApiSerializerException.__init__(
            self,
            message,
            400,
            api_error_code,
            payload
        )
