#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from xml.dom import INVALID_ACCESS_ERR


__author__ = 'lovemyself'
'''
Json api defination
'''
import json, logging, inspect, functools

class APIError(Exception):
    '''
    the base APIError while contains error(required),
    data(optional), and messages(optional))
    '''
    def __init__(self, error, data = '', message = ''):
        super(APIError, self).__init__(message)
        self.error = error
        self.data = data
        self.message = message

class APIValueError(APIError):
    '''
    Indicate the input value has error or invalid.
    the data specifies the error field of input form.
    '''
    def __init__(self, field, message=''):
        super(APIValueError, self).__init__('value:invalid', field, message)
    
class APIResourceNotFoundError(APIError):
    '''
    Indicate the resource was not found. the data specifies the resource name.
    '''
    def __init__(self, field, message =''):
        super(APIResourceNotFoundError, self).__init__('value:not found', field, message)


class APIPermissionError(APIError):
    '''
    Indicate the api has no permission
    '''
    def __init__(self, field, message = ''):
        super(APIPermissionError, self).__init__('permission:forbidden', 'permission', message)
    
    