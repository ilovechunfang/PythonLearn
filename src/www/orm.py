'''
Created on 2015-9-17

@author: gcf
'''
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'lovemyself'

import asyncio, logging
import aiomysql

def log(sql, args = ()):
    logging.info('SQL: %s' %sql)
    
def create_pool(loop, **kw):
    logging.info('create database connection pool...')
    global __pool
    __pool = aiomysql.create_pool(
        host = kw.get('host', 'localhost'),
        port = kw.get('port', 3306),
        user = kw.get['user'],
        password = kw.get['password'],
        db = kw['db'],
        charset = kw.get('charset', 'utf-8'),
        autocommit = kw.get('autocommit', True),
        maxsize = kw.get('maxsize', 10),
        minsize = kw.get('minsize', 1),
        loop = loop
    )
    
def select(sql, args, size = None):
    'query'
    log(sql, args)
    global __pool
    with (yield from __pool) as conn:
        cur = yield from conn.cursor(aiomysql.DictCursor)
        yield from cur.exec(sql.replace('?', '%s'), args or ())
        if size:
            rs = yield from cur.fetchmany(size)
        else :
            rs = yield from cur.fetchall()
        yield from cur.close()
        logging.info('rows returned: %s' %len(rs))
        return rs

def execute(sql, args, autoCommit = True):
    'add/modify/delete'
    log(sql)
    with (yield from __pool) as conn:
        if not autoCommit:
            yield from conn.begin()
        try:
            cur = conn.cursor()
            cur = yield  from conn.execute(sql.replace(sql, '?', '%s'), args)
            affected = cur.rowcount
            yield from cur.close()
            if not autoCommit:
                yield from conn.commit()
        except BaseException  as e:
            logging.info(e)
            if not autoCommit:
                yield from conn.rollback()
            raise
        return affected


def create_args_string(num):
    L = []
    for n in range(num):
        L.append('?')
    return ','.join(L)


class Field(object):
    def __init__(self, name, column_type, primary_key, default_val):
        self.name = name
        self.column_type = column_type
        self.primary_key = primary_key
        self.default_val = default_val
        
    def __str__(self):
        return '<classname = %s, instance: name=%s, type=%s, default val = %s>' % (self.__class__.__name__, self.name, self.column_type, self.default_val)

class StringField(Field):
    def __init__(self, name = None, primary_key = False, default_val = None, dd1 ='varchar(100)'):
        super().__init__(name, dd1, primary_key, default_val)  

class BooleanField(Field):
    def __init__(self, name = None, default_val = False):
        super().__init__(name, 'boolean', False, default_val)

class IntegerField(Field):
    def __init__(self, name = None, primary_key = False, default_val = 0):
        super().__init__(name, 'bigint', primary_key, default_val)

class FloatField(Field):
    def __init__(self, name = None, primary_key = False, default_val = 0.0):
        super().__init__(name, 'float', primary_key, default_val)

class TextField(Field):
    def __init__(self, name = None, default_val = None):
        super().__init__(name, 'real', False, default_val)


class StandardError(BaseException):
    def __init__(self, message):
        print('StandardError: %s' %message)

class ModelMetaClass(type):
    def __new__(cls, name, bases, attrs):
        if name == 'Model':
            return type.__new__(cls, name, bases, attrs)
        tableName = attrs.get('__table__', None) or name
        logging.info('found model : %s (table: %s)' %(name, tableName))
        mappings = dict()
        fields = []
        primaryKey  = None
        for k, v in attrs.items():
            if isinstance(v, Field):
                logging.info('found mapping: %s ==> %s' %(k,v))
                mappings[k] = v
                if v.primary_key:
                    if primaryKey:
                        raise StandardError('Duplicate primary key for field: %s'  %k)
                    primaryKey = k
                else :
                    fields.append(k)
        if not primaryKey:
            raise StandardError('Primary key not found.')
        for k in mappings.keys():
            attrs.pop(k)
        print('test')
        print('test')
        print('test')
        
        
        
        return type.__new__(cls, name, bases, attrs)
        
