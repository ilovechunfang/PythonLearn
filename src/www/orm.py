'''
Created on 2015-9-17

@author: gcf
'''
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import asyncio, logging
from test.pickletester import metaclass

import aiomysql


__author__ = 'lovemyself'


def log(sql, args = ()):
    logging.info('SQL: %s' %sql)

@asyncio.coroutine   
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

@asyncio.coroutine     
def select(sql, args, size = None):
    'query'
    log(sql, args)
    print('sql=================' + sql)
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

@asyncio.coroutine 
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
        except BaseException as e:
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
        print('ModelMetaClass1' + str(cls) + ' ===>' + name)
        if name == 'Model':
            return type.__new__(cls, name, bases, attrs)
        print('ModelMetaClass2')
        tableName = attrs.get('__table__', None) or name
        logging.info('found model : %s (table: %s)' %(name, tableName))
        mappings = dict()
        fields = [] #除主键外的属性名
        primaryKey  = None
        for k, v in attrs.items():
            print('%s = %s' %(k, v))
            if isinstance(v, Field):
                logging.info('found mapping: %s ==> %s' %(k,v))
                mappings[k] = v
                if v.primary_key:
                    if primaryKey:
                        raise RuntimeError('Duplicate primary key for field: %s'  %k)
                    primaryKey = k
                else :
                    fields.append(k)
                    
        if not primaryKey:
            raise RuntimeError('Primary key not found.')
        for k in mappings.keys():
            attrs.pop(k)
        
        escapd_fields = list(map(lambda f : '%s' %f, fields))
        attrs['__mappings__'] = mappings #保存属性和列的映射关系
        attrs['__table__'] = tableName;
        attrs['__primarykey__'] = primaryKey #主键属性名
        attrs['__fields__'] = fields
        attrs['__select__'] = 'select `%s`, %s from `%s`' %(primaryKey, ','.join(escapd_fields), tableName)
        attrs['__insert__'] = 'insert into `%s` (%s, `%s`) values(%s)' %(tableName, ','.join(escapd_fields), primaryKey, create_args_string(len(escapd_fields) + 1) )
        attrs['__update__'] = 'update `%s` set %s where `%s` = ?' %(tableName, ','.join(map(lambda f : '%s = ?' %(mappings.get(f).name or f), fields)), primaryKey)
        attrs['__delete__'] = 'delete from `%s` where `%s` = ?' %(tableName, primaryKey)
        return type.__new__(cls, name, bases, attrs)


class Model(dict, metaclass = ModelMetaClass):
    def __init__(self, **kw):
        super(Model, self).__init__(**kw)
        print('__model__init__')
        
    def __getattr__(self, key):
        """
                        每次通过'实例'访问属性，都会经过__getattribute__函数。而当属性不存在时，
                         仍然需要访问__getattribute__，不过接着要访问'__getattr__'
                 
                        当使用类访问不存在的变量是，不会经过__getattr__函数。
        """ 
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Model' object has no attribute: '%s'" %key)
    
    def __setattr__(self, key, value):
        self[key] = value
    
    def getValue(self, key):
        print('getValue')
        return getattr(self, key, None)
    
    def getValueOrDefault(self, key):
        value = getattr(self, key, None)
        if value is None:
            field = self.__mappings__[key]
            if field.default_val is not None:
                value = field.default() if  callable(field.default) else field.default_val
                logging.debug('using default value for %s: %s' % (key, str(value)))
                setattr(self, key, value)
        return value
    
    @classmethod
    @asyncio.coroutine
    def find(cls, pk):
        'find object by primary key'
        rs = yield from select('%s where `%s` = ?' % (cls.__select__, cls.__primarykey__), [pk], 1)
        if len(rs) == 0:
            return None
        return None
    
    @classmethod
    @asyncio.coroutine
    def findAll(cls, where = None, args = None, **kw):
        'find objects by where clause'
        sql = [cls.__select__]
        if where:
            sql.append('where')
            sql.append(where)
        if args is None:
            args = []
        orderby = kw.get('orderBy', None)
        if orderby:
            sql.append('order by')
            sql.append(orderby)
        limit = kw.get('limit', None)
        if limit is not None:
            sql.append('limit')
            if isinstance(limit, int):
                sql.append('?')
                args.append(limit)
            elif isinstance(limit, tuple) and len(limit) == 2:
                sql.append('?, ?')
                args.extend(limit)
            else :
                raise ValueError('Invalid limit value: %s' %str(limit))
        rs = yield from select(' '.join(sql), args)
        
        return [cls(**r) for r in rs]
    
    @classmethod
    @asyncio.coroutine
    def findNumber(cls, selectField, where = None, args = None):
        'find number by select and where.'
        sql = ['select %s _num_ from `%s`' %(selectField, cls.__table__)]
        if where:
            sql.append('where')
            sql.append(where)
        rs = yield from select(' '.join(sql), args, 1)
        if len(rs) == 0 :
            return None
        return rs[0]['_num_']
    
    
    @asyncio.coroutine
    def save(self):
        args = list(map(self.getValueOrDefault, self.__fields__))
        args.append(self.getValueOrDefault(self.__primarykey__))
        rows = yield from execute(self.__insert__, args)
        if rows != 1:
            logging.warn('failed to insert record: affected rows :%s' %rows)
    
      
    def update(self):
        args = list(map(self.getValue, self.__fields__))
        args.append(self.getValueOrDefault(self.__primarykey__))
        rows = yield from execute(self.__update__, args)
        if rows != 1:
            logging.warn('failed to remove by primary key: affected rows: %s' %rows)

    def remove(self):
        args = [self.getValue(self.__primarykey__)]
        rows = yield from execute(self.__delete__, args)
        if rows != 1:
            logging.warn('failed to delete by primary key : affected rows: %s' %rows)
    
    
    
    @classmethod
    def hello(self):
        print("hello")
    
class User(Model):
    __table__ = 'users' #类属性
    
    id = IntegerField(primary_key = True)
    name = StringField()


def test():
    loop = asyncio.get_event_loop()
    print('1')
    create_pool(loop, db = 'test')
    print('2')
    User.hello()
    User.find(1)
    

test()    












