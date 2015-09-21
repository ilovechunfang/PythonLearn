#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
models for user, blog, comment
'''

__author__ = 'lovemyself'

import time, uuid

from orm import Model, StringField, BooleanField, FloatField, TextField


def next_id():
    return '%015d%s000' %(int(time.time()*1000), uuid.uuid4().hex())

class User(Model):
    __table__ = 'users'
    
    id = StringField(primary_key = True, default_val = next_id(), dd1 = 'varchar(50)')
    email = StringField(dd1 = 'varchar(50)')
    password = StringField(dd1 = 'varchar(50)')
    admin = BooleanField()
    name = StringField(dd1 = 'varchar(50)')
    image = StringField(dd1 = 'varchar(500)')
    create_at = FloatField(default_val = time.time())
    

class Blog(Model):
    __table__ = 'blogs'
    
    id = StringField(primary_key = True, default_val= next_id(), dd1 = 'varchar(50)')
    user_id = StringField(dd1 = 'varchar(50)')
    user_name = StringField(dd1 = 'varchar(50)')
    user_image = StringField(dd1 = 'varchar(500)')
    name = StringField(dd1 = 'varchar(50)')
    summary = StringField(dd1 = 'varchar(200)')
    content = TextField()
    created_at = FloatField(default_val = time.time())
    
    
class comment(Model):
    __table__ = 'comments'
    
    id = StringField(primary_key = True, default_val = next_id(), dd1 = 'varchar(50)')
    blog_id = StringField(dd1 = 'varchar(50)')
    user_id = StringField(dd1 = 'varchar(50)')
    user_name = StringField(dd1 = 'varchar(50)')
    user_image = StringField(dd1 = 'varchar(50)')
    content = TextField()
    created_at = FloatField(default_key = time.time())
    
    