#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from aiohttp import web
import asyncio

__author__ = 'lovemyself'

'''
asyn web Application
'''

import logging
from aiohttp import web

logging.basicConfig(level = logging.INFO)

def index(request):
    return web.Response(body = b'<h1>Welcome !</h1>')

def init(loop):
    app = web.Application(loop = loop)
    app.router.add_route('GET', '/', index)
    srv = yield from loop.create_server(app.make_handler(), '127.0.0.1', 9000)
    logging.info('server started at http://127.0.0.1:9000')
    return srv

loop = asyncio.get_event_loop()
loop.run_until_complete(init(loop))
loop.run_forever()

