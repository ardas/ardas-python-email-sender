import asyncio
import multiprocessing
from concurrent.futures import ProcessPoolExecutor

from aiohttp import web
import uvloop
from motor import motor_asyncio
import aioredis

from utils import get_config
from view import routes, listen_to_rabbit, listen_to_redis
from models import DataBase
from rabbit import RabbitConnector


asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


async def start_background_tasks(app):
    # app['rabbit_listener'] = app.loop.create_task(listen_to_rabbit(app))
    app['redis_listener'] = app.loop.create_task(listen_to_redis(app))


async def cleanup_background_tasks(app):
    # app['rabbit_listener'].cancel()
    # await app['rabbit_listener']
    app['redis_listener'].cancel()
    await app['redis_listener']


async def init_redis(app):
    print('init redis')
    app['redis'] = await aioredis.create_redis(('localhost', 6379), loop=app.loop)


def create_app(config=None):

    if not config:
        config = get_config()

    cpu_count = multiprocessing.cpu_count()
    loop = asyncio.get_event_loop()
    app = web.Application(loop=loop)
    app['executor'] = ProcessPoolExecutor(cpu_count)
    app['config'] = config
    app.router.add_routes(routes)
    # db connection
    app.client = motor_asyncio.AsyncIOMotorClient(config['MONGO_HOST'])
    app.db = DataBase(app.client[config['MONGO_DB_NAME']])
    # app.rmq = RabbitConnector(app['config'], app.loop)
    # app.loop.run_until_complete(app.rmq.connect())
    app.on_startup.append(init_redis)
    app.on_startup.append(start_background_tasks)

    app.on_cleanup.append(cleanup_background_tasks)
    return app


if __name__ == '__main__':
    app = create_app()
    web.run_app(app, host=app['config']['HOST'], port=app['config']['PORT'])
