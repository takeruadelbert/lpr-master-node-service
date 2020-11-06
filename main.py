import asyncio
import os

from aiohttp import web
from aiojobs.aiohttp import setup

from broker.broker import Broker
from misc.constant.value import DEFAULT_PORT, DEFAULT_RESET_STATE_SCHEDULER_TIME
from service import LPRMasterService

service = LPRMasterService()
broker = Broker()


def setup_route():
    return [
        web.post('/notify', service.notify),
    ]


async def initialization():
    app = web.Application()
    asyncio.get_event_loop().create_task(scheduler_reset_state())
    app.on_startup.append(start_background_tasks)
    app.on_cleanup.append(cleanup_background_tasks)
    app.router.add_routes(setup_route())
    setup(app)
    return app


async def scheduler_reset_state():
    while True:
        service.reset_state()
        await asyncio.sleep(int(os.getenv("RESET_STATE_SCHEDULER_TIME", DEFAULT_RESET_STATE_SCHEDULER_TIME)))


async def consume_message_queue(app):
    while True:
        broker.consume()


async def start_background_tasks(app):
    app['kafka_listener'] = asyncio.create_task(consume_message_queue(app))


async def cleanup_background_tasks(app):
    broker.close_consumer()
    app['kafka_listener'].cancel()
    await app['kafka_listener']


if __name__ == "__main__":
    web.run_app(initialization(), port=os.getenv('PORT', DEFAULT_PORT))
