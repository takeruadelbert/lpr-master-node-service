import asyncio
import os

from aiohttp import web

from broker.broker import Broker
from misc.constant.value import DEFAULT_PORT, DEFAULT_RESET_STATE_SCHEDULER_TIME, DEFAULT_KAFKA_CONSUME_DELAY_TIME
from service import LPRMasterService

service = LPRMasterService()
broker = Broker()


def setup_route():
    return [
        web.post('/notify', service.notify),
        web.post('/get-state', service.get_data_last_state)
    ]


async def initialization():
    app = web.Application()
    asyncio.get_event_loop().create_task(scheduler_reset_state())
    asyncio.get_event_loop().create_task(consume_message_queue())
    app.router.add_routes(setup_route())
    return app


async def scheduler_reset_state():
    while True:
        service.reset_state()
        await asyncio.sleep(int(os.getenv("RESET_STATE_SCHEDULER_TIME", DEFAULT_RESET_STATE_SCHEDULER_TIME)))


async def consume_message_queue():
    while True:
        broker.consume()
        await asyncio.sleep(DEFAULT_KAFKA_CONSUME_DELAY_TIME)


if __name__ == "__main__":
    web.run_app(initialization(), port=os.getenv('PORT', DEFAULT_PORT))
