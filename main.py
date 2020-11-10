import asyncio
import logging
import os
import re
from logging.handlers import TimedRotatingFileHandler

from aiohttp import web

from broker.broker import Broker
from misc.constant.value import DEFAULT_PORT, DEFAULT_RESET_STATE_SCHEDULER_TIME, DEFAULT_KAFKA_CONSUME_DELAY_TIME
from misc.helper.takeruHelper import create_log_dir_if_does_not_exists
from service import LPRMasterService

logger = logging.getLogger("master-node")


def setup_log():
    log_format = "%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s"
    log_level = logging.DEBUG
    handler = TimedRotatingFileHandler("log/master-node.log", when="midnight", interval=1)
    handler.setLevel(log_level)
    formatter = logging.Formatter(log_format)
    handler.setFormatter(formatter)
    handler.suffix = "%Y%m%d"
    handler.extMatch = re.compile(r"^\d{8}$")
    logger.addHandler(handler)


create_log_dir_if_does_not_exists('log')
setup_log()
service = LPRMasterService(logger)
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
