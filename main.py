import asyncio
import json
import os

from aiohttp import web
from aiojobs.aiohttp import setup

from misc.constant.value import DEFAULT_PORT, DEFAULT_RESET_STATE_SCHEDULER_TIME
from misc.helper.takeruHelper import get_current_datetime
from service import LPRMasterService

service = LPRMasterService()


def setup_route():
    return [
        web.post('/notify', service.notify),
    ]


async def initialization():
    app = web.Application()
    asyncio.get_event_loop().create_task(scheduler_reset_state())
    app.router.add_routes(setup_route())
    setup(app)
    return app


def test_update_state():
    states = service.fetch_whole_state()
    for state in states:
        gate_id = state[0]
        new_state = {'vehicle_type': 'bike', 'license_plate': 'D1234 ABG'}
        service.update_state(gate_id, json.dumps(new_state), get_current_datetime())


async def scheduler_reset_state():
    while True:
        service.reset_state()
        await asyncio.sleep(int(os.getenv("RESET_STATE_SCHEDULER_TIME", DEFAULT_RESET_STATE_SCHEDULER_TIME)))


if __name__ == "__main__":
    web.run_app(initialization(), port=os.getenv('PORT', DEFAULT_PORT))
