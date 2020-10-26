import asyncio
import json
import os

from aiohttp import web
from aiojobs.aiohttp import setup, get_scheduler

from misc.constant.value import DEFAULT_PORT
from misc.helper.takeruHelper import get_current_datetime
from service import LPRMasterService

service = LPRMasterService()


def setup_route():
    return [
        web.post('/notify', service.notify),
        web.post('/forward', service.forward),
        web.get('/start', handler)
    ]


async def initialization():
    app = web.Application()
    app.router.add_routes(setup_route())
    setup(app)
    return app


def test_update_state():
    states = service.fetch_whole_state()
    for state in states:
        gate_id = state[0]
        new_state = {'vehicle_type': 'bike', 'license_plate': 'D1234 ABG'}
        service.update_state(gate_id, json.dumps(new_state), get_current_datetime())


async def test_reset_state(scheduler):
    await asyncio.sleep(3)
    service.reset_state()
    await scheduler.spawn(test_reset_state(scheduler))


async def handler(request):
    scheduler = get_scheduler(request)
    await scheduler.spawn(test_reset_state(scheduler))
    return web.Response(text='Task Scheduler has been started.')


if __name__ == "__main__":
    web.run_app(initialization(), port=os.getenv('PORT', DEFAULT_PORT))
