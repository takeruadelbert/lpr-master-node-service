import logging
import os

import aiohttp
from aiohttp import web

from database.database import Database, setup_data_state
from misc.constant.message import *
from misc.constant.value import *
from misc.helper.takeruHelper import *
from broker.broker import Broker


def setup_log():
    logging.basicConfig(
        filename=os.getcwd() + '/log/' + DEFAULT_LOG_NAME,
        level=logging.DEBUG,
        format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )


def return_message(**kwargs):
    message = kwargs.get("message", "")
    status = kwargs.get("status", HTTP_STATUS_OK)
    return web.json_response({'message': message}, status=status)


async def forward(payload):
    forward_url = os.getenv("FORWARD_URL", "")
    if not forward_url:
        logging.error(INVALID_FORWARD_URL_MESSAGE)
    else:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(forward_url, json=payload) as response:
                    message = await response.text()
                    if response.status == HTTP_STATUS_OK:
                        logging.info("[{}] {}".format(response.status, message))
                    else:
                        logging.error("[{}] {}".format(response.status, message))
        except Exception as error:
            logging.error(error)


class LPRMasterService:
    def __init__(self):
        self.database = Database()
        setup_log()

    async def notify(self, request):
        payload = await request.json()
        if not payload['data']:
            return return_message(message=INVALID_PAYLOAD_DATA_MESSAGE, status=HTTP_STATUS_BAD_REQUEST)
        await forward(payload)
        result = []
        for data in payload['data']:
            gate_id = data['gate_id']
            if gate_id:
                self.database.check_if_default_state_exist(gate_id)
                result.append(self.database.fetch_state(gate_id))
            else:
                return return_message(message=INVALID_GATE_ID_MESSAGE, status=HTTP_STATUS_BAD_REQUEST)
        return return_message(message=result)

    def reset_state(self):
        print('test')
        states = self.database.fetch_whole_state()
        if states:
            for state in states:
                gate_id = state[0]
                modified = state[1]
                current_dt = str_to_datetime(modified)
                limit = os.getenv("MAX_LIMIT_RESET_STATE", DEFAULT_MAX_LIMIT_RESET_STATE)
                added_current_dt = add_second_to_datetime(current_dt, int(limit))
                now = get_current_datetime()
                if not added_current_dt > now:
                    self.database.update_state(gate_id, setup_data_state(), modified)
