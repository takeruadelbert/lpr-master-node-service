import os

import aiohttp
from aiohttp import web

from database.database import Database, setup_data_state
from misc.constant.message import *
from misc.constant.value import *
from misc.helper.takeruHelper import *


def return_message(**kwargs):
    message = kwargs.get("message", "")
    status = kwargs.get("status", HTTP_STATUS_OK)
    return web.json_response({'message': message}, status=status)


class LPRMasterService:
    def __init__(self, logger):
        self.database = Database()
        self.logger = logger

    async def notify(self, request):
        payload = await request.json()
        if not payload['data']:
            self.logger.warning(INVALID_PAYLOAD_DATA_MESSAGE)
            return return_message(message=INVALID_PAYLOAD_DATA_MESSAGE, status=HTTP_STATUS_BAD_REQUEST)
        for data in payload['data']:
            gate_id = data['gate_id']
            if gate_id:
                self.database.check_if_default_state_exist(gate_id)
        response = await self.forward(payload)
        if response:
            return return_message(message=FORWARD_SUCCESS_MESSAGE)
        else:
            return return_message(message=ERROR_FORWARD_MESSAGE, status=HTTP_STATUS_BAD_REQUEST)

    async def get_data_last_state(self, request):
        payload = await request.json()
        if not payload['gate_id']:
            self.logger.warning(INVALID_PAYLOAD_DATA_MESSAGE)
            return return_message(status=HTTP_STATUS_BAD_REQUEST, message=INVALID_PAYLOAD_DATA_MESSAGE)
        result = []
        for gate_id in payload['gate_id']:
            if gate_id:
                if self.database.check_if_default_state_exist(gate_id, False):
                    result.append(self.database.fetch_state(gate_id))
                else:
                    self.logger.warning(INVALID_GATE_ID_MESSAGE)
                    return return_message(status=HTTP_STATUS_NOT_FOUND, message=INVALID_GATE_ID_MESSAGE)
            else:
                self.logger.warning(INVALID_GATE_ID_MESSAGE)
                return return_message(message=INVALID_GATE_ID_MESSAGE, status=HTTP_STATUS_BAD_REQUEST)
        return return_message(message=result)

    def reset_state(self):
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

    async def forward(self, payload):
        forward_url = os.getenv("FORWARD_URL", "")
        if not forward_url:
            self.logger.warning(INVALID_FORWARD_URL_MESSAGE)
            return return_message(status=HTTP_STATUS_BAD_REQUEST, message=INVALID_FORWARD_URL_MESSAGE)
        else:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post(forward_url, json=payload) as response:
                        message = await response.text()
                        if response.status == HTTP_STATUS_OK:
                            self.logger.info("[{}] {}".format(response.status, message))
                            return message
                        else:
                            self.logger.warning("[{}] {}".format(response.status, message))
                            return message
            except Exception as error:
                self.logger.error(error)
