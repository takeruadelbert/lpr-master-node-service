import logging
import os
import sqlite3

import aiohttp
from aiohttp import web

from misc.constant.message import *
from misc.constant.value import *
from misc.helper.takeruHelper import *


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


def setup_data_state(**kwargs):
    status = kwargs.get("status", "Undetected")
    data = kwargs.get("data", DEFAULT_STATE)
    return json.dumps({'status': status, 'data': data})


class LPRMasterService:
    def __init__(self):
        db_path = os.path.join(os.getcwd(), os.getenv("DB_NAME", DEFAULT_DB_FILE))
        self.db_connection = sqlite3.connect(db_path)
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
                self.check_if_default_state_exist(gate_id)
                result.append(self.fetch_state(gate_id))
            else:
                return return_message(message=INVALID_GATE_ID_MESSAGE, status=HTTP_STATUS_BAD_REQUEST)
        return return_message(message=result)

    async def test_update_state(self, request):
        payload = await request.json()
        updated_state = payload['state']
        gate_id = payload['gate_id']
        self.update_state(gate_id, updated_state)

    def add_default_state(self, gate_id):
        self.db_connection.execute("INSERT INTO state (last_state, gate_id) VALUES (?, ?)",
                                   (DEFAULT_STATE, gate_id))
        self.db_connection.commit()

    def check_if_default_state_exist(self, gate_id):
        cursor = self.db_connection.cursor()
        cursor.execute("SELECT gate_id FROM state WHERE gate_id = ?", (gate_id,))
        result = cursor.fetchone()
        if not result:
            self.add_default_state(gate_id)

    def update_state(self, gate_id, state, modified):
        self.db_connection.execute("UPDATE state SET last_state = ?, modified = ? WHERE gate_id = ?",
                                   (state, modified, gate_id))
        self.db_connection.commit()

    def fetch_whole_state(self):
        cursor = self.db_connection.cursor()
        cursor.execute("SELECT gate_id, modified FROM state")
        return cursor.fetchall()

    def fetch_state(self, gate_id):
        cursor = self.db_connection.cursor()
        cursor.execute("SELECT gate_id, last_state FROM state WHERE gate_id = ?", (gate_id,))
        result = cursor.fetchone()
        last_state = json.loads(result[1]) if check_if_string_is_json(result[1]) else result[1]
        return {
            'gate_id': result[0],
            'last_state': last_state
        }

    def reset_state(self):
        states = self.fetch_whole_state()
        if states:
            for state in states:
                gate_id = state[0]
                modified = state[1]
                current_dt = str_to_datetime(modified)
                limit = os.getenv("MAX_LIMIT_RESET_STATE", DEFAULT_MAX_LIMIT_RESET_STATE)
                added_current_dt = add_second_to_datetime(current_dt, int(limit))
                now = get_current_datetime()
                if not added_current_dt > now:
                    self.update_state(gate_id, setup_data_state(), modified)
