import os
import sqlite3

from aiohttp import web

from misc.constant.message import *
from misc.constant.value import *
from misc.helper.takeruHelper import *


class LPRMasterService:
    def __init__(self):
        db_path = os.path.join(os.getcwd(), os.getenv("DB_NAME", DEFAULT_DB_FILE))
        self.db_connection = sqlite3.connect(db_path)

    async def notify(self, request):
        payload = await request.json()
        if not payload['data']:
            return self.return_message(message=INVALID_PAYLOAD_DATA_MESSAGE, status=HTTP_STATUS_NO_CONTENT)
        result = []
        for data in payload['data']:
            gate_id = data['gate_id']
            if gate_id:
                self.check_if_default_state_exist(gate_id)
                result.append(self.fetch_state(gate_id))
            else:
                return self.return_message(message=INVALID_GATE_ID_MESSAGE, status=HTTP_STATUS_BAD_REQUEST)
        return self.return_message(message=result)

    def forward(self, payload):
        forward_url = os.getenv("FORWARD_URL", "")
        if forward_url:
            return self.return_message(message=OK_MESSAGE)
        else:
            return self.return_message(message=INVALID_FORWARD_URL_MESSAGE, status=HTTP_STATUS_NOT_FOUND)

    async def test_update_state(self, request):
        payload = await request.json()
        updated_state = payload['state']
        gate_id = payload['gate_id']
        self.update_state(gate_id, updated_state)

    def return_message(self, **kwargs):
        message = kwargs.get("message", "")
        status = kwargs.get("status", HTTP_STATUS_OK)
        return web.json_response({'message': message}, status=status)

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
                    self.update_state(gate_id, DEFAULT_STATE, modified)
