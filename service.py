import aiohttp
from aiohttp import web

from database.database import setup_data_state
from misc.constant.message import *
from misc.constant.value import *
from misc.helper.takeruHelper import *


def return_message(**kwargs):
    message = kwargs.get("message", OK_MESSAGE)
    status = kwargs.get("status", HTTP_STATUS_OK)
    data = kwargs.get("data", [])
    return web.json_response({'message': message, 'data': data}, status=status)


class LPRMasterService:
    def __init__(self, logger, database):
        self.database = database
        self.logger = logger

    async def register(self, request):
        payload = await request.json()
        if not payload['data']:
            self.logger.warning(INVALID_PAYLOAD_DATA_MESSAGE)
            return return_message(message=INVALID_PAYLOAD_DATA_MESSAGE, status=HTTP_STATUS_BAD_REQUEST)
        for data in payload['data']:
            gate_id = data['gate_id']
            url_stream = data['url_stream']
            if gate_id and url_stream:
                self.database.check_if_default_state_exist(gate_id, url_stream)
        return return_message(message=REGISTER_SUCCESS)

    async def delete_gate_id(self, request):
        payload = await request.json()
        self.logger.info('receiving data payload : {}'.format(payload))
        if not payload['gate_id']:
            self.logger.warning(INVALID_PAYLOAD_DATA_MESSAGE)
            return return_message(status=HTTP_STATUS_BAD_REQUEST, message=INVALID_PAYLOAD_DATA_MESSAGE)
        gate_id = payload['gate_id']
        if self.database.check_if_default_state_exist(gate_id, None, False):
            if self.database.delete_gate_id(gate_id):
                message = '{} {}'.format(MESSAGE_DELETE_GATE_ID_SUCCESS, gate_id)
                return return_message(message=message)
            else:
                message = '{} {}'.format(MESSAGE_DELETE_GATE_ID_FAILED, gate_id)
                return return_message(status=HTTP_STATUS_UNPROCESSABLE_ENTITY, message=message)
        else:
            message = '{} : {}'.format(MESSAGE_GATE_ID_NOT_FOUND, gate_id)
            self.logger.warning(message)
            return return_message(status=HTTP_STATUS_NOT_FOUND, message=message)

    async def get_data_last_state(self, request):
        payload = await request.json()
        self.logger.info('received data request : {}'.format(payload))
        if not payload['gate_id']:
            self.logger.warning(INVALID_PAYLOAD_DATA_MESSAGE)
            return return_message(status=HTTP_STATUS_BAD_REQUEST, message=INVALID_PAYLOAD_DATA_MESSAGE)
        result = []
        for gate_id in payload['gate_id']:
            if gate_id:
                if self.database.check_if_default_state_exist(gate_id, None, False):
                    result.append(self.database.fetch_state(gate_id))
                else:
                    message = "{} : '{}'".format(INVALID_GATE_ID_MESSAGE, gate_id)
                    self.logger.warning(message)
                    return return_message(status=HTTP_STATUS_NOT_FOUND, message=message)
            else:
                self.logger.warning(INVALID_GATE_ID_MESSAGE)
                return return_message(message=INVALID_GATE_ID_MESSAGE, status=HTTP_STATUS_BAD_REQUEST)
        self.logger.info('success fetch data last state.')
        return return_message(message=OK_MESSAGE, data=result)

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

    async def forward(self, request):
        forward_url = os.getenv("FORWARD_URL", "")
        if not forward_url:
            self.logger.warning(INVALID_FORWARD_URL_MESSAGE)
            return return_message(status=HTTP_STATUS_BAD_REQUEST, message=INVALID_FORWARD_URL_MESSAGE)
        else:
            payload = await request.json()
            if not payload['filename']:
                self.logger.warning(INVALID_FILENAME_MESSAGE)
                return return_message(status=HTTP_STATUS_BAD_REQUEST, message=INVALID_FILENAME_MESSAGE)
            encoded_image = payload['encoded_file']
            if not encoded_image:
                self.logger.warning(INVALID_DATA_IMAGE_MESSAGE)
                return return_message(status=HTTP_STATUS_BAD_REQUEST, message=INVALID_DATA_IMAGE_MESSAGE)
            if DEFAULT_PREFIX_BASE64 not in encoded_image:
                payload['encoded_file'] = '{}{}'.format(DEFAULT_PREFIX_BASE64, encoded_image)
            self.logger.info('forwarding data to process service : {}'.format(payload))
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post(forward_url, json=payload) as response:
                        temp_response = await response.text()
                        temp = json.loads(temp_response)
                        message = temp['message']
                        data = temp['data']
                        if response.status == HTTP_STATUS_OK:
                            self.logger.info("[{}] {}".format(response.status, message))
                            return return_message(message=message, data=data)
                        else:
                            self.logger.warning("[{}] {}".format(response.status, message))
                            return return_message(status=response.status, message=message)
            except Exception as error:
                self.logger.error(error)
                return return_message(status=HTTP_STATUS_UNPROCESSABLE_ENTITY, message=ERROR_FORWARD_MESSAGE)

    async def get_data_image_result_by_ticket_number(self, request):
        payload = await request.json()
        ticket_number = payload['ticket_number']
        if not ticket_number:
            self.logger.warning(INVALID_TICKET_NUMBER_MESSAGE)
            return return_message(status=HTTP_STATUS_BAD_REQUEST, message=INVALID_TICKET_NUMBER_MESSAGE)
        data = self.database.fetch_data_image_result_by_ticket_number(ticket_number)
        if data:
            return return_message(data=data)
        else:
            return return_message(status=HTTP_STATUS_BAD_REQUEST, message=MESSAGE_TICKET_NUMBER_NOT_FOUND)
