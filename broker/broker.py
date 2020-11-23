import json
import os

from kafka import KafkaConsumer

from database.database import setup_data_state
from misc.constant.value import STATUS_DETECTED, STATUS_UNDETECTED
from misc.helper.takeruHelper import get_current_datetime

bootstrap_server = "{}:{}".format(os.getenv("KAFKA_HOST"), os.getenv("KAFKA_PORT"))
consume_topic = os.getenv("KAFKA_CONSUME_TOPIC", "LPRResult")
consume_topic_image_result = os.getenv("KAFKA_CONSUME_TOPIC_IMAGE_RESULT")
consume_topic_group_id = os.getenv("KAFKA_CONSUME_TOPIC_GROUP_ID", "lpr-service-result")


class Broker:
    def __init__(self, logger, database):
        self.consumer = KafkaConsumer(bootstrap_servers=bootstrap_server, enable_auto_commit=False,
                                      group_id=consume_topic_group_id, consumer_timeout_ms=1000)
        self.consumer.subscribe([consume_topic, consume_topic_image_result])
        self.database = database
        self.logger = logger

    def consume(self):
        for message in self.consumer:
            data = json.loads(message.value)
            if message.topic == consume_topic:
                self.process_lpr_frame_result(data)
            else:
                self.process_lpr_image_result(data)

    def close_consumer(self):
        self.consumer.close()

    def process_lpr_frame_result(self, data):
        gate_id = data['gate_id']
        lpr_result = data['result']
        token_input = data['token_input'] if "token_input" in data else None
        updated_last_state = setup_data_state(status=STATUS_DETECTED, data=lpr_result)
        self.database.update_state(gate_id, updated_last_state, get_current_datetime())
        self.logger.info('data last state {} has been updated : {}'.format(gate_id, lpr_result))
        self.add_frame_output(gate_id, lpr_result, token_input)
        self.consumer.commit()

    def process_lpr_image_result(self, data):
        ticket_number = data['ticket_number']
        lpr_result = data['result']
        token = lpr_result['token']
        self.database.update_data_image_result(lpr_result, token, ticket_number)
        self.consumer.commit()

    def add_frame_output(self, gate_id, str_lpr_result, token_input):
        data_lpr_frame_input = self.database.get_data_lpr_frame_input_by_gate(gate_id)
        if data_lpr_frame_input:
            lpr_frame_input_id = data_lpr_frame_input['id']
            data_lpr = json.loads(str_lpr_result)
            vehicle_type = data_lpr['type']
            license_plate_number = data_lpr['license_plate_number']
            if license_plate_number != STATUS_UNDETECTED:
                token = data_lpr['token']
                state_id = self.database.get_state_id_by_gate(gate_id)
                self.database.add_data_lpr_frame_input(state_id, token_input)
                self.database.add_data_lpr_frame_output(lpr_frame_input_id=lpr_frame_input_id,
                                                        vehicle_type=vehicle_type,
                                                        license_plate_number=license_plate_number, token=token)
