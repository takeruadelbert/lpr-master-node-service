import json
import os

from kafka import KafkaConsumer

from database.database import setup_data_state
from misc.constant.value import STATUS_DETECTED
from misc.helper.takeruHelper import get_current_datetime

bootstrap_server = "{}:{}".format(os.getenv("KAFKA_HOST"), os.getenv("KAFKA_PORT"))
consume_topic = os.getenv("KAFKA_CONSUME_TOPIC", "LPRResult")
consume_topic_image_result = os.getenv("KAFKA_CONSUME_TOPIC_IMAGE_RESULT")
consume_topic_group_id = os.getenv("KAFKA_CONSUME_TOPIC_GROUP_ID", "lpr-service-result")
consume_topic_image_result_group_id = os.getenv("KAFKA_CONSUME_TOPIC_IMAGE_RESULT_GROUP_ID")


class Broker:
    def __init__(self, logger, database):
        self.consumer = KafkaConsumer(bootstrap_servers=bootstrap_server, enable_auto_commit=True,
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
        updated_last_state = setup_data_state(status=STATUS_DETECTED, data=lpr_result)
        self.database.update_state(gate_id, updated_last_state, get_current_datetime())
        self.logger.info('data last state {} has been updated : {}'.format(gate_id, lpr_result))

    def process_lpr_image_result(self, data):
        ticket_number = data['ticket_number']
        lpr_result = data['result']
        lpr_result_in_string = json.dumps(lpr_result)
        token = lpr_result['token']
        self.database.update_data_image_result(lpr_result_in_string, token, ticket_number)
        self.logger.info("data image result for ticket number '{}' has been updated.".format(ticket_number))
