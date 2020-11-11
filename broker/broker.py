import json
import os

from kafka import KafkaConsumer

from database.database import Database, setup_data_state
from misc.constant.value import STATUS_DETECTED
from misc.helper.takeruHelper import get_current_datetime

bootstrap_server = "{}:{}".format(os.getenv("KAFKA_HOST"), os.getenv("KAFKA_PORT"))
consume_topic = os.getenv("KAFKA_CONSUME_TOPIC", "LPRResult")
consume_topic_group_id = os.getenv("KAFKA_CONSUME_TOPIC_GROUP_ID", "lpr-service-result")


class Broker:
    def __init__(self, logger, database):
        self.consumer = KafkaConsumer(consume_topic, bootstrap_servers=bootstrap_server, enable_auto_commit=True,
                                      group_id=consume_topic_group_id, consumer_timeout_ms=1000)
        self.database = database
        self.logger = logger

    def consume(self):
        for message in self.consumer:
            data = json.loads(message.value)
            gate_id = data['gate_id']
            lpr_result = data['result']
            updated_last_state = setup_data_state(status=STATUS_DETECTED, data=lpr_result)
            self.database.update_state(gate_id, updated_last_state, get_current_datetime())
            self.logger.info('data last state {} has been updated : {}'.format(gate_id, lpr_result))

    def close_consumer(self):
        self.consumer.close()
