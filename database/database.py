import pymysql.cursors

from misc.constant.value import *
from misc.helper.takeruHelper import *

db_host = os.getenv("DB_HOST")
db_username = os.getenv("DB_USERNAME")
db_password = os.getenv("DB_PASSWORD")
db_name = os.getenv("DB_NAME")


def setup_data_state(**kwargs):
    status = kwargs.get("status", STATUS_UNDETECTED)
    data = kwargs.get("data", DEFAULT_STATE)
    return json.dumps({'status': status, 'data': data})


class Database:
    def __init__(self, logger):
        try:
            self.logger = logger
            self.db_connection = pymysql.connect(host=db_host, user=db_username, password=db_password, db=db_name,
                                                 autocommit=True, port=3306)
            self.db_cursor = self.db_connection.cursor()
        except Exception as error:
            self.logger.error(error)

    def add_default_state(self, gate_id, url):
        self.logger.info('adding {}'.format(gate_id))
        self.db_cursor.execute("INSERT INTO state (last_state, gate_id, url) VALUES (%s, %s, %s)",
                               (DEFAULT_STATE, gate_id, url))
        self.db_connection.commit()

    def check_if_default_state_exist(self, gate_id, url=None, auto_add=True):
        self.db_cursor.execute("SELECT gate_id FROM state WHERE gate_id = %s", (gate_id,))
        result = self.db_cursor.fetchone()
        if auto_add:
            if not result:
                self.add_default_state(gate_id, url)
                self.logger.info('{} has been added into database.'.format(gate_id))
        return False if not result else True

    def update_state(self, gate_id, state, modified):
        self.db_cursor.execute("UPDATE state SET last_state = %s, modified = %s WHERE gate_id = %s",
                               (state, modified, gate_id))
        self.db_connection.commit()

    def fetch_whole_state(self):
        self.db_cursor.execute("SELECT gate_id, modified FROM state")
        return self.db_cursor.fetchall()

    def fetch_state(self, gate_id):
        self.db_cursor.execute("SELECT gate_id, last_state FROM state WHERE gate_id = %s", (gate_id,))
        result = self.db_cursor.fetchone()
        last_state = json.loads(result[1]) if check_if_string_is_json(result[1]) else result[1]
        return {
            'gate_id': result[0],
            'last_state': last_state
        }

    def delete_gate_id(self, gate_id):
        try:
            self.db_cursor.execute("DELETE FROM state WHERE gate_id = %s", (gate_id,))
            self.db_connection.commit()
            return True
        except Exception as error:
            self.db_connection.rollback()
            self.logger.error(error)
            return False

    def update_data_image_result(self, result, token, ticket_number):
        self.db_cursor.execute("UPDATE image_result SET result = %s, token = %s, status = %s WHERE ticket_number = %s",
                               (result, token, STATUS_DONE, ticket_number))
        self.db_connection.commit()

    def fetch_data_image_result_by_ticket_number(self, ticket_number):
        self.db_cursor.execute("SELECT ticket_number, result, token, status FROM image_result WHERE ticket_number = %s",
                               (ticket_number,))
        result = self.db_cursor.fetchone()
        if result:
            return {
                'ticket_number': result[0],
                'result': json.loads(result[1]) if check_if_string_is_json(result[1]) else result[1],
                'token': result[2],
                'status': result[3]
            }
        else:
            return None
