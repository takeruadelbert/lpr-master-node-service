import sqlite3

from misc.constant.value import *
from misc.helper.takeruHelper import *

db_path = os.path.join(os.getcwd(), os.getenv("DB_NAME", DEFAULT_DB_FILE))


def setup_data_state(**kwargs):
    status = kwargs.get("status", STATUS_UNDETECTED)
    data = kwargs.get("data", DEFAULT_STATE)
    return json.dumps({'status': status, 'data': data})


class Database:
    def __init__(self, logger):
        self.db_connection = sqlite3.connect(db_path)
        self.logger = logger

    def add_default_state(self, gate_id):
        self.logger.info('adding {}'.format(gate_id))
        self.db_connection.execute("INSERT INTO state (last_state, gate_id) VALUES (?, ?)",
                                   (DEFAULT_STATE, gate_id))
        self.db_connection.commit()

    def check_if_default_state_exist(self, gate_id, auto_add=True):
        cursor = self.db_connection.cursor()
        cursor.execute("SELECT gate_id FROM state WHERE gate_id = ?", (gate_id,))
        result = cursor.fetchone()
        if auto_add:
            if not result:
                self.add_default_state(gate_id)
                self.logger.info('{} has been added into database.'.format(gate_id))
        return False if not result else True

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
