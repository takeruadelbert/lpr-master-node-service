DEFAULT_STATE = "No Vehicle Detected"
DEFAULT_PORT = 8080
DEFAULT_DB_FILE = "data-state.sqlite3"
DEFAULT_APP_NAME = "master-node"

HTTP_STATUS_OK = 200
HTTP_STATUS_BAD_REQUEST = 400
HTTP_STATUS_NOT_FOUND = 404
HTTP_STATUS_INTERNAL_SERVER_ERROR = 500
HTTP_STATUS_UNPROCESSABLE_ENTITY = 412

DEFAULT_DATETIME_FORMAT = "YYYY-MM-DD HH:mm:ss"
DEFAULT_DATE_FORMAT = "YYYY-MM-DD"
DEFAULT_MAX_LIMIT_RESET_STATE = 10  # in seconds
DEFAULT_RESET_STATE_SCHEDULER_TIME = 1  # in seconds
DEFAULT_KAFKA_CONSUME_DELAY_TIME = 1  # in seconds
DEFAULT_PREFIX_BASE64 = 'data:image/jpeg;base64,'

STATUS_DETECTED = "detected"
STATUS_UNDETECTED = "undetected"
STATUS_DONE = "DONE"
STATUS_UNKNOWN = "unknown"
STATUS_RUNNING = "RUNNING"
STATUS_STOPPED = "STOPPED"
