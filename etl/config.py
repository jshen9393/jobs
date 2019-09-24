
from etl.utils.environ import get_str_value, get_bool_value, get_int_value


# PostGres connection
PG_DB_HOST = get_str_value('PG_DB_HOST', None)
PG_DB_PORT = get_int_value('PG_DB_PORT', 5432)
PG_DB_NAME = get_str_value('PG_DB_NAME', None)
PG_DB_USER = get_str_value('PG_DB_USER', None)
PG_DB_PASS = get_str_value('PG_DB_PASS', None)

# Indeed
INDEED_PUB_ID = get_str_value('INDEED_PUB_ID', None)

# DB connection timeout in seconds
ETL_DB_TIMEOUT = get_int_value('ETL_DB_TIMEOUT', 30)
# DB connection retries amount
ETL_DB_RETRIES = get_int_value('ETL_DB_RETRIES', 3)