from datetime import timedelta

DEFAULT_ARGS = {
    'owner': 'admin',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=10),
}
