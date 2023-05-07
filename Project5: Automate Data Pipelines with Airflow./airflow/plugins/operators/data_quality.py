from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id='',
                 tables=None,
                 dq_checks=None,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
        self.dq_checks = dq_checks

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for table in self.tables:
            self.log.info(f"Running data quality checks on table {table}")
            for check in self.dq_checks:
                check_sql = check.get('check_sql')
                expected_result = check.get('expected_result')
                records = redshift_hook.get_records(check_sql.format(table))
                num_records = records[0][0]
                if num_records != expected_result:
                    raise ValueError(f"Data quality check failed for table {table}. Expected {expected_result} records, but got {num_records} records.")
            self.log.info(f"Data quality checks passed for table {table}")