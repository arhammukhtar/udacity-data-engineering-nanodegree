from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 checks=[],
                 redshift_conn_id="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.checks = checks
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        
        redshift_hook = PostgresHook(self.redshift_conn_id)
        errors = 0
        failing_checks = []
        
        for check in self.checks:
            sql = check['check_sql']
            exp_result = check['expected_result']

            try:
                self.log.info(f"Running query: {sql}")
                records = redshift_hook.get_records(sql)[0]
            except Exception as e:
                self.log.info(f"Query failed with exception: {e}")

            if exp_result != records[0]:
                errors = errors +  1

        if errors > 0:
            self.log.info('Tests failed')
        else:
            self.log.info("All data quality checks passed")