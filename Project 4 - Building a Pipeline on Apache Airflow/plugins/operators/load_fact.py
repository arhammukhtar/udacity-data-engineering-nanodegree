from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    insert_sql = """
        INSERT INTO {}
        {};
    """
    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 sql_query,
                 table_name,
                 insert_columns,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.table_name = table_name
        self.insert_columns = insert_columns
        # Map params here
        # Example:
        # self.conn_id = conn_id

    def execute(self, context):

        self.log.info('LoadFactOperator not implemented yet')
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        sql_run = LoadFactOperator.insert_sql.format(self.table_name, self.sql_query)
        self.log.info(f"Starting Load of Fact Table table")
        redshift.run(sql_run)
        self.log.info(f"Completed Load of Fact table")
