from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    INSERT_DIM_SQL = """
        INSERT INTO {} {};
        """
    truncate_sql = """
        TRUNCATE TABLE {};
    """
    
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 table,
                 redshift_conn_id,
                 sql_query,
                 truncate,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.truncate = truncate
        # Map params here
        # Example:
        # self.conn_id = conn_id

    def execute(self, context):
        
        self.log.info('LoadFactOperator not implemented yet')
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        if self.truncate_table:
            self.log.info(f"Truncating dimension table: {self.table}")
            redshift.run(LoadDimensionOperator.truncate_sql.format(self.table))
            
        sql_run = LoadDimensionOperator.INSERT_DIM_SQL.format(self.table, self.sql_query)

        self.log.info(f"Starting Load of Fact Dimension table {self.table}")
        redshift.run(sql_run)
        self.log.info(f"Completed Load of Dimension table {self.table}")
