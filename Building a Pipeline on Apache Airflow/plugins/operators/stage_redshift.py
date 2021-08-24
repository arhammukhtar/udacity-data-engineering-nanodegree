from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    copy_sql = """
        COPY {table}
        FROM '{s3_url}'
        ACCESS_KEY_ID '{key_id}'
        SECRET_ACCESS_KEY '{key_secret}'
        JSON '{json_config}'
    """
    @apply_defaults
    def __init__(self,
                target_table,
                s3_bucket,
                s3_key,
                json_path,
                redshift_conn_id,
                aws_credentials,
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.target_table = target_table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key   
        self.json_path = json_path
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials = aws_credentials
        # Map params here
        # Example:
        # self.conn_id = conn_id

    def execute(self, context):

        aws_hook = AwsHook(self.aws_credentials)
        credentials = aws_hook.get_credentials()

        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)

        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)

        sql_copy = StageToRedshiftOperator.copy_sql.format(
            table = self.target_table,
            s3_url = s3_path,
            key_id = credentials.access_key,
            key_secret = credentials.secret_key,
            json_config = self.json_path
        )
        self.log.info(f"Starting sql copy command to {self.target_table} table")

        redshift.run(sql_copy)

        self.log.info(f"Completed Staging to {self.target_table} table")


