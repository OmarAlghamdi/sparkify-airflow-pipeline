from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_cluster="",
                 aws_credentials="",
                 s3_bucket="", s3_key="",
                 table="", extras="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_cluster = redshift_cluster
        self.aws_credentials = aws_credentials
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.table = table
        self.extras = extras

    def execute(self, context):
        self.log.info("connecting to redshift")
        cluster = PostgresHook(postgres_conn_id=self.redshift_cluster)
        
        self.log.info("getting aws credentials")
        aws= AwsHook(aws_conn_id=self.aws_credentials)
        credentials = aws.get_credentials()

        self.log.info("rendering s3 path")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        
        self.log.info("fromating 'copy' sql query")
        copy_sql = """ COPY {table} FROM '{path}'
                        ACCESS_KEY_ID '{access_key}'
                        SECRET_ACCESS_KEY '{secret_key}'
                        {extras}""".format(table=self.table,
                                    path=s3_path,
                                    access_key=credentials.access_key,
                                    secret_key=credentials.secret_key,
                                    extras=self.extras)
        
        self.log.info("executing 'copy' sql query")
        cluster.run(copy_sql)





