from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_cluster="",
                 tables=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_cluster = redshift_cluster
        self.tables = tables

    def execute(self, context):
        self.log.info("connecting to redshift")
        cluster = PostgresHook(postgres_conn_id=self.redshift_cluster)
        
        for table in self.tables:
            records = cluster.get_records("SELECT COUNT(*) FROM {}".format(table))        
            if len(records) < 1 or len(records[0]) < 1:
                self.log.error("{} returned no results".format(table))
                raise ValueError("Data quality check failed. {} returned no results".format(table))
            num_records = records[0][0]
            if num_records == 0:
                self.log.error("No records present in destination table {}".format(table))
                raise ValueError("No records present in destination {}".format(table))
            self.log.info("Data quality on table {} check passed with {} records".format(table, num_records))