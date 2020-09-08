from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_cluster="",
                 table="",
                 transform_sql="",
                 append=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_cluster = redshift_cluster
        self.table = table
        self.transform_sql = transform_sql
        self.append = append

    def execute(self, context):
        self.log.info("connecting to redshift")
        cluster = PostgresHook(postgres_conn_id=self.redshift_cluster)
        
        if not self.append:
            self.log.info("deleting old records")
            cluster.run("DELETE FROM {}".format(self.table)) 

        self.log.info("fromating sql query")
        sql_query = """INSERT INTO {table}
        {query}""".format(table=self.table,
                    query=self.transform_sql)
        
        self.log.info("executing sql query")
        cluster.run(sql_query)
