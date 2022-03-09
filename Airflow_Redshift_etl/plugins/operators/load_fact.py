from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):    
    '''
    takes data from the staging tables in Redshift
    to the fact tables in the same redshift cluster.
    it uses SQL queries in order to create 
    and insert data.
    Insertion query should be just a SELECT
    statement.
    '''

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 table="",
                 redshift_conn_id="",
                 insert_query="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.insert_query = insert_query

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        self.log.info("Inserting data...")
        redshift_hook.run(f"INSERT INTO {self.table}({self.insert_query})")
        
        self.log.info("Completed data insertion")