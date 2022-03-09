from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    
    '''
    takes data from the staging tables in Redshift
    to the dimension tables in the same redshift cluster.
    it uses SQL queries in order to create 
    and insert data.
    Insertion query should be just a SELECT
    statement.
    Gives the possibility to truncate or
    just insert data through "operation"
    parameter which accepts:
    -truncate
    -append
    Default "append".
    '''

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 table="",
                 redshift_conn_id="",
                 insert_query="",
                 operation="append",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.insert_query = insert_query
        self.operation = operation

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        if self.operation == 'truncate':
            self.log.info("Removing data...")
            redshift_hook.run(f"TRUNCATE {self.table}")
            
                
        self.log.info("Inserting data...")
        redshift_hook.run(f"INSERT INTO {self.table}({self.insert_query})")
            
        
        self.log.info("Completed data insertion")