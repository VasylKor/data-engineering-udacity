from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    '''
    This operator takes as input the conn_id from redshift
    and a list of dictionaries containing SQL queries as counts, 
    an expected value and the behaviour expected.
    behaviour is expressed by the key "how" and can be:
    -less
    -more
    -equal
    It then proceeds to check the queries against their expected
    value.
    It will add any unsuccessful check in a list.
    If there are unsuccesful checks, we will write on the logs
    every check (taken from the list) not passed and raise
    an Exception ath the end.
    Otherwise the operator will print a message stating
    the successful outcome.
    '''

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 checks="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.checks = checks

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        errors= []
        i = 0
        for check in self.checks:
            query=check["query"]
            how=check["how"]
            expected_value=check["expected_value"]
            records = redshift_hook.get_records(query) 
            if how == "equal":
                if records[0][0] != expected_value:
                    errors.append(f"The query '{query}' didn't return the expected result '{how} to {expected_value}'")
            elif how == "less":
                if records[0][0] >= expected_value:
                    errors.append(f"The query '{query}' didn't return the expected result '{how} than {expected_value}'")
            elif how == "more":
                if records[0][0] <= expected_value:
                    errors.append(f"The query '{query}' didn't return the expected result '{how} than {expected_value}'")
            else:
                raise Exception("value of 'how' in checks is not expected")
                    
            
        if errors:
            for err in errors:
                self.log.info(err)
            raise Exception('Data hasn\'t passed the check, see previous logs')
        else:
            self.log.info('Quality checks passed')