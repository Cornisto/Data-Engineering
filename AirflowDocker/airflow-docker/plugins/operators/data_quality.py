from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 dq_checks=None,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        if dq_checks == None:
            self.dq_checks = []
        else:
            self.dq_checks = dq_checks

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        error_count = 0
        failed_tests = []
        
        for check in self.dq_checks:
            records = redshift.get_records(check['check_sql'])
            if len(records) < 1 or len(records[0]) < 1:
                error_count += 1
                failed_tests.append(check['check_sql'])
                failed_tests.append(f"Data quality check failed. Query {check['check_sql']} returned no results.")
                continue
            num_records = records[0][0]
            if num_records != check['expected_result']:
                error_count += 1
                failed_tests.append(f"Data quality check failed. Query {check['check_sql']} returned {num_records} records instead of {check['expected_result']}.")
            
        if error_count > 0:
            self.log.info('Tests failed')
            self.log.info(failed_tests)
            raise ValueError('Data quality check failed.')
        else:                  
            self.log.info(f"Data quality check passed.")
