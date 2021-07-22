from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 create_stmt="",
                 sql="",
                 insert_mode="append",
                 *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.create_stmt = create_stmt
        self.sql = sql
        self.insert_mode = insert_mode

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Creating dimension table if not exists {self.table}")
        redshift.run(self.create_stmt)

        if self.insert_mode == "truncate":
            self.log.info(f"Deleting data from dimension table {self.table}")
            redshift.run("TRUNCATE {};".format(self.table))

        self.log.info(f"Inserting data into dimension table {self.table}")
        redshift.run(f"INSERT INTO {self.table} {self.sql}")
