from airflow.models import BaseOperator
from airflow.utils.context import Context
from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from helpers import SqlQueries
import re


class LoadDimensionOperator(BaseOperator):

    def __init__(
        self,
        *,
        redshift_conn_id: str = "redshift",
        sql: str = "",
        truncate: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.truncate = truncate

    def execute(self, context: Context) -> None:

        # Regular expression to extract the table name after 'INSERT INTO'
        pattern = r"INSERT\s+INTO\s+(\w+)\s*\("

        # Search for the pattern in the SQL query
        match = re.search(pattern, self.sql)

        if not match:
            error_msg = "Table name not found in the SQL query."
            self.log.error(error_msg)
            raise AirflowException(error_msg)

        table_name = match.group(1)

        self.log.info(
            "LoadDimensionOperator - loading to dimension table {}".format(table_name)
        )
        redshift_hook = RedshiftSQLHook(redshift_conn_id=self.redshift_conn_id)

        # Optional truncate
        if self.truncate:
            self.log.info("truncating dimension table {}".format(table_name))
            redshift_hook.run("TRUNCATE {};".format(table_name))

        # Insert
        redshift_hook.run(self.sql)
        self.log.info("insert to dimension table {} complete".format(table_name))
