from airflow.models import BaseOperator
from airflow.utils.context import Context
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from helpers import SqlQueries


class LoadFactOperator(BaseOperator):

    def __init__(
        self,
        *,
        redshift_conn_id: str = "redshift",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context: Context) -> None:
        self.log.info("LoadFactOperator - appending to fact table songplays")
        redshift_hook = RedshiftSQLHook(redshift_conn_id=self.redshift_conn_id)
        redshift_hook.run(SqlQueries.songplay_table_insert)
