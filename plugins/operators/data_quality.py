from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook


class DataQualityOperator(BaseOperator):

    def __init__(
        self, redshift_conn_id: str = "redshift", sql: str = "", *args, **kwargs
    ):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        self.log.info("Starting data quality check...")

        redshift_hook = RedshiftSQLHook(redshift_conn_id=self.redshift_conn_id)
        conn = redshift_hook.get_conn()
        cursor = conn.cursor()

        # Execute the SQL
        cursor.execute(self.sql)
        column_names = [desc[0] for desc in cursor.description]
        records = cursor.fetchall()

        self.log.info(f"Column names: {column_names}")
        self.log.info(f"Records: {records}")

        # Example logic to check for NULL values
        for i, column_name in enumerate(column_names):
            null_count = records[0][i]  # Assuming only one row is returned
            if null_count > 0:
                raise ValueError(
                    f"Data quality check failed: Column '{column_name}' has {null_count} NULL values."
                )

        self.log.info("Data quality check passed with 0 NULL values found.")

        if cursor:
            cursor.close()
        if conn:
            conn.close()
