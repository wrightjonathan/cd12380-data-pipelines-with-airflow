from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator

from airflow.models import Variable
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from operators import LoadFactOperator, LoadDimensionOperator, DataQualityOperator
from helpers import SqlQueries

create_table_sql_queries = {
    key: value
    for key, value in SqlQueries.__dict__.items()
    if "table_create" in key and not callable(value)
}

default_args = {
    "owner": "udacity",
    "start_date": pendulum.now(),
    "retires": 3,
    "email_on_retry": False,
}

bucket_name = Variable.get("s3_bucket_name")

@dag(
    default_args=default_args,
    description="Load and transform data in Redshift with Airflow",
    schedule_interval="0 * * * *",
    catchup=False,
)
def final_project():

    start_operator = EmptyOperator(task_id="Begin_execution")

    # Indicate all tables have been completed
    tables_created_operator = EmptyOperator(task_id="Tables_created")

    # Iterate all "create table" queries
    for query_name, sql in create_table_sql_queries.items():

        task = PostgresOperator(
            task_id=query_name,
            retry_delay=timedelta(minutes=5),
            postgres_conn_id="redshift",
            sql=sql,
        )
        start_operator >> task >> tables_created_operator


    # Stage from S3 to redshift
    stage_events_to_redshift = S3ToRedshiftOperator(
        task_id="Stage_events",
        retry_delay=timedelta(minutes=5),
        redshift_conn_id="redshift",
        aws_conn_id="aws_credentials",
        s3_bucket=bucket_name,
        s3_key="log-data",
        schema="PUBLIC",
        table="staging_events",
        copy_options=["FORMAT AS JSON 's3://{}/log_json_path.json'".format(bucket_name)],
        method="REPLACE"
    )

    stage_songs_to_redshift = S3ToRedshiftOperator(
        task_id='Stage_songs',
        retry_delay=timedelta(minutes=5),
        redshift_conn_id="redshift",
        aws_conn_id="aws_credentials",
        s3_bucket=bucket_name,
        s3_key="song-data",
        schema="PUBLIC",
        table="staging_songs",
        copy_options=["FORMAT AS JSON 's3://{}/song_json_path.json'".format(bucket_name)],
        method="REPLACE"
    )

    staging_complete_operator = EmptyOperator(task_id="Staging_complete")

    # Load fact table
    load_songplays_table = LoadFactOperator(
        task_id="Load_songplays_fact_table", retry_delay=timedelta(minutes=5)
    )

    # Load dimension tables
    load_user_dimension_table = LoadDimensionOperator(
        task_id="Load_user_dim_table",
        retry_delay=timedelta(minutes=5),
        sql=SqlQueries.user_dim_table_insert,
        truncate=True,
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id="Load_song_dim_table",
        retry_delay=timedelta(minutes=5),
        sql=SqlQueries.song_dim_table_insert,
        truncate=True,
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id="Load_artist_dim_table",
        retry_delay=timedelta(minutes=5),
        sql=SqlQueries.artist_dim_table_insert,
        truncate=True,
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id="Load_time_dim_table",
        retry_delay=timedelta(minutes=5),
        sql=SqlQueries.time_dim_table_insert,
        truncate=True,
    )

    loading_complete_operator = EmptyOperator(task_id="Loading_complete")

    # Quality checks

    user_quality_checks = DataQualityOperator(
        task_id="User_quality_checks",
        retry_delay=timedelta(minutes=5),
        sql=SqlQueries.user_data_quality_check,
    )

    time_quality_checks = DataQualityOperator(
        task_id="Time_quality_checks",
        retry_delay=timedelta(minutes=5),
        sql=SqlQueries.time_data_quality_check,
    )

    songs_quality_checks = DataQualityOperator(
        task_id="Songs_quality_checks",
        retry_delay=timedelta(minutes=5),
        sql=SqlQueries.songs_data_quality_check,
    )

    artists_quality_checks = DataQualityOperator(
        task_id="Artists_quality_checks",
        retry_delay=timedelta(minutes=5),
        sql=SqlQueries.artists_data_quality_check,
    )

    songplays_quality_checks = DataQualityOperator(
        task_id="Songplays_quality_checks",
        retry_delay=timedelta(minutes=5),
        sql=SqlQueries.songplays_data_quality_check,
    )

    stop_operator = EmptyOperator(task_id="Stop_execution")

    tables_created_operator >> stage_events_to_redshift >> staging_complete_operator
    tables_created_operator >> stage_songs_to_redshift >> staging_complete_operator

    # replace this with above
    #tables_created_operator >> staging_complete_operator

    staging_complete_operator >> load_songplays_table

    (
        load_songplays_table
        >> [
            load_user_dimension_table,
            load_song_dimension_table,
            load_artist_dimension_table,
            load_time_dimension_table,
        ]
        >> loading_complete_operator
    )

    (
        loading_complete_operator
        >> [
            user_quality_checks,
            time_quality_checks,
            songs_quality_checks,
            artists_quality_checks,
            songplays_quality_checks,
        ]
        >> stop_operator
    )


final_project_dag = final_project()
