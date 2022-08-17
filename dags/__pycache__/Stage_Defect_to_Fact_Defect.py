from airflow import DAG
from datetime import timedelta
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago
import datetime

args = {'owner' : 'airflow'}

default_args = {
    'owner': 'airflow',    
    #'start_date': airflow.utils.dates.days_ago(2),
    # 'end_date': datetime(),
    # 'depends_on_past': False,
    #'email': ['airflow@example.com'],
    #'email_on_failure': False,
    # 'email_on_retry': False,
    # If a task fails, retry it once after waiting
    # at least 5 minutes
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='Defect_Fact_Insert',
    start_date=datetime.datetime(2022, 8, 15),
    schedule_interval='@daily'
) as dag:
    copy_insert_query = """
        INSERT INTO fact_defect_event
        SELECT "jobID",
        "itemID",
        "jobID" || '-' || "itemID" AS jobitemid,
        "customerID",
        datefound,
        inspectshift,
        inspectgluer,
        defectcode,
        defectdesc,
        caseqty,
        caseid,
        defectivesamples,
        totalsamples
        FROM "Stage_Defect_Event";"""

    drop_table_query = """
        DROP TABLE IF EXISTS public."Stage_Defect_Event";
        CREATE TABLE IF NOT EXISTS public."Stage_Defect_Event"(
                "jobID" bigint,
                "itemID" text COLLATE pg_catalog."default",
                "customerID" text COLLATE pg_catalog."default",
                datefound text COLLATE pg_catalog."default",
                inspectshift text COLLATE pg_catalog."default",
                inspectgluer text COLLATE pg_catalog."default",
                caseqty bigint,
                caseid bigint,
                defectcode text COLLATE pg_catalog."default",
                defectivesamples bigint,
                totalsamples bigint,
                defectdesc character varying(30) COLLATE pg_catalog."default"
            );
        """


    Copy_Insert_Task = PostgresOperator(
        sql = copy_insert_query,
        task_id = "Copy_Insert_Task",
        postgres_conn_id = "CurtisDW",
        dag = dag
    )

    Drop_Stage_Defect_Task = PostgresOperator(
        sql = drop_table_query,
        task_id = "Drop_Stage_Defect_Task",
        postgres_conn_id = "CurtisDW",
        dag = dag
    )

Copy_Insert_Task >> Drop_Stage_Defect_Task