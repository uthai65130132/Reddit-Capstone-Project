
from airflow import DAG
from airflow.utils import timezone
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

import praw
from datetime import datetime
import pytz
import json
# from requests import Session




def _get_Data_Submission_Reddit(**context):
    # session = Session()
    # session.verify = "/path/to/certfile.pem"
    reddit = praw.Reddit(
        client_id=Variable.get("client_id"),
        client_secret=Variable.get("client_secret"),
        password=Variable.get("password"),
        user_agent=Variable.get("user_agent"),
        username=Variable.get("username"),
    )
    print(reddit.read_only)
    # print("ID : ",submission.id)
    # print("Title : ",submission.title)
    # print("Score : ",submission.score)
    # print("Author : ",submission.author.name)
    # print("Description : ",submission.selftext)
    # print("Create Date : ",datetime.fromtimestamp(int(submission.created_utc),tz=pytz.UTC))
    list_datareddit=[]
    for submission in reddit.subreddit("dataengineering").new(limit=20):
        listdata={
        "submission_ID":submission.id,
        "submission_Title":submission.title,
        "submission_Score":submission.score,
        "submission_Author":submission.author.name,
        "submission_selftext":submission.selftext,
        "Create Date":str(datetime.fromtimestamp(int(submission.created_utc),tz=pytz.UTC))
        }
        list_datareddit.append(listdata)
    timestamp = context["execution_date"]
    with open(f"/opt/airflow/dags/staging/Submission_Reddit_Data_{str(timestamp)}.json", "w") as f:
        json.dump(list_datareddit, f)

    return f"/opt/airflow/dags/staging/Submission_Reddit_Data_{timestamp}.json"

def _Create_Submission_table(**context):
    pg_hook = PostgresHook(
        postgres_conn_id="my_postgres_conn",
        schema="postgres"
    )
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    sql = """
        CREATE TABLE IF NOT EXISTS Submission (
            submission_ID varchar(30) UNIQUE PRIMARY KEY,
            submission_Title varchar(500) NOT NULL,
            submission_Score int NULL,
            submission_Author varchar(200) NOT NULL,
            submission_selftext Text NOT NULL,
            Create_Date timestamp NOT NULL
        )
    """

    # sql = """
    #     DROP TABLE Submission
    # """
    cursor.execute(sql)
    connection.commit()

def _load_data_to_postgres(**context):
    ti = context["ti"]
    file_name = ti.xcom_pull(task_ids="get_Data_Submission_Reddit", key="return_value")
    print(file_name)

    pg_hook = PostgresHook(
        postgres_conn_id="my_postgres_conn",
        schema="postgres"
    )
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    with open(file_name, "r") as f:
        data = json.load(f)

    for i in range(len(data)):
        ID=data[i]["submission_ID"]
        Title=data[i]["submission_Title"]
        Score=data[i]["submission_Score"]
        Author=data[i]["submission_Author"]
        selftext=data[i]["submission_selftext"]
        Create_Date=data[i]["Create Date"]

        sql = """
            INSERT INTO Submission (submission_ID, 
                                    submission_Title,
                                    submission_Score,
                                    submission_Author,
                                    submission_selftext,
                                    Create_Date) 
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (submission_ID) DO NOTHING
        """
        cursor.execute(sql, (ID, Title, Score, Author, selftext, Create_Date))
    connection.commit()
    # Close the cursor and connection
    cursor.close()
    connection.close()


with DAG(
    "get_Data_Submission_Reddit_pipeline",
    start_date=timezone.datetime(2024, 4, 26),
    schedule="0 * * * *",
    catchup=False
    # schedule_interval='@daily'
) as dag:
    start = EmptyOperator(task_id="start")

    get_Data_Submission_Reddit = PythonOperator(
        task_id="get_Data_Submission_Reddit",
        python_callable=_get_Data_Submission_Reddit,
    )

    Create_Submission_table = PythonOperator(
    task_id="Create_Submission_table",
    python_callable=_Create_Submission_table,
    )

    load_data_to_postgres = PythonOperator(
    task_id="load_data_to_postgres",
    python_callable=_load_data_to_postgres,
    )

    end = EmptyOperator(task_id="end")

    start >> [get_Data_Submission_Reddit,Create_Submission_table] >> load_data_to_postgres >> end