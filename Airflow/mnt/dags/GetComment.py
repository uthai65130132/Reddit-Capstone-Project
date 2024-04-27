
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




def _get_Data_CommentSubmission_Reddit(**context):
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
    for comment in reddit.subreddit("dataengineering").comments(limit=40):
        listdata={
        "comment_ID":comment.id,
        "comment_Body":comment.body,
        "comment_Score":comment.score,
        "comment_Author":comment.author.name,
        "comment_Link_id":comment.link_id.split('_')[-1],
        "Create Date":str(datetime.fromtimestamp(int(comment.created_utc),tz=pytz.UTC))
        }
        list_datareddit.append(listdata)
    timestamp = context["execution_date"]
    with open(f"/opt/airflow/dags/staging/CommentSubmission_Reddit_Data_{timestamp}.json", "w") as f:
        json.dump(list_datareddit, f)

    return f"/opt/airflow/dags/staging/CommentSubmission_Reddit_Data_{timestamp}.json"
def _Create_CommentSubmission_table(**context):
    pg_hook = PostgresHook(
        postgres_conn_id="my_postgres_conn",
        schema="postgres"
    )
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    sql = """
        CREATE TABLE IF NOT EXISTS Comment (
            comment_ID varchar(30) UNIQUE PRIMARY KEY,
            comment_Body Text NOT NULL,
            comment_Score int NULL,
            comment_Author varchar(100) NOT NULL,
            comment_Link_id varchar(30) NOT NULL,
            Create_Date timestamp NOT NULL
        )
    """
    cursor.execute(sql)
    connection.commit()

def _load_data_to_postgres(**context):
    ti = context["ti"]
    file_name = ti.xcom_pull(task_ids="get_Data_CommentSubmission_Reddit", key="return_value")
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
        ID=data[i]["comment_ID"]
        Body=data[i]["comment_Body"]
        Score=data[i]["comment_Score"]
        Author=data[i]["comment_Author"]
        Link_id=data[i]["comment_Link_id"]
        Create_Date=data[i]["Create Date"]

        sql = """
            INSERT INTO Comment (comment_ID, 
                                    comment_Body,
                                    comment_Score,
                                    comment_Author,
                                    comment_Link_id,
                                    Create_Date) 
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (comment_ID) DO NOTHING
        """
        cursor.execute(sql, (ID, Body, Score, Author, Link_id, Create_Date))
    connection.commit()
    # Close the cursor and connection
    cursor.close()
    connection.close()

with DAG(
    "get_Data_CommentSubmission_Reddit_pipeline",
    start_date=timezone.datetime(2024, 4, 26),
    schedule="30 * * * *",
    catchup=False
    # schedule_interval='@daily'
) as dag:
    start = EmptyOperator(task_id="start")

    get_Data_CommentSubmission_Reddit = PythonOperator(
        task_id="get_Data_CommentSubmission_Reddit",
        python_callable=_get_Data_CommentSubmission_Reddit,
    )
    
    Create_CommentSubmission_table = PythonOperator(
    task_id="Create_CommentSubmission_table",
    python_callable=_Create_CommentSubmission_table,
    )

    load_data_to_postgres = PythonOperator(
    task_id="load_data_to_postgres",
    python_callable=_load_data_to_postgres,
    )

    end = EmptyOperator(task_id="end")

    start >> [get_Data_CommentSubmission_Reddit,Create_CommentSubmission_table] >> load_data_to_postgres >> end