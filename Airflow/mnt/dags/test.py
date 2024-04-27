# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
# from datetime import datetime
# import praw

# def fetch_reddit_data():
#     reddit = praw.Reddit(client_id='YOUR_CLIENT_ID',
#                          client_secret='YOUR_CLIENT_SECRET',
#                          user_agent='YOUR_USER_AGENT',
#                          username='YOUR_USERNAME',
#                          password='YOUR_PASSWORD')

#     # Use PRAW to fetch Reddit data
#     for submission in reddit.subreddit("dataengineering").new(limit=2):
#         print("----------------------------------")
#         print("ID: ", submission.id)
#         print("Title: ", submission.title)
#         print("Score: ", submission.score)
#         print("Author: ", submission.author)
#         print("Create Date: ", datetime.utcfromtimestamp(submission.created_utc))

# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': datetime(2024, 4, 21),
#     'retries': 1
# }

# with DAG('reddit_data_dag', default_args=default_args, schedule_interval='@daily') as dag:
#     fetch_data_task = PythonOperator(
#         task_id='fetch_reddit_data',
#         python_callable=fetch_reddit_data
#     )

#     fetch_data_task
