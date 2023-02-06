# Library imports
import os
from datetime import timedelta, datetime
import random
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Constant list of apples.
APPLES =['pink lady', 'jazz', 'orange pippin', 'granny smith', 'red delicious', 'gala', 'honeycrisp', 'mcintosh', 'fuji']

# Task that will read and print content from the txt file along with a greeting
def print_hello():
  # get file path of code review txt file
  filepath = os.path.abspath(__file__)
  dir_name = os.path.dirname(filepath)
  code_path = os.path.join(dir_name, "code_review.txt")

  with open(code_path, "r") as name_file:
    print(f"Hello {name_file}!")

# Picks random apple from APPLES list
def pick_apple():
  apple = random.choice(APPLES)
  print(apple)

# Sets the default DAG args
default_args = {
  'start_date': days_ago(2),
  'schedule_interval': timedelta(days=1),
  'retries': 1,
  'retry_delay': timedelta(minutes=5)
}

# Instantiate a DAG
with DAG(
  'code_review',
  description='Code review DAG that will run a series of tasks including outputting a name to a txt file and then reading it, and then picking a random name of apples from a list 3 times.',
  default_args=default_args
) as dag:

  # Task that will write my name to txt file
  echo_name = BashOperator(
    task_id = 'echo_name',
    bash_command='echo "Philip" > code_review.txt'
  )

  # Task that will will execute print_hello function
  print_name = PythonOperator(
    task_id="print_name",
    python_callable=print_hello
  )

  # Task that will echo "picking three random apples"
  echo_apples = BashOperator(
    task_id="echo_apples",
    bash_command="echo 'picking three random apples'"
  )
  
  # Task 4-6 that will pick the name of an apple from the list
  apple_task_list = []
  for i in range(1,4):
    apple_task=PythonOperator(
      task_id=f"pick_apple_{i}",
      python_callable=pick_apple
    )
    apple_task_list.append(apple_task)

  end_task = DummyOperator(
    task_id="end_task"
  )

  # Task order
  echo_name >> print_name >> echo_apples >> apple_task_list >> end_task