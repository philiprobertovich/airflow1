# Library imports
from datetime import timedelta, datetime
import random
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from airflow.utils.dates import days_ago

# Constant list of apples.
APPLES =['pink lady', 'jazz', 'orange pippin', 'granny smith', 'red delicious', 'gala', 'honeycrisp', 'mcintosh', 'fuji']

# Task that will read and print content from the txt file along with a greeting
def print_hello():
  with open("code_review.txt", "r") as name_file:
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
  task_1 = BashOperator(
    task_id = 'task_1',
    bash_command='echo "Philip" > code_review.txt'
  )

  # Task that will will execute print_hello function
  task_2 = PythonOperator(
    task_id="task_2",
    python_callable=print_hello
  )

  # Task that will echo "picking three random apples"
  task_3 = BashOperator(
    task_id="task_3",
    bash_command="echo 'picking three random apples'"
  )
  
  
  # ***Alternate way to create tasks 4-6*** 
  # for i in range(3):
    
  #   @task(task_id=f"task_{i+4}")
  #   def pick_apples():
  #     apple = random.choice(APPLES)
  #     print(apple)

  #   tasks = pick_apples()

  #   # Set task orders 4-6 to be parallel
  #   task_1 >> task_2 >> task_3 >> tasks

  task_4=PythonOperator(
    task_id="task_4",
    python_callable=pick_apple
  )

  task_5=PythonOperator(
    task_id="task_5",
    python_callable=pick_apple
  )

  task_6=PythonOperator(
    task_id="task_6",
    python_callable=pick_apple
  )

  task_7 = DummyOperator(
    task_id="task_7"
  )

  # Task order
  task_1 >> task_2 >> task_3 >> [task_4, task_5, task_6] >> task_7