# Airflow Week 1

### By Philip Kendall

#### This project sets up a basic pipline in Airflow that accomplishes simple tasks.
## Technologies Used

* GIT
* Python 3.7
* Airflow 2.3.2
* Docker


## Description

The pipeline created for this project accomplishes up to 7 tasks. The first task echoes my name and the output gets put into a txt file. The second task then reads and prints the contents of that txt file. The next task echoes that three apples are going to be picked. Once that task is completed, the following next 3 taks run parallel with each other and all pick and print out a random name of a type of apple which is stored in a static list. The final task does nothing, but signify the end of the DAG.


Below is the DAG:
<img src="img/code_review_dag.jpg" alt="DAG of Code Review Pipeline" width=400/>

## Setup/Installation Requirements

* Fork over the the repository to your own Github account.
* Clone your Github repo down to your local machine and into the directory you would like this project to be stored.
* Create a virtual environment:
  ```
  python3.7 -m venv venv
  ```

* Install the requirements.txt file:
  ```
  pip install -r requirements.txt
  ```

* Navigate to the dsa-airflow directory and use the curl command to download the latest docker-compose.yaml file
  ```
  cd dsa-airflow
  curl -LfO "https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml"
  ```
* Set the .env file
  ```
  echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
  ```
* Create directories for logs and plugins:
  ```
  mkdir ./logs ./plugins
  ```
* Initialize Airflow with the docker compose up command:
  ```
  docker-compose up airflow-init
  ```
* Once Airflow is initialized, docker compoese up to run Airflow.
  ```
  docker-compose up
  ```
* Navigate to localhost:8080/home in your browser and enter the Airflow credentials. Both username and password should be "airflow".
* Find "code_review" under the DAG column and unpause the DAG.
* After that, the DAG should run. Click on "code_review" to see the different views of the DAG's execution.

## Known Bugs

N/A

## License

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The below copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

Copyright (c) 2023 Philip Kendall