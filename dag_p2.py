from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

import pandas as pd

# Funciones para los operadores de python


def join_data(temperature_filename, humidity_filename, result_filename):
    temperature = pd.read_csv(temperature_filename, header=0, usecols=['datetime', 'San Francisco'])
    humidity = pd.read_csv(humidity_filename, header=0, usecols=['datetime', 'San Francisco'])

    temperature.columns = ['date', 'temperature']
    humidity.columns = ['date', 'humidity']

    data = pd.merge(temperature, humidity)
    data.to_csv(result_filename, sep=',', encoding='utf-8')


# Argumentos por defecto
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

# Inicialización del grafo DAG de tareas para el flujo de trabajo
dag = DAG(
    'plantilla_practica2',
    default_args=default_args,
    description='DAG para la práctica 2 de CC',
    schedule_interval=timedelta(days=1),
)

# Definimos directorio de tareas
rootdir = '/home/carlos/Escritorio/MASTER/CC/Practica2/'
workdir = '/home/carlos/Escritorio/MASTER/CC/Practica2/workspace/'
resourcesdir = '/home/carlos/Escritorio/MASTER/CC/Practica2/services/'



# Operadores o tareas
# Crea el directorio de trabajo si no existe todavia
setup = BashOperator(
    task_id='setup',
    depends_on_past=False,
    bash_command='if test -d ' + workdir + '; then echo "Working directory already created"; else mkdir ' +
    workdir + '; fi && if test -d ' + workdir + 'data; then echo "Data directory already created"; else mkdir ' + workdir + 'data; fi',
    dag=dag,
)

# Captura los datos de humedad de github
capture_a = BashOperator(
    task_id='capture_data_a',
    depends_on_past=False,
    bash_command='wget -O ' + workdir +
    'data/humidity.csv.zip https://github.com/manuparra/MaterialCC2020/raw/master/humidity.csv.zip',
    dag=dag,
)
# Captura los datos de temperatura de github
capture_b = BashOperator(
    task_id='capture_data_b',
    depends_on_past=False,
    bash_command='wget -O ' + workdir +
    'data/temperature.csv.zip https://github.com/manuparra/MaterialCC2020/raw/master/temperature.csv.zip',
    dag=dag,
)

# Descomprimir datos
unzip_data = BashOperator(
    task_id='unzip_data',
    depends_on_past=False,
    bash_command='unzip -o ' + workdir + 'data/humidity.csv.zip -d ' + workdir +
    'data && unzip -o ' + workdir + 'data/temperature.csv.zip -d ' + workdir + 'data',
    dag=dag,
)

# Unir datos
create_data_file = PythonOperator(
    task_id='join_data',
    depends_on_past=False,
    provide_context=False,
    python_callable=join_data,
    op_kwargs={'temperature_filename': workdir + 'data/temperature.csv',
               'humidity_filename': workdir + 'data/humidity.csv', 'result_filename': workdir + 'data/data.csv'},
    dag=dag,
)
# Copiar el fichero de datos a la carpeta de mongo
cp_data_file = BashOperator(
    task_id='cp_data_file',
    depends_on_past=False,
    bash_command='cp ' + workdir + 'data/data.csv ' + resourcesdir + 'mongo',
    dag=dag,
)

# Clonar el repositorio donde esta el microservicio de forecasting.
clone_repo = BashOperator(
    task_id='clone_repo',
    depends_on_past=False,
    bash_command='if [ -z "$(ls -A ' + resourcesdir + 'prediction_microservice)" ]; then git clone https://github.com/carlos-el/CCP2-Prediction_Microservice.git '+ resourcesdir + 'prediction_microservice/; else git -C '+ resourcesdir + 'prediction_microservice pull; fi',
    dag=dag,
)

# Descomprimir modelos del repo
unzip_models = BashOperator(
    task_id='unzip_models',
    depends_on_past=False,
    bash_command='unzip -o ' + resourcesdir + 'prediction_microservice/prediction_microservice/trained_models/compressed_models.zip -d ' + resourcesdir + 'prediction_microservice/prediction_microservice/trained_models',
    dag=dag,
)

# Crear entorno virtual
create_env = BashOperator(
    task_id='create_env',
    depends_on_past=False,
    bash_command='virtualenv ' + resourcesdir + 'prediction_microservice/venv_tmp && source ' + resourcesdir + 'prediction_microservice/venv_tmp/bin/activate && pip3 install -r ' + resourcesdir +'prediction_microservice/requirements.txt && deactivate',
    dag=dag,
)

# Realizar test unitarios
execute_unittest = BashOperator(
    task_id='execute_unittest',
    depends_on_past=False,
    bash_command='source ' + resourcesdir + 'prediction_microservice/venv_tmp/bin/activate && PYTHONPATH='+ resourcesdir +'prediction_microservice/ pytest '+ resourcesdir +'prediction_microservice/ && deactivate',
    dag=dag,
)

# Levantar contenedores
start_containers = BashOperator(
    task_id='start_containers',
    depends_on_past=False,
    bash_command='docker-compose -f '+ rootdir +'docker-compose.yml up',
    dag=dag,
)

# Dependencias
#setup >> [capture_a, capture_b] >> unzip_data >> create_data_file >> cp_data_file
[setup >> [capture_a, capture_b] >> unzip_data >> create_data_file >> cp_data_file, clone_repo >> unzip_models >> create_env >> execute_unittest] >> start_containers