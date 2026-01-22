from airflow import DAG
from datetime import datetime
from helpers import DEFAULT_ARGS
from model_train_dag.metric_operator import MetricOperator
from model_train_dag.skip_operator import SkipOperator
from model_train_dag.train_model_operator import TrainModelOperator
from model_train_dag.visualize_operator import VisualizeOperator
from model_train_dag.branch_operator import BranchOperator


with DAG(
        dag_id='train_dag',
        default_args=DEFAULT_ARGS,
        start_date=datetime(2025, 11, 20),
        catchup=False,
) as dag:
    branch = BranchOperator(task_id='branch')

    skip_operator = SkipOperator(task_id='skip_operator', reason='Обучение модели отключено')
    train_model_operator = TrainModelOperator(task_id='train_model_operator')
    metric_operator = MetricOperator(task_id='metric_operator')
    visualize_operator = VisualizeOperator(task_id='visualize_operator')

    branch >> [train_model_operator, skip_operator]

    train_model_operator >> metric_operator >> visualize_operator
