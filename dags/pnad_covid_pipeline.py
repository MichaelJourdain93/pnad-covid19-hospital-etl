# Stub de orquestração: mapeia as fases já validadas no notebook (extração -> tratamento -> visualização)
# para tasks agendáveis. Ainda não aponta para um cluster Spark real; ajustar spark_conn_id ao produtizar.

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    "owner": "michael-jourdain",
    "retries": 1,
}

with DAG(
    dag_id="pnad_covid_pipeline",
    description="ETL e consolidação da PNAD-COVID-19 (set/out/nov 2020)",
    default_args=default_args,
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["pnad-covid", "pyspark", "portfolio"],
) as dag:

    extract = SparkSubmitOperator(
        task_id="extract_raw_datasets",
        application="src/extract.py",
        conn_id="spark_default",
    )

    transform = SparkSubmitOperator(
        task_id="transform_and_consolidate",
        application="src/transform.py",
        conn_id="spark_default",
    )

    def validate_schema():
        import pandas as pd

        df = pd.read_parquet("data/processed/pnad_covid_consolidado.parquet")
        expected_columns = {
            "uf",
            "faixa_etaria",
            "sexo",
            "teve_febre",
            "teve_tosse",
            "dificuldade_respirar",
            "area_domicilio",
            "solicitacao_emprestimo",
        }
        missing = expected_columns - set(df.columns)
        if missing:
            raise ValueError(f"Colunas ausentes na base consolidada: {missing}")

    validate = PythonOperator(
        task_id="validate_consolidated_schema",
        python_callable=validate_schema,
    )

    generate_reports = PythonOperator(
        task_id="generate_reports",
        python_callable=lambda: __import__("src.reports", fromlist=["build_all"]).build_all(),
    )

    extract >> transform >> validate >> generate_reports
