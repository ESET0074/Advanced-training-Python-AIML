from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context
import pandas as pd
import sqlalchemy
import os

DATABASE_URL = "postgresql+psycopg2://postgres:postgres@postgres:5432/postgres"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="elt_pipeline_url_csv",
    default_args=default_args,
    description="ELT pipeline that extracts data from a URL, transforms it, and saves to CSV",
    schedule="@daily",  # ✅ updated from schedule_interval
    start_date=datetime(2025, 11, 10),
    catchup=False,
    tags=["elt", "astronomer", "postgres", "csv"],
) as dag:

    def extract_data():
        url = "https://raw.githubusercontent.com/mwaskom/seaborn-data/master/iris.csv"
        data = pd.read_csv(url)
        print("✅ Extracted data from URL:")
        print(data.head())
        return data.to_dict()

    def load_raw_data():
        context = get_current_context()
        raw_dict = context["ti"].xcom_pull(task_ids="extract_task")
        df = pd.DataFrame(raw_dict)
        engine = sqlalchemy.create_engine(DATABASE_URL)
        df.to_sql("staging_iris", con=engine, if_exists="replace", index=False)
        print("✅ Loaded raw data into table: staging_iris")

    def transform_data():
        engine = sqlalchemy.create_engine(DATABASE_URL)
        transform_sql = """
        DROP TABLE IF EXISTS iris_transformed;
        CREATE TABLE iris_transformed AS
        SELECT
            species,
            AVG(sepal_length) AS avg_sepal_length,
            AVG(sepal_width) AS avg_sepal_width,
            AVG(petal_length) AS avg_petal_length,
            AVG(petal_width) AS avg_petal_width
        FROM staging_iris
        GROUP BY species;
        """
        with engine.begin() as conn:
            conn.execute(transform_sql)
        print("✅ Transformed data and created table: iris_transformed")

    def push_data():
        engine = sqlalchemy.create_engine(DATABASE_URL)
        df = pd.read_sql("SELECT * FROM iris_transformed", con=engine)

        output_dir = "/usr/local/airflow/dags/output"
        os.makedirs(output_dir, exist_ok=True)

        # ✅ Save a new file every hour (timestamped)
        output_path = os.path.join(
            output_dir, f"iris_transformed_{datetime.now():%Y%m%d_%H%M%S}.csv"
        )
        # 👉 Or overwrite the same file every run:
        # output_path = os.path.join(output_dir, "iris_transformed.csv")

        df.to_csv(output_path, index=False)
        print(f"✅ Final transformed data saved to CSV at: {output_path}")
        print(df.head())

    extract_task = PythonOperator(task_id="extract_task", python_callable=extract_data)
    load_task = PythonOperator(task_id="load_task", python_callable=load_raw_data)
    transform_task = PythonOperator(task_id="transform_task", python_callable=transform_data)
    push_task = PythonOperator(task_id="push_task", python_callable=push_data)

    extract_task >> load_task >> transform_task >> push_task
