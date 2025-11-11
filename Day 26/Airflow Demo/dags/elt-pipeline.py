from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context
import pandas as pd
import sqlalchemy

# Database connection for Astronomer setup
DATABASE_URL = "postgresql+psycopg2://postgres:postgres@postgres:5432/postgres"

# Default DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define DAG
with DAG(
    dag_id="elt_pipeline",
    default_args=default_args,
    description="ELT pipeline that fetches data from a URL and processes it in Postgres",
    schedule="* * * *  *",
    start_date=datetime(2025, 11, 10),
    catchup=False,
    tags=["elt", "astronomer", "postgres", "url"],
) as dag:

    # 1️⃣ EXTRACT — Pull data from a public URL
    def extract_data():
        url = "https://raw.githubusercontent.com/mwaskom/seaborn-data/master/iris.csv"
        data = pd.read_csv(url)
        print("✅ Extracted data from URL. Sample:")
        print(data.head())
        return data.to_dict()

    # 2️⃣ LOAD — Load raw data into Postgres (staging)
    def load_raw_data():
        context = get_current_context()
        raw_dict = context["ti"].xcom_pull(task_ids="extract_task")
        df = pd.DataFrame(raw_dict)

        engine = sqlalchemy.create_engine(DATABASE_URL)

        df.to_sql("staging_iris", con=engine, if_exists="replace", index=False)
        print("✅ Loaded raw Iris data into 'staging_iris' table.")

    # 3️⃣ TRANSFORM — Clean and enrich data inside Postgres (SQL)
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
        print("✅ Transformed data: created 'iris_transformed' table with aggregated metrics.")

    # 4️⃣ PUSH — Read transformed data for verification or downstream use
    def push_data():
        engine = sqlalchemy.create_engine(DATABASE_URL)
        df = pd.read_sql("SELECT * FROM iris_transformed", con=engine)
        print("✅ Final transformed data ready for use:\n", df)

    # Define tasks
    extract_task = PythonOperator(
        task_id="extract_task",
        python_callable=extract_data,
    )

    load_task = PythonOperator(
        task_id="load_task",
        python_callable=load_raw_data,
    )

    transform_task = PythonOperator(
        task_id="transform_task",
        python_callable=transform_data,
    )

    push_task = PythonOperator(
        task_id="push_task",
        python_callable=push_data,
    )

    # Define ELT order
    extract_task >> load_task >> transform_task >> push_task
