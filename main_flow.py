from prefect import flow, task
from scripts.extract_data import create_dimension_table
from scripts.load_data import sending_data_to_postgresql
from scripts.transform_data import clean_data
from scripts.create_fact import create_fact_table

@task
def extract():
    create_dimension_table()
    return

@task
def transform():
    clean_data()
    return

@task
def create_fact():
    create_fact_table()
    return

@task
def load_data():
    sending_data_to_postgresql()
    return

@flow(name="Youtube_ETL_Flow")
def youtube_etl_flow():
    extract_task = extract()
    transform_task = transform()
    create_fact_task = create_fact()
    load_task = load_data()

if __name__ == "__main__":
    youtube_etl_flow()