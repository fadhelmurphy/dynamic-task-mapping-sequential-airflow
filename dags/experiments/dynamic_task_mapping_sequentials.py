from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator

CHUNK_SIZE_PER_TASK = 100_000  # Jumlah data per task

def fetch_data():
    data = list(range(551_000))

    # jumlah task
    total_data = len(data)
    chunks = [
        {'chunk': data[i:i + CHUNK_SIZE_PER_TASK]} for i in range(0, total_data, CHUNK_SIZE_PER_TASK)
    ]

    print(f"Total data: {total_data}, Total tasks: {len(chunks)}")
    return chunks

def process_chunk(chunk):
    """Proses chunk tertentu."""
    print(f"Jumlah data: {len(chunk)} data. Contoh data: {chunk[:5]}...")
    for code in chunk:
        print(f"Processing code: {code}")

with DAG(
    "dynamic_task_mapping_with_fixed_chunk_size",
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    concurrency=5,
) as dag:

    fetch_data_task = PythonOperator(
        task_id="fetch_data",
        python_callable=fetch_data,
    )

    process_chunk_task = PythonOperator.partial(
        task_id="process_chunk",
        python_callable=process_chunk,
        max_active_tis_per_dag=1, # Sequential
    ).expand(op_kwargs=fetch_data_task.output)
