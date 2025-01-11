from airflow import DAG
from datetime import datetime
from airflow.decorators import task
from airflow.operators.python import PythonOperator

MESSAGES_PER_TASK = 100_000  # Jumlah pesan per task

@task
def fetch_data():
    messages = list(range(551_000))

    # Hitung jumlah task
    total_messages = len(messages)
    chunk_size = MESSAGES_PER_TASK
    chunks = [
        messages[i:i + chunk_size] for i in range(0, total_messages, chunk_size)
    ]

    print(f"Total messages: {total_messages}, Total tasks: {len(chunks)}")

    return chunks


@task(
        task_id='process_data',
        max_active_tis_per_dag=1, # Sequentials
        )
def process_chunk(chunk):
    """Proses chunk tertentu."""
    print(f"Memproses {len(chunk)} pesan. Contoh pesan: {chunk[:5]}...")
    for code in chunk:
        # Simulasikan pemrosesan
        print(f"Processing code: {code}")


# Definisi DAG
with DAG(
    "dynamic_task_mapping_with_fixed_chunk_size_decorator",
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    concurrency=5,
) as dag:

    # Task 1: Fetch data
    chunks = fetch_data()

    # Task 2: Generate tasks to process data chunks
    process_chunk.expand(chunk=chunks)
