FROM apache/airflow:2.10.3-python3.10

# Install Airflow
USER airflow
COPY requirements.txt .
RUN pip install -r requirements.txt

# Install gcloud SDK
USER root
# Adding the package path to local
ENV PATH $PATH:/opt/gcp/gcloud/google-cloud-sdk/bin
RUN chmod 777 /home/airflow
	

USER airflow
