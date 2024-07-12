FROM openjdk:8-jdk-slim

WORKDIR /dag
COPY . /dag
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*
RUN pip install dagster-webserver dagster-pyspark dagster pandas pyarrow dagster_aws

EXPOSE 3000
ENV DAGSTER_HOME=/dag

# Create the DAGSTER_HOME directory
RUN mkdir -p $DAGSTER_HOME

RUN echo "DAGSTER_HOME=/dag" > .env

RUN  dagster job execute -f dagster_module/pipelines.py -j determine_range_job
RUN  python3 scripts/backfill.py   
ENTRYPOINT ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000", "-f", "./dagster_module/pipelines.py"]
