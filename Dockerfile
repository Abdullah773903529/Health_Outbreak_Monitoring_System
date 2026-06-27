FROM python:3.10-slim

# تثبيت Java والأدوات الأساسية
RUN apt-get update && apt-get install -y \
    default-jre \
    wget \
    curl \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$JAVA_HOME/bin:$PATH

# تثبيت المكتبات مع توحيد إصدار PySpark ليتطابق مع صور easewithdata:3.5.5
RUN pip install --no-cache-dir \
    dagster \
    dagster-webserver \
    dagster-postgres \
    pyspark==3.5.5 \
    psycopg2-binary \
    pandas \
    pyarrow \
    confluent-kafka \
    boto3 \
    s3fs

WORKDIR /opt/dagster/app
COPY dagster_code/ /opt/dagster/app/

ENV DAGSTER_HOME=/opt/dagster/app
ENV PYTHONPATH=/opt/dagster/app
ENV PYSPARK_PYTHON=python3

EXPOSE 3000

CMD ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000"]