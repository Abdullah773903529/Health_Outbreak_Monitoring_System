FROM python:3.10-slim

# تثبيت Java
RUN apt-get update && apt-get install -y \
    default-jre \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$JAVA_HOME/bin:$PATH

# إعداد بيئة العمل
WORKDIR /opt/dagster/app

# نسخ المتطلبات أولاً (للاستفادة من الكاش)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# تهيئة المجلدات والأكواد
RUN mkdir -p /shared_jars
COPY shared_jars/ /shared_jars/
COPY dagster_code/ /opt/dagster/app/

# متغيرات البيئة
ENV DAGSTER_HOME=/opt/dagster/app
ENV PYTHONPATH=/opt/dagster/app
ENV PYSPARK_PYTHON=python3

EXPOSE 3000
CMD ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000"]