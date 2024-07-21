FROM --platform=linux/amd64 bitnami/spark:latest

WORKDIR /app

COPY queries/ /app/queries
COPY auxiliary_files/ /app/auxiliary_files
COPY utils.py/ /app/
COPY data_transform.py /app/

COPY data/ /app/data

COPY etl_main.py /app/

RUN pip install py4j pandas
RUN mkdir /app/databases

CMD ["python", "etl_main.py"]
