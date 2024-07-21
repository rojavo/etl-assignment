# etl-assignment
"repository to upload the data-engineer-assignment solution"

This repository contains a Docker setup for running apache pyspark with a custom script. Follow the instructions below to build and run the Docker image.

- [Docker](https://www.docker.com/get-started) should be installed on your machine.

## Files in this Repository

- `Dockerfile`: Defines the Docker image with Apache Spark.
- `etl_main.py`: The python script with the etl to be run inside the container.
- `utils.py`: Here all the functions that perform transformations are defined.
- `data_transform.py`: Here all the transformations are applied to the dataframes.
- `queries/`: Directory with all the sql queries (a query for each .txt).
- `databases/database.db`: Sql database.
- `auxiliary_files/`: Directory with lists that were used to perform the data cleaning processes (animal_names.txt, condition_list.txt, verb_list.txt).
- `EDA.ipynb`: Notebook where I record my first approach to data, two of the auxiliary lists used are also extracted.

## Building and the docker image and run the container
After downloading the repository, builde the docker image ("etl_image" can be changed by other tags).
```bash
docker build -t etl_image .
```
Then run the docker containers using -v to ensure that the database is read and not deleted when the process ends (etl_volume can be changed by a volume name).
```bash
docker run -v etl_volume:databases etl_iamge:latest
```

## Outputs
The etl_main.py script runs making changes to the sql database, then prints the results of the queries to respond:
1. How many squirrels are there in each Park?
2. How many squirrels are there in each Borough?
3. A count of "Other Animal Sightings" by Park.
4. What is the most common activity for Squirrels? (e.g. eating, running, etc..)
5. A count of all Primary Fur Colors by Park.

The changes are saved in the database contained in the docker volume
