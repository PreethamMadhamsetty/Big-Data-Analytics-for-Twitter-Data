@echo off

echo Stopping existing Docker containers...
docker-compose down

echo Building custom docker image
docker build -t custom-spark .

echo Starting Docker containers...
docker-compose up -d

echo Copying Spark application file to Spark master container...
docker cp spark_app.py spark-master:/opt/bitnami/spark/spark_app.py

echo Submitting Spark job...
docker-compose exec spark-master ./bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.elasticsearch:elasticsearch-spark-30_2.12:7.15.1 spark_app.py

echo Execution completed.
