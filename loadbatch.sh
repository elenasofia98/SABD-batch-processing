docker exec -it namenode hdfs dfs -mkdir -p /input/
docker exec -it namenode hdfs dfs -put /input/yellow_tripdata_2021-12.parquet /input/
docker exec -it namenode hdfs dfs -put /input/yellow_tripdata_2022-01.parquet /input/
docker exec -it namenode hdfs dfs -put /input/yellow_tripdata_2022-02.parquet /input/

