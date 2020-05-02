# Kafka-SparkStreaming-Cassandra 

This Project finds the suspicious transaction details.

Source: **Kafka**,
Processing: **Spark-Streaming**,
Target: **Cassandra**

This project uses Kafka for streaming CSV file, Spark-Streaming for Processing and Cassandra for Storing the final data in a table.

KafkaProd.scala is a consumer API which produces data by reading it from a csv file.
It reads csv and write it in topic.

KafStreamCSV.scala is the processing application which reads data from broker using topic 
and make dataframe on top of it and processes it using Spark Streaming 
and stores resultant DataFrame in Cassandra.

