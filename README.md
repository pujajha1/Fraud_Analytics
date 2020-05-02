# Kafka-SparkStreaming-Cassandra
KafkaProd.scala is a consumer API which produces data by reading it from a csv file.
It reads csv and write it in topic.

KafStreamCSV.scala is the processing application which reads data from topic 
and make dataframe on top of it and processes it using Spark Streaming 
and stores resultant datframe in Cassandra.

