import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql.functions.lit

object KafStreamCSV  extends App {
  """This Program reads data from Kafka Producer Console
    |and create dataframe on top of it and
    |processes it using Spark-Streaming
    |and store the final result in Casssandra
    |"""

  val conf = new SparkConf().setMaster("local[*]").setAppName("KafkaCSVSparkStreaming")
  val ssc = new StreamingContext(conf, Seconds(6))
  val spark=SparkSession.builder()
    .config(conf)
    .getOrCreate()

  spark.sparkContext.setLogLevel("OFF")
  val topicsSet = Set("csv-test16")

  val kafkaParams = Map[String, Object](
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
    ConsumerConfig.GROUP_ID_CONFIG -> "kafka.sparkSteaming",
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer])
  val LogsStream = KafkaUtils
    .createDirectStream(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topicsSet,kafkaParams))

  // Create DataFrame using Reflection Method
  case class TransactionLogs (customerid:String, creditcardno:Long, TransactionLocation:String,
                              transactionamount:Int, transactioncurrency:String, MerchantName:String,
                              numberofpasswordtries:Int, totalcreditlimit:Int,creditcardcurrency:String)


  val LogValues=LogsStream.map(x=>x.value())

  // When using window based processing, we use below line
  val windowedLogsStream = LogValues.window(Seconds(12),Seconds(12))


  import spark.implicits._
  //Reading each rdd from Dstream
  windowedLogsStream.foreachRDD(
    rdd =>
    {
      val df=rdd.map(line=>line.split(','))
        .map{c=>TransactionLogs(c(0), c(1).trim.toLong,
          c(2), c(3).trim.toInt, c(4), c(5), c(6).trim.toInt, c(7).trim.toInt,c(8) )}.toDF()

      val dfFraud= df
        .select("customerid","creditcardno","transactionamount",
          "transactioncurrency","numberofpasswordtries",
          "totalcreditlimit","creditcardcurrency")
           .filter(($"numberofpasswordtries">lit(3))|| ($"transactioncurrency"==$"creditcardcurrency")
            || ($"transactionamount"*100/$"totalcreditlimit" >lit(50)))

      // Datframe is getting written to Cassandra in Append Mode
      dfFraud.show(200)
      dfFraud.write
        .format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> "transactions", "keyspace" -> "antifraud"))
        .mode(SaveMode.Append)
        .save()


      // Using Spark SQL

     /*df.createOrReplaceTempView("CustomerTransactionLogs")
      val dfFraud = spark.sql("Select CustomerID, CreditCardNo, TransactionAmount, TransactionCurrency, NumberofPasswordTries, TotalCreditLimit, CreditCardCurrency
       from CustomerTransactionLogs
       where NumberofPasswordTries > 3
       OR TransactionCurrency != CreditCardCurrency
       OR ( TransactionAmount * 100.0 / TotalCreditLimit ) > 50");
      dfFraud.show()
     */
    })
    ssc.start()
  ssc.awaitTermination()
}
