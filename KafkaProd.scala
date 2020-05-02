import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import scala.io.Source
object KafkaProd extends App {
  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("client.id", "ScalaProducerExample")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)

  val TOPIC="csv-test16"

  val file = Source.fromFile("C:/Users/sumit/Desktop/TranasctionLogs.csv")
  val src = file.getLines()
  // print the uid for Guest
  src.foreach(
    a => {
      val record = new ProducerRecord(TOPIC, "key", a)
      producer.send(record)
    }
  )
  file.close()

  // val record = new ProducerRecord(TOPIC, "key", "the end "+new java.util.Date)
  // producer.send(record)

  producer.close()

}
