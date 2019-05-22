package com.bigdata.Operator

import java.util.Properties

import com.bigdata.bean.KafkaEvent
import org.apache.flink.api.common.serialization.{SerializationSchema, SimpleStringSchema, DeserializationSchema}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaProducer010, FlinkKafkaConsumer010}

object KafkaEventConnectScala {

  case class KafkaEvent(word:String,frequency:Int,timestamp:Long)

  class KafkaEventSchema extends DeserializationSchema[KafkaEvent]with org.apache.flink.api.common.serialization.SerializationSchema[KafkaEvent]{
    override def isEndOfStream(nextElement: KafkaEvent): Boolean = false

    override def deserialize(message: Array[Byte]): KafkaEvent = {
      val split = new String(message).split(",")

      new KafkaEvent(split(0),split(1).toInt,split(2).toLong)
    }

    override def getProducedType: TypeInformation[KafkaEvent] = {
      TypeInformation.of(classOf[KafkaEvent])
    }

    override def serialize(element: KafkaEvent): Array[Byte] = {
      (element.word+","+element.frequency+","+element.timestamp).getBytes
    }
  }

  def main(args: Array[String]) {
    val properties: Properties = new Properties
    properties.setProperty("bootstrap.servers", "pckafka2node001:9092")
    properties.setProperty("group.id", "test")
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val kafkaConsum = new FlinkKafkaConsumer010[KafkaEvent]("wordfre",new KafkaEventSchema,properties)
//    env.addSource(kafkaConsum).assignAscendingTimestamps(new AscendingTimestampExtractor[KafkaEvent]{
    //      override def extractAscendingTimestamp(element: KafkaEvent): Long = {
    //        element.timestamp
    //      }
    //    })
    val input = env.addSource(kafkaConsum)


    val kafkaproce: FlinkKafkaProducer010[KafkaEvent] = new FlinkKafkaProducer010[KafkaEvent]("wordresu",new KafkaEventSchema,properties)

    input.addSink(kafkaproce)
    env.execute("KafkaEventConnectScala")

  }

}
