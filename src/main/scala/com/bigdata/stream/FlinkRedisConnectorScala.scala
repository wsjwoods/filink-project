package com.bigdata.stream

import com.bigdata.Operator.WindowDataSampleScala
import com.bigdata.Operator.WindowSplitScala.Grade
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object FlinkRedisConnectorScala {

  def main(args: Array[String]) {
    val conf: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder().setHost("localhost").setPort(6379).build
    val rate =3
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val grade: DataStream[Grade] = WindowDataSampleScala.getGradeSource(env,rate)

    grade.addSink(new RedisSink[Grade](conf,new RedisMapper[Grade] {
      override def getCommandDescription: RedisCommandDescription = {
        new RedisCommandDescription(RedisCommand.HSET, "flink_redis")
      }

      override def getKeyFromData(t: Grade): String = {
        t.name
      }

      override def getValueFromData(t: Grade): String = {
        t.name+"-"+t.grade
      }
    }))

    env.execute("FlinkRedisConnectorScala")

  }


}
