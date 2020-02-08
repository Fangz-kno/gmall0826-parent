package com.atguigu.gmall0826.realtime.app

import java.text.SimpleDateFormat
import java.util.Date
import java.util.logging.SimpleFormatter

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall0826.common.constant.GmallConstant
import com.atguigu.gmall0826.realtime.bean.StartupLog
import com.atguigu.gmall0826.realtime.util.MykafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object DauApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("dau_app").setMaster("local[*]")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val recordDstream: InputDStream[ConsumerRecord[String, String]] = MykafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_STARTUP, ssc)
    //    recordDstream.map(_.value()).print()
    //TODO 1 格式转换 补充时间字段
    val startUpLogDstream: DStream[StartupLog] = recordDstream.map {
      record => {
        //        1.1 把记录转换为样例类
        val jsonString: String = record.value()
        val startupLog: StartupLog = JSON.parseObject(jsonString, classOf[StartupLog])
        //          1.2 给空字段赋值
        val format = new SimpleDateFormat("yyyy-MM-dd HH")
        val dateString: String = format.format(new Date(startupLog.ts))
        val dateArray: Array[String] = dateString.split(" ")
        startupLog.logDate = dateArray(0)
        startupLog.logHour = dateArray(1)

        startupLog

      }
    }
    //TODO 2 去重   保留每个mid当日的第一条   其他的启动日志过滤掉
    // TODO  然后再利用清单进行过滤筛选 把清单中已有的用户的新日志过滤掉
    //    TODO 利用redis保存当日访问过的用户清单
    startUpLogDstream.foreachRDD {
      rdd =>
        rdd.foreachPartition { startupLogItr =>
//          TODO redis连接不能序列化 需要优化每个分区一个连接
          val jedis = new Jedis("hadoop102", 6379) // ex
          for (startupLog <- startupLogItr) {
//            拼一个key
            val dauKey = "dau:" + startupLog.logDate
//TODO set 类型可以去重
            jedis.sadd(dauKey, startupLog.mid)
//            失效时间
            jedis.expire(dauKey, 60 * 60 * 24);
          }
          jedis.close()
        }
    }
    ssc.start()
    ssc.awaitTermination()

  }
}
