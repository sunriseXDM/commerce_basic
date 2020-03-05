import java.util.Date

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.utils.DateUtils
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

object Test {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("test")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    val streamingContext = new StreamingContext(sparkConf, Seconds(5))

    streamingContext.checkpoint(".")

    val kafkaTopics = ConfigurationManager.config.getString(Constants.KAFKA_TOPICS)
    val kafkaBrokers = ConfigurationManager.config.getString(Constants.KAFKA_BROKERS)

    val kafkaParam = Map(
      "bootstrap.servers" -> kafkaBrokers,
      "key.deserializer" -> classOf[StringDecoder],
      "value.deserializer" -> classOf[StringDecoder],
      "group.id" -> "group1",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false:java.lang.Boolean)
    )


    val OriDStream = KafkaUtils.createDirectStream(
      streamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(kafkaTopics), kafkaParam))
    val ValueDStream = OriDStream.map(item => item.value())

    //过滤黑名单
    val adRealDStream = ValueDStream.transform {
      logRdd =>
        val blackList = AdBlacklistDAO.findAll()
        val userIdList = blackList.map(item => item.userid)
        logRdd.filter {
          rdd =>
            val rddSplit = rdd.split(" ")
            val userId = rddSplit(0)
            !userIdList.contains(userId)
        }
    }

    adRealDStream.checkpoint(Duration(10000))

    // 需求一：实时维护黑名单 将每天对某个广告点击超过 100 次的用户拉黑 date_userid_adid
    val key2oneDStream = adRealDStream.map {
      log =>
        val logSplit = log.split(" ")
        val timeStamp = logSplit(0).toLong
        val date = DateUtils.formatDate(new Date(timeStamp))
        val userID = logSplit(1)
        val adId = logSplit(2)

        val key = date + "_" + userID + "_" + adId

        (key, 1L)
    }

    val key2countDStream = key2oneDStream.reduceByKey(_ + _)

    key2countDStream.foreachRDD{
      rdd =>
        rdd.foreachPartition{
          rddPartition =>
            for(item <- rddPartition){
              val stringSplit = item._1.split("_")
              //update
            }
        }
    }



  }
}
