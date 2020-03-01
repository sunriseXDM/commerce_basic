import java.util.Date

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.utils.DateUtils
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Duration, Minutes, Seconds, StreamingContext}

object adOneHourClick {
  def main(args: Array[String]): Unit = {
    //sparkconf sparksession sparkstream
    val sparkConf = new SparkConf().setAppName("test").setMaster("local[*]")

    val sparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()

    val streamingContext = new StreamingContext(sparkSession.sparkContext, Seconds(5))

    //kafka
    val kafkaTopics = ConfigurationManager.config.getString(Constants.KAFKA_TOPICS)
    val kafkaBrokers = ConfigurationManager.config.getString(Constants.KAFKA_BROKERS)

    val kafkaParam = Map(
      "bootstrap.servers" -> kafkaBrokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "group1",
      // auto.offset.reset
      // latest: 先去Zookeeper获取offset，如果有，直接使用，如果没有，从最新的数据开始消费；
      // earlist: 先去Zookeeper获取offset，如果有，直接使用，如果没有，从最开始的数据开始消费
      // none: 先去Zookeeper获取offset，如果有，直接使用，如果没有，直接报错
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false:java.lang.Boolean)
    )

    val adDStream = KafkaUtils.createDirectStream(
      streamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(kafkaTopics), kafkaParam)
    )

    //过滤黑名单
    val adValueDStream: DStream[String] = adDStream.map(item => item.value())
    val adFilterDStream: DStream[String] = adValueDStream.transform {
      logRDD =>

        // blackListArray: Array[AdBlacklist]     AdBlacklist: userId
        val blackListArray = AdBlacklistDAO.findAll()

        // userIdArray: Array[Long]  [userId1, userId2, ...]
        val userIdArray = blackListArray.map(item => item.userid)

        logRDD.filter {
          // log : timestamp province city userid adid
          case log =>
            val logSplit = log.split(" ")
            val userId = logSplit(3).toLong
            !userIdArray.contains(userId)
        }
    }

    val key2numDStream = adFilterDStream.map {
      logRdd =>
        val rddSplit = logRdd.split(" ")
        val timestamp = rddSplit(0)
        val date = DateUtils.formatTimeMinute(timestamp)
        val adId = rddSplit(2)

        (date + "_" + adId, 1L)
    }

    val key2countDStream = key2numDStream.reduceByKeyAndWindow((_ + _), Minutes(60), Minutes(1))

    //foreachRdd -> foreachPartition -> for(i <- items)

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
