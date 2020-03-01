import java.util.Date

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.utils.DateUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

import scala.collection.mutable.ArrayBuffer


object provinceCityClickStat2 {

  def main(args: Array[String]): Unit = {
    //sparkconf、sparksession、sparkstreaming
    val sparkConf = new SparkConf().setAppName("test").setMaster("local[*]")

    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    val streamingContext = new StreamingContext(sparkSession.sparkContext, Seconds(5))

    //kafka
    val kafka_brokers = ConfigurationManager.config.getString(Constants.KAFKA_BROKERS)
    val kafka_topics = ConfigurationManager.config.getString(Constants.KAFKA_TOPICS)
    val kafkaParam = Map(
      "bootstrap.servers" -> kafka_brokers,
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

    val adverDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(kafka_topics), kafkaParam)
    )

    //过滤黑名单
    val adverValueDStream = adverDStream.map(record => record.value())
    val adFilterDStream = adverValueDStream.transform {
      rdd =>
        rdd.filter {
          rdd =>
            val rddSplit = rdd.split(" ")
            val userId = rddSplit(1).toLong

            val adBlacklist: Array[AdBlacklist] = AdBlacklistDAO.findAll()

            !adBlacklist.contains(AdBlacklist(userId))
        }
    }

    //需求二 实时统计每天广告点击量
    streamingContext.checkpoint(".")
    adFilterDStream.checkpoint(Duration(10000))

    //（date_province_city_adid,1L)
    val key2countDStream = adFilterDStream.map {
      rdd =>
        val rddSplit: Array[String] = rdd.split(" ")
        val timeStamp = rddSplit(0).toLong
        val date = DateUtils.formatDateKey(new Date(timeStamp))
        val provinceId = rddSplit(1)
        val cityId = rddSplit(2)
        val adId = rddSplit(3)
        val key = date + "_" + provinceId + "_" + cityId + "_" + adId

        (key, 1L)
    }

    //TODO updateStateByKey
    val key2numDStream = key2countDStream.updateStateByKey[Long] {
      (values:Seq[Long], state:Option[Long]) =>
        val currentValue = values.foldLeft(0L)(_ + _)
        //val currentValue = values.foldLeft(0)((b,a)=>b+a)
        val previousValue = state.getOrElse(0L)
        var newValue = 0L
        newValue = currentValue + previousValue
        Some(newValue)
    }

    key2numDStream.foreachRDD{
      rddDS =>
        rddDS.foreachPartition{
          items =>
            items.foreach{
              case (key,count) =>
                var adArray = new ArrayBuffer[AdStat]()
                val keySplit = key.split("_")
                val date = keySplit(0)
                val province = keySplit(1)
                val city = keySplit(2)
                val ad = keySplit(3).toLong

                adArray += AdStat(date,province,city,ad,count)

                //更新数据库
            }
        }
    }
  }



}
