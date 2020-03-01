import java.util.Date

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.utils.DateUtils
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

import scala.collection.mutable.ArrayBuffer

object provincePopular3 {
  def main(args: Array[String]): Unit = {
    //sparkconf sparksession sparkstream
    val sparkConf = new SparkConf().setAppName("test").setMaster("local[*]")

    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    val streamingContext = new StreamingContext(sparkSession.sparkContext, Seconds(5))

    val kafka_brokers = ConfigurationManager.config.getString("kafka.broker.list")
    val kafka_topics = ConfigurationManager.config.getString("kafka.topics")
    //kafka
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

    val adDStream = KafkaUtils.createDirectStream(
      streamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](
        Array(ConfigurationManager.config.getString(Constants.KAFKA_TOPICS)), kafkaParam
      )
    )

    //过滤黑名单(transform)
    val adValueDStream = adDStream.map(item => item.value())
    val adFilterDStream = adValueDStream.filter {
      rdd =>
        val rddSplit = rdd.split(" ")
        val userId = rddSplit(1).toLong

        val adBlacklist: Array[AdBlacklist] = AdBlacklistDAO.findAll()

        !adBlacklist.contains(AdBlacklist(userId))
    }

    //需求三 统计每天各省top3热门广告
    //数据源于需求二的updateStateByKey
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
    val key2numDStream: DStream[(String, Long)] = key2countDStream.updateStateByKey[Long] {
      (values:Seq[Long], state:Option[Long]) =>
        val currentValue = values.foldLeft(0L)(_ + _)
        //val currentValue = values.foldLeft(0)((b,a)=>b+a)
        val previousValue = state.getOrElse(0L)
        var newValue = 0L
        newValue = currentValue + previousValue
        Some(newValue)
    }

    //需求三
    val newKeyCountDStream = key2numDStream.map {
      case (key, count) =>
        val keySplit = key.split("_")
        val date = keySplit(0)
        val province = keySplit(1)
        val adId = keySplit(3)

        val newKey = date + "_" + province + "_" + adId
        (newKey, count)
    }

    val keyCountDStream = newKeyCountDStream.reduceByKey(_ + _)

    //TODO transform->rdd->rdd.map
    val top3DStream: DStream[Row] = keyCountDStream.transform {
      rdd =>
        val basicDateRDD: RDD[(String, String, String, Long)] = rdd.map {
          case (key, count) =>
            val keySplit = key.split("_")
            val date = keySplit(0)
            val province = keySplit(1)
            val adId = keySplit(2)

            (date, province, adId, count)
        }
        import sparkSession.implicits._
        basicDateRDD.toDF("date", "province", "adid", "count").createOrReplaceTempView("tmp_basic_info")

        val sql = "select date, province, adid, count from(" +
          "select date, province, adid, count, " +
          "row_number() over(partition by date,province order by count desc) rank from tmp_basic_info) t " +
          "where rank <= 3"

        sparkSession.sql(sql).rdd
    }

    top3DStream.foreachRDD{
      // rdd : RDD[row]
      rdd =>
        rdd.foreachPartition{
          // items : row
          items =>
            val top3Array = new ArrayBuffer[AdProvinceTop3]()
            for(item <- items){
              val date = item.getAs[String]("date")
              val province = item.getAs[String]("province")
              val adid = item.getAs[Long]("adid")
              val count = item.getAs[Long]("count")

              top3Array += AdProvinceTop3(date, province, adid, count)
            }
            AdProvinceTop3DAO.updateBatch(top3Array.toArray)
        }
    }

    streamingContext.start()
    streamingContext.awaitTermination()

  }
}
