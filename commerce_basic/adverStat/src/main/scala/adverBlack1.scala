import java.util.Date

import commons.conf.ConfigurationManager
import commons.utils.DateUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ArrayBuffer

object adverBlack1 {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("adverTest").setMaster("local[*]")

    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    //参数查看快捷点command+p
    val streamingContext = new StreamingContext(sparkSession.sparkContext, Seconds(5))

    val kafka_brokers = ConfigurationManager.config.getString("kafka.broker.list")
    val kafka_topics = ConfigurationManager.config.getString("kafka.topics")

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

    val adRealTimeDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(kafka_topics), kafkaParam)
    )

    val adRealTimeValueDStream: DStream[String] = adRealTimeDStream.map(item => item.value())

    //过滤黑名单
    val filteredDStream: DStream[String] = adRealTimeValueDStream.transform {
      rdd =>
        //只有userid的list
        val getBlackList = AdBlacklistDAO.findAll()
        val getUserIdList: Array[Long] = getBlackList.map(item => item.userid)
        rdd.filter {
          rdd =>
            val rddSplit = rdd.split(" ")
            val userId = rddSplit(3).toLong
            !getUserIdList.contains(userId)
        }
    }

    //需求一：广告黑名单实时统计
    generateBlackList(filteredDStream)

    streamingContext.start()
    streamingContext.awaitTermination()
  }

  /************广告黑名单实时统计************/
  def generateBlackList(filteredDStream: DStream[String]) = {
    val dateUserAdKey: DStream[(String, Long)] = filteredDStream.map {
      rdd =>
        val rddSplit = rdd.split(" ")
        val timeStamp: String = rddSplit(0)
        val date: String = DateUtils.formatDateKey(new Date(timeStamp))
        val userId = rddSplit(3)
        val adId = rddSplit(4)
        val key = date + "_" + userId + "_" + adId
        (key, 1L)
    }

    //只考虑当下一个流的数据统计,然后更新到数据库，所以不需考虑updateStateByKey
    val key2num: DStream[(String, Long)] = dateUserAdKey.reduceByKey(_ + _)

    //遍历每个DS下的rdd，为每个rdd的分区创建一个数据库连接线程进行更新，
    // 不能用foreach代替foreachPartition，不然会对rdd中的每条记录创建一个数据库连接线程
    key2num.foreachRDD{
      DSRdd => DSRdd.foreachPartition{
        rddPartition =>
          val clickCountArray = new ArrayBuffer[AdUserClickCount]()

          for((key,count) <- rddPartition){
            val keySplit = key.split("_")
            val date = keySplit(0)
            val userId = keySplit(1).toLong
            val adid = keySplit(2).toLong

            clickCountArray += AdUserClickCount(date,userId,adid,count)
          }
      }
    }

    //查询用户广告点击大于100的userid
    val newBlackList: DStream[(String, Long)] = key2num.filter {
      case (key, count) =>
        val keySplit = key.split("_")
        val date = keySplit(0)
        val userId = keySplit(1).toLong
        val adid = keySplit(2).toLong

        val clickCount = AdUserClickCountDAO.findClickCountByMultiKey(date, userId, adid)

        if (clickCount > 100) {
          true
        } else {
          false
        }
    }

    //预备黑名单取出userid并去重
    val newBlackUserIdList = newBlackList.map {
      DStream =>
        val key = DStream._1
        val userId = key.split("_")(1)
        userId
    }.transform(rdd => rdd.distinct())

    newBlackUserIdList.foreachRDD{
      rdd => rdd.foreachPartition{
        rddPartition =>
          val userIdArray = new ArrayBuffer[AdBlacklist]()

          for(item <- rddPartition){
            userIdArray += AdBlacklist(item.toLong)
          }

          AdBlacklistDAO.insertBatch(userIdArray.toArray)
      }
    }


  }
}
