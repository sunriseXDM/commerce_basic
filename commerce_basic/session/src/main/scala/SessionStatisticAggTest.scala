import java.util.{Date, Random, UUID}

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.{ProductInfo, ProductInfo2, UserInfo, UserVisitAction}
import commons.utils._
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * 测试统计前10位点击、下单、付款的商品
  * 方法不同，此方法使用表内的productId关联点击列表、下单列表、付款列表
  * 可以考虑使用createOrReplaceTempView的方式用产品表关联点击表、下单表、付款表（不用sortBy）
  */
object SessionStatisticAggTest {

  def main(args: Array[String]): Unit = {

    // 获取查询的限制条件
    val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    val taskParam = JSONObject.fromObject(jsonStr)

    // 获取全局独一无二的主键
    val taskUUID = UUID.randomUUID().toString

    // 创建sparkConf
    val sparkConf = new SparkConf().setAppName("session").setMaster("local[*]")

    // 创建sparkSession
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    // actionRDD: rdd[UserVisitAction]
    val actionRDD = getActionRDD(sparkSession, taskParam)

    // sessionId2ActionRDD: rdd[(sid, UserVisitAction)]
    val sessionId2ActionRDD = actionRDD.map{
      item => (item.session_id, item)
    }

    // sessionId2GroupRDD: rdd[(sid, iterable(UserVisitAction))]
    val sessionId2GroupRDD = sessionId2ActionRDD.groupByKey()

    // sparkSession.sparkContext.setCheckpointDir()
    sessionId2GroupRDD.cache()
    // sessionId2GroupRDD.checkpoint()

    // 获取聚合数据里面的聚合信息
    val sessionId2FullInfoRDD = getFullInfoData(sparkSession, sessionId2GroupRDD)

    // 创建自定义累加器对象
    val sessionStatAccumulator = new SessionAccumulator
    // 注册自定义累加器
    sparkSession.sparkContext.register(sessionStatAccumulator, "sessionAccumulator")

    // 过滤用户数据
    val sessionId2FilterRDD = getFilteredData(taskParam, sessionStatAccumulator, sessionId2FullInfoRDD)

    sessionId2FilterRDD.count()

    // sessionId2ActionRDD: RDD[(sessionId, action)]
    // sessionId2FilterRDD : RDD[(sessionId, FullInfo)]  符合过滤条件的
    // sessionId2FilterActionRDD: join
    // 获取所有符合过滤条件的action数据
    val sessionId2FilterActionRDD = sessionId2ActionRDD.join(sessionId2FilterRDD).map{
      case (sessionId, (action, fullInfo)) =>
        (sessionId, action)
    }

    //获取商品列表
    val productRdd = getProductList(sparkSession)

    //productRdd.foreach(println(_))
    productRdd.cache()

    getTop10List(sparkSession,taskUUID,sessionId2FilterActionRDD,productRdd)

  }

  def getTop10List(sparkSession: SparkSession, taskUUID: String, sessionId2FilterActionRDD: RDD[(String, UserVisitAction)], productRdd: RDD[(Long, Long)]): Unit ={
    //从sessionId2FilterActionRDD获取点击列表
    val clickList = getClickList(sessionId2FilterActionRDD)
    //获取下单列表
    val orderList = getOrderList(sessionId2FilterActionRDD)
    //获取付款列表
    val payList = getPayList(sessionId2FilterActionRDD)
    //product left join clickList
    val joinList = productRdd.leftOuterJoin(clickList).map{
      case (id,(num,option)) =>
        val clickCount = if(option.isDefined) option.get else 0
        (id,num+clickCount)
    }
    //继续join orderList
    val joinList2 = joinList.leftOuterJoin(orderList).map{
      case (id,(click,option))=>
        val orderCount = if(option.isDefined) option.get else 0
        (id,(click,orderCount))
    }

    //继续join payList
    val joinList3 = joinList2.leftOuterJoin(payList).map{
      case (id,((click,order),option))=>
        val payCount = if(option.isDefined) option.get else 0
        (id,click,order,payCount)
    }

    val sortRdd = joinList3.map{
      case (id,click,order,pay)=>
        val sortKey = SortKey(click,order,pay)
        (sortKey,(id,click,order,pay))
    }

    val top10 = sortRdd.sortByKey(false).take(10)

    /*(sortKey,(id,click,order,pay))
    (SortKey(101,80,57),(88,101,80,57))
    (SortKey(91,79,83),(10,91,79,83))
     */
    top10.foreach(println(_))
  }

  def getClickList(sessionId2FilterActionRDD: RDD[(String, UserVisitAction)]) = {
    //过滤
    val filterRdd = sessionId2FilterActionRDD.filter(item => item._2.click_category_id != -1L)
    //rdd转化为(product_id,1)
    val rddGroup = filterRdd.map(item => (item._2.click_category_id, 1L))
    //reduceByKey
    rddGroup.reduceByKey(_+_)
  }

  def getOrderList(sessionId2FilterActionRDD: RDD[(String, UserVisitAction)]) = {
    //过滤
    val fliterRdd = sessionId2FilterActionRDD.filter(item => item._2.order_category_ids != null)
    //ids转化
    fliterRdd.flatMap(item => item._2.order_category_ids.split(",").map(id => (id.toLong,1L))).reduceByKey(_+_)
  }

  def getPayList(sessionId2FilterActionRDD: RDD[(String, UserVisitAction)]) ={
    //过滤
    val fliterRdd = sessionId2FilterActionRDD.filter(item => item._2.pay_category_ids != null)
    //ids转化
    fliterRdd.flatMap(item => item._2.pay_category_ids.split(",").map(id => (id.toLong,1L))).reduceByKey(_+_)
  }

  def getFilteredData(taskParam: JSONObject,
                      sessionStatAccumulator: SessionAccumulator,
                      sessionId2FullInfoRDD: RDD[(String, String)])= {

    val startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE)
    val endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE)
    val professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS)
    val cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES)
    val sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX)
    val keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS)
    val categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS)

    var filterInfo = (if(startAge != null) Constants.PARAM_START_AGE + "=" + startAge + "|" else "") +
      (if (endAge != null) Constants.PARAM_END_AGE + "=" + endAge + "|" else "") +
      (if (professionals != null) Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" else "") +
      (if (cities != null) Constants.PARAM_CITIES + "=" + cities + "|" else "") +
      (if (sex != null) Constants.PARAM_SEX + "=" + sex + "|" else "") +
      (if (keywords != null) Constants.PARAM_KEYWORDS + "=" + keywords + "|" else "") +
      (if (categoryIds != null) Constants.PARAM_CATEGORY_IDS + "=" + categoryIds else "")

    if(filterInfo.endsWith("\\|"))
      filterInfo = filterInfo.substring(0, filterInfo.length - 1)

    val sessionId2FilterRDD = sessionId2FullInfoRDD.filter{
      case (sessionId, fullInfo) =>
        var success = true

        if(!ValidUtils.between(fullInfo, Constants.FIELD_AGE, filterInfo, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE))
          success = false

        if(!ValidUtils.in(fullInfo, Constants.FIELD_PROFESSIONAL, filterInfo, Constants.PARAM_PROFESSIONALS))
          success = false

        if (!ValidUtils.in(fullInfo, Constants.FIELD_CITY, filterInfo, Constants.PARAM_CITIES))
          success = false

        if (!ValidUtils.equal(fullInfo, Constants.FIELD_SEX, filterInfo, Constants.PARAM_SEX))
          success = false

        if (!ValidUtils.in(fullInfo, Constants.FIELD_SEARCH_KEYWORDS, filterInfo, Constants.PARAM_KEYWORDS))
          success = false

        if (!ValidUtils.in(fullInfo, Constants.FIELD_CATEGORY_ID, filterInfo, Constants.PARAM_CATEGORY_IDS))
          success = false

        if(success){

          // 只要进入此处，就代表此session数据符合过滤条件，进行总数的计数
          sessionStatAccumulator.add(Constants.SESSION_COUNT)

          val visitLength = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_VISIT_LENGTH).toLong
          val stepLength = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_STEP_LENGTH).toLong

          calculateVisitLength(visitLength, sessionStatAccumulator)
          calculateStepLength(stepLength, sessionStatAccumulator)
        }
        success
    }

    sessionId2FilterRDD
  }

  def calculateVisitLength(visitLength:Long, sessionStatisticAccumulator: SessionAccumulator): Unit ={
    if(visitLength >=1 && visitLength<=3) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_1s_3s)
    } else if (visitLength >= 4 && visitLength <= 6) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_4s_6s)
    } else if (visitLength >= 7 && visitLength <= 9) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_7s_9s)
    } else if (visitLength >= 10 && visitLength <= 30) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_10s_30s)
    } else if (visitLength > 30 && visitLength <= 60) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_30s_60s)
    } else if (visitLength > 60 && visitLength <= 180) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_1m_3m)
    } else if (visitLength > 180 && visitLength <= 600) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_3m_10m)
    } else if (visitLength > 600 && visitLength <= 1800) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_10m_30m)
    } else if (visitLength > 1800) {
      sessionStatisticAccumulator.add(Constants.TIME_PERIOD_30m)
    }

  }

  def calculateStepLength(stepLength:Long, sessionStatisticAccumulator: SessionAccumulator): Unit ={
    if(stepLength >=1 && stepLength <=3){
      sessionStatisticAccumulator.add(Constants.STEP_PERIOD_1_3)
    }else if (stepLength >= 4 && stepLength <= 6) {
      sessionStatisticAccumulator.add(Constants.STEP_PERIOD_4_6)
    } else if (stepLength >= 7 && stepLength <= 9) {
      sessionStatisticAccumulator.add(Constants.STEP_PERIOD_7_9)
    } else if (stepLength >= 10 && stepLength <= 30) {
      sessionStatisticAccumulator.add(Constants.STEP_PERIOD_10_30)
    } else if (stepLength > 30 && stepLength <= 60) {
      sessionStatisticAccumulator.add(Constants.STEP_PERIOD_30_60)
    } else if (stepLength > 60) {
      sessionStatisticAccumulator.add(Constants.STEP_PERIOD_60)
    }
  }


  def getFullInfoData(sparkSession: SparkSession,
                      sessionId2GroupRDD: RDD[(String, Iterable[UserVisitAction])]) = {
    val userId2AggrInfoRDD = sessionId2GroupRDD.map{
      case (sid, iterableAction) =>
        var startTime:Date = null
        var endTime:Date = null

        var userId = -1L

        val searchKeywords = new StringBuffer("")
        val clickCategories = new StringBuffer("")

        var stepLength = 0

        for(action <- iterableAction){
          if(userId == -1L){
            userId = action.user_id
          }

          val actionTime = DateUtils.parseTime(action.action_time)

          if(startTime == null || startTime.after(actionTime))
            startTime = actionTime

          if(endTime == null || endTime.before(actionTime))
            endTime = actionTime

          val searchKeyword = action.search_keyword
          val clickCategory = action.click_category_id

          if(StringUtils.isNotEmpty(searchKeyword) &&
            !searchKeywords.toString.contains(searchKeyword))
            searchKeywords.append(searchKeyword + ",")

          if(clickCategory != -1L &&
            !clickCategories.toString.contains(clickCategory))
            clickCategories.append(clickCategory + ",")

          stepLength += 1
        }

        val searchKw = StringUtils.trimComma(searchKeywords.toString)
        val clickCg = StringUtils.trimComma(clickCategories.toString)

        val visitLength = (endTime.getTime - startTime.getTime) / 1000

        val aggrInfo = Constants.FIELD_SESSION_ID + "=" + sid + "|" +
          Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKw + "|" +
          Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCg + "|" +
          Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|" +
          Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|" +
          Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime)

        (userId, aggrInfo)
    }

    val sql = "select * from user_info"

    import sparkSession.implicits._
    // sparkSession.sql(sql): DateFrame DateSet[Row]
    // sparkSession.sql(sql).as[UserInfo]: DateSet[UserInfo]
    //  sparkSession.sql(sql).as[UserInfo].rdd: RDD[UserInfo]
    // sparkSession.sql(sql).as[UserInfo].rdd.map(item => (item.user_id, item)): RDD[(userId, UserInfo)]
    val userInfoRDD = sparkSession.sql(sql).as[UserInfo].rdd.map(item => (item.user_id, item))

    userId2AggrInfoRDD.join(userInfoRDD).map{
      case (userId, (aggrInfo, userInfo)) =>
        val age = userInfo.age
        val professional = userInfo.professional
        val sex = userInfo.sex
        val city = userInfo.city

        val fullInfo = aggrInfo + "|" + Constants.FIELD_AGE + "=" + age + "|" +
          Constants.FIELD_PROFESSIONAL + "=" + professional + "|" +
          Constants.FIELD_SEX + "=" + sex + "|" +
          Constants.FIELD_CITY + "=" + city

        val sessionId = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_SESSION_ID)

        (sessionId, fullInfo)
    }
  }


  def getActionRDD(sparkSession: SparkSession, taskParam: JSONObject) = {

    val startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)

    val sql = "select * from user_visit_action where date>='" + startDate + "' and date<='" +
      endDate + "'"

    import sparkSession.implicits._
    // sparkSession.sql(sql) : DataFrame   DateSet[Row]
    // sparkSession.sql(sql).as[UserVisitAction]: DateSet[UserVisitAction]
    // sparkSession.sql(sql).as[UserVisitAction].rdd: rdd[UserVisitAction]
    sparkSession.sql(sql).as[UserVisitAction].rdd
  }

  def getProductList(sparkSession: SparkSession) = {
    val sql = "select product_id from product_info"

    import sparkSession.implicits._
    sparkSession.sql(sql).as[ProductInfo2].rdd.map(item => (item.product_id,0L))
  }

}
