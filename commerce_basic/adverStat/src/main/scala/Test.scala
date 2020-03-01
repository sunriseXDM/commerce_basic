import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Test {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("test")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    val tuples: Array[(String, Int)] = Array(("a", 1), ("a", 3), ("b", 3), ("b", 5), ("c", 4))

    val rdd: RDD[(String, Int)] = sparkSession.sparkContext.makeRDD(tuples)

    //rdd.foreach(println(_))

    rdd.combineByKey(
      v => (v,1),
      (acc:(Int,Int),newValue) => (acc._1+newValue,acc._2+1),
      (acc1:(Int,Int),acc2:(Int,Int)) => (acc1._1+acc2._1,acc1._2+acc2._2)
    )

    val value = rdd.combineByKey(v=>(v,1),
      (acc:(Int,Int),newV)=>(acc._1+newV,acc._2+1),
      (acc1:(Int,Int),acc2:(Int,Int))=>(acc1._1+acc2._1,acc1._2+acc2._2))



    value.foreach(println(_))

  }
}
