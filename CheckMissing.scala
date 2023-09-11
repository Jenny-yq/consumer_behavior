import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.xml._


object Missing {
    def main(args: Array[String]) {
    	val ds = spark.read.csv("loudacre/UserBehavior.csv")
        val df = ds.withColumnRenamed("_c0", "user_id").withColumnRenamed("_c1", "item_id").withColumnRenamed("_c2","category_id").withColumnRenamed("_c3","behavior_type").withColumnRenamed("_c4","time_stamp")
        df.filter(col("user_id").isNull || col("user_id") === "").count()
        df.filter(col("item_id").isNull || col("item_id") === "").count()
        df.filter(col("category_id").isNull || col("category_id") === "").count()
        df.filter(col("behavior_type").isNull || col("behavior_type") === "").count()
        df.filter(col("time_stamp").isNull || col("time_stamp") === "").count()
}