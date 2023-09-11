import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.xml._


object Activities {
    def main(args: Array[String]) {
        val ds = spark.read.csv("loudacre/UserBehavior.csv")
        val df = ds.withColumnRenamed("_c0", "user_id").withColumnRenamed("_c1", "item_id").withColumnRenamed("_c2","category_id").withColumnRenamed("_c3","behavior_type").withColumnRenamed("_c4","time_stamp")
        val activities_per_user = df.groupBy("user_id").count()
        val total_pv = df.where($”behavior_type” === "pv").count()
        val total_buy = df.where($"behavior_type" === "buy").count()
        val total_cart = df.where($"behavior_type" === "cart").count()
        val total_fav = df.where($"behavior_type" === "fav").count()
        val each_behavior_per_user = df.groupby("user_id", "behavior").count()
}