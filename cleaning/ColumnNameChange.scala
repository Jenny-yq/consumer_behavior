import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.xml._

object Renaming {
    def main(args: Array[String]) {
        val ds = spark.read.csv("loudacre/UserBehavior.csv")
        val df = ds.withColumnRenamed("_c0", "user_id").withColumnRenamed("_c1", "item_id").withColumnRenamed("_c2","category_id").withColumnRenamed("_c3","behavior_type").withColumnRenamed("_c4","time_stamp")
    }
}