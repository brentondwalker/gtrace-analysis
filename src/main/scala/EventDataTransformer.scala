import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, LongType, DoubleType, TimestampType}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._ // for `when`


/**
 * This will be code to turn job/task event data into a table of
 * job/task data with arrive/start/end times and total durations computed
 */
object EventDataTransformer {
	
  def transformTaskData(spark: SparkSession, taskdf: Dataset[Row]) : Dataset[Row] = {
    import spark.implicits._
    
    println("observed event types:")
    taskdf.groupBy("evtype").count().show()
    
    //println("task finish events:")
    //taskdf.where($"evtype" === 4).sort(asc("timestamp")).show(10000)
    
    val tmp = taskdf.withColumn("arrive", when($"evtype" === 0, $"timestamp").otherwise(0))
          .withColumn("schedule", when($"evtype" === 1, $"timestamp").otherwise(0))
          .withColumn("finish", when($"evtype" === 4, $"timestamp").otherwise(0))
          .withColumn("fail", when($"evtype" === 2 or $"evtype" === 3 or $"evtype" === 5 or $"evtype" === 6, $"timestamp").otherwise(0));
    
     return tmp.groupBy("jobid", "taskix")
          .agg(max("arrive").alias("arrive"),max("schedule").alias("schedule"),max("finish").alias("finish"),max("fail").alias("fail"))
          .where($"arrive" > 0 and $"arrive" < $"schedule" and $"schedule" < $"finish" and $"fail" < $"schedule")
          .withColumn("tasksize", $"finish" - $"schedule")
          .withColumn("waittime", $"schedule" - $"arrive")
          .sort(asc("jobid"), asc("arrive"))
  }
  
  
}