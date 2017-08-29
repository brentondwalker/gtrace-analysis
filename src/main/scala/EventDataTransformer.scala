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
	
  def transformTaskData(spark: SparkSession, df: Dataset[Row], join_metadata:Boolean) : Dataset[Row] = {
    import spark.implicits._
    
    println("observed task event types:")
    df.groupBy("evtype").count().show()
    
    //println("task finish events:")
    //taskdf.where($"evtype" === 4).sort(asc("timestamp")).show(10000)
    
    val tmp = df.withColumn("arrive", when($"evtype" === 0, $"timestamp").otherwise(0))
          .withColumn("schedule", when($"evtype" === 1, $"timestamp").otherwise(0))
          .withColumn("finish", when($"evtype" === 4, $"timestamp").otherwise(0))
          .withColumn("fail", when($"evtype" === 2 or $"evtype" === 3 or $"evtype" === 5 or $"evtype" === 6, $"timestamp").otherwise(0));
    
     val taskds = tmp.groupBy("jobid", "taskix")
          .agg(max("arrive").alias("arrive"),max("schedule").alias("schedule"),max("finish").alias("finish"),max("fail").alias("fail"))
          .where($"arrive" > 0 and $"arrive" < $"schedule" and $"schedule" < $"finish" and $"fail" < $"schedule")
          .withColumn("size", $"finish" - $"schedule")
          .withColumn("waittime", $"schedule" - $"arrive")
          .sort(asc("jobid"), asc("arrive"));
     
     if (join_metadata) {
       val taskds2 = taskds.as("taskds").join(df.as("task_event_ds"), $"task_event_ds.jobid" === $"taskds.jobid" && $"task_event_ds.taskix" === $"taskds.taskix" && $"evtype" === 4);
       return taskds2.select($"taskds.jobid", $"taskds.taskix", $"arrive", $"schedule", $"finish", $"fail", $"size", $"waittime", $"machineid", $"username", $"sclass", $"priority", $"cpureq", $"ramreq", $"hdreq");
     }
     
     return taskds;     
  }
  
  def transformJobData(spark: SparkSession, df: Dataset[Row], join_metadata:Boolean) : Dataset[Row] = {
    import spark.implicits._
    
    println("observed job event types:")
    df.groupBy("evtype").count().show()
        
    val tmp = df.withColumn("arrive", when($"evtype" === 0, $"timestamp").otherwise(0))
          .withColumn("schedule", when($"evtype" === 1, $"timestamp").otherwise(0))
          .withColumn("finish", when($"evtype" === 4, $"timestamp").otherwise(0))
          .withColumn("fail", when($"evtype" === 2 or $"evtype" === 3 or $"evtype" === 5 or $"evtype" === 6, $"timestamp").otherwise(0));
    
     val jobds = tmp.groupBy("jobid")
          .agg(max("arrive").alias("arrive"),max("schedule").alias("schedule"),max("finish").alias("finish"),max("fail").alias("fail"))
          .where($"arrive" > 0 and $"arrive" < $"schedule" and $"schedule" < $"finish" and $"fail" < $"schedule")
          .withColumn("size", $"finish" - $"schedule")
          .withColumn("waittime", $"schedule" - $"arrive")
          .sort(asc("arrive"));
     
     if (join_metadata) {
       val jobds2 = jobds.filter("fail==0").join(df.as("job_event_ds"), $"job_event_ds.jobid" === $"jobds.jobid" && $"evtype" === 4 );
       return jobds2.select($"jobds.jobid", $"jobds.arrive", $"jobds.schedule", $"jobds.finish", $"jobds.fail", $"jobds.size", $"jobds.waittime", $"job_event_ds.username", $"job_event_ds.sclass");
     }
     
     return jobds;
  }
  
  
  /**
   * In some cases we want the metadata from the event table available with the
   * transformed data.  The extra metadata columns are "machineid", "username", 
   * "sclass", "priority", "cpureq", "ramreq", "hdreq".
   * 
   * The taskds Dataset passed in should be a task Dataset transformed by the function above.
   * The task_event_ds Dataset should be the task event data read from the files.
   */
  def joinTaskMetadata(spark:SparkSession, taskds:Dataset[Row], task_event_ds:Dataset[Row]): Dataset[Row] = {
    import spark.implicits._
    
    val taskds2 = taskds.as("taskds").join(task_event_ds.as("task_event_ds"), $"task_event_ds.jobid" === $"taskds.jobid" && $"task_event_ds.taskix" === $"taskds.taskix" && $"evtype" === 4);
    return taskds2.select($"taskds.jobid", $"taskds.taskix", $"arrive", $"schedule", $"finish", $"fail", $"size", $"waittime", $"machineid", $"username", $"sclass", $"priority", $"cpureq", $"ramreq", $"hdreq");
  }
  
  
  /**
   * We also sometimes want the job metadata joined back onto the table.
   * The extra metadata columns are "username", "sclass".
   */
  def joinJobMetadata(spark:SparkSession, jobds:Dataset[Row], job_event_ds:Dataset[Row]): Dataset[Row] = {
    import spark.implicits._
    
    val jobds2 = jobds.filter("fail==0").join(job_event_ds.as("job_event_ds"), $"job_event_ds.jobid" === $"jobds.jobid" && $"evtype" === 4 );
    return jobds2.select($"jobds.jobid", $"jobds.arrive", $"jobds.schedule", $"jobds.finish", $"jobds.fail", $"jobds.size", $"jobds.waittime", $"job_event_ds.username", $"job_event_ds.sclass");
  }
  
  
}