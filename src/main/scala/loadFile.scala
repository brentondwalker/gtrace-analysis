/**
 * sbt --warn "run loadFile"
 * sbt --warn "run loadFile filename.txt"
 */

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, LongType, DoubleType, TimestampType}
import org.apache.spark.storage.StorageLevel._


object loadFile {
  
  def main(args: Array[String]) {
    //println(args.deep.mkString("\n"))
    if (args.length < 1) {
      println("usage: loadFile <infile>")
      System.exit(0)
    }
    val task_infile = args(0)
    val job_infile = args(1)

    // get a SparkSession in the currently preferred way
    val spark = utils.createSparkSession("gtrace-analsis")
    import spark.implicits._
    
    val task_event_df = gtraceReader.readTaskEvents(spark, task_infile).persist(MEMORY_AND_DISK);
    println("task schema: ")
    task_event_df.printSchema()
    
    val job_event_df = gtraceReader.readJobEvents(spark, job_infile).persist(MEMORY_AND_DISK);
    println("job schema: ")
    job_event_df.printSchema()
    
    println("read in task records: "+task_event_df.count())
    println("read in job records: "+job_event_df.count())
    println("taskdf="+task_event_df.show())
    println("jobdf="+job_event_df.show())
    
    val taskdf = EventDataTransformer.transformTaskData(spark, task_event_df)
    println("transformed task data:")
    taskdf.show()
    
    val jobdf = EventDataTransformer.transformJobData(spark, job_event_df)
    println("transformed job data:")
    jobdf.show()
    
    
    
    Thread sleep 1000
    
    spark.stop
  }


}



