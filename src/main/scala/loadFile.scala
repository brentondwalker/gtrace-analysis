/**
 * sbt --warn "run loadFile"
 * sbt --warn "run loadFile filename.txt"
 * 
 * **OR**
 * 
 * Build the project with:
 * sbt package
 * 
 * Run the main:
 * bin/spark-submit --master spark://sparkserver:7077 gtrace-analysis/target/scala-2.11/gtrace-analysis_2.11-1.0.jar task_events/part-00495-of-00500.csv.gz job_events/part-00495-of-00500.csv.gz
 */

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, LongType, DoubleType, TimestampType}
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.rdd.DoubleRDDFunctions

object loadFile {
  
  def main(args: Array[String]) {
    //println(args.deep.mkString("\n"))
    if (args.length < 2) {
      println("usage: loadFile <task_file(s)> <job_trace_file(s)>")
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
    
    val taskdf = EventDataTransformer.transformTaskData(spark, task_event_df, true)
    println("transformed task data:")
    taskdf.show()
    
    val jobdf = EventDataTransformer.transformJobData(spark, job_event_df, true)
    println("transformed job data:")
    jobdf.show()
    
    val dd = taskdf.select("size").persist(MEMORY_AND_DISK);
    val ddc = dd.collect()
    ddc(1).getLong(0)
    val ddcl = ddc.map(x => x.getLong(0))
    
    val ddd = dd.map(x => x.getLong(0)).map( _.toDouble )
    val ddh = ddd.rdd.histogram(5)
    ddh._1.toList
    
    
    Thread sleep 1000
    
    spark.stop
  }


}



