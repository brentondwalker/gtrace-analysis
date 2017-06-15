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
    val infile = args(0)

    // get a SparkSession in the currently preferred way
    val spark = utils.createSparkSession("gtrace-analsis")
    import spark.implicits._
    
    val taskdf = gtraceReader.readTaskEvents(spark, infile).persist(MEMORY_AND_DISK);
    
    println("read in records: "+taskdf.count())

    println("schema: ")
    taskdf.printSchema()
    println("taskdf="+taskdf.show())

    println("sorting...")
    val taskdf2 = taskdf.sort(asc("timestamp"))
    println("done!")
    
    taskdf2.take(20).foreach(println)
    
    val taskdf3 = EventDataTransformer.transformTaskData(spark, taskdf2)
    println("with extra columns:")
    taskdf3.show()
    
    spark.stop
  }


}



