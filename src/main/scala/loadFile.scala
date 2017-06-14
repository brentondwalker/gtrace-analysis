/**
 * sbt --warn "run loadFile"
 * sbt --warn "run loadFile filename.txt"
 */

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, DoubleType, TimestampType}

object loadFile {
  
  def main(args: Array[String]) {
    //println(args.deep.mkString("\n"))
    if (args.length <= 1) {
      println("usage: loadFile <infile>")
      System.exit(0)
    }
    val infile = args(1)

    // get a SparkSession in the currently preferred way
    val spark = utils.createSparkSession("perCellTimeSeriesCorrelation")
    import spark.implicits._
    
    // data types:
    // https://spark.apache.org/docs/1.4.0/api/java/org/apache/spark/sql/types/DataType.html
    val taskSchema = StructType(Array(
    		StructField("timestamp", LongType, false),
    		StructField("missing", IntegerType, true),
        StructField("jobid", IntegerType, false),
        StructField("taskix", IntegerType, false),
        StructField("machineid", IntegerType, true),
        StructField("evtype", IntegerType, false),
        StructField("username", StringType, true),
        StructField("sclass", IntegerType, false),
        StructField("priority", IntegerType, false),
        StructField("cpureq", DoubleType, false),
        StructField("ramreq", DoubleType, false),
        StructField("hdreq", DoubleType, false),
        StructField("constraint", IntegerType, false)))

    val df = spark.read
      .option("header","true")
      .option("sep","\t")
      .option("nullValue","NULL")
      .option("mode","DROPMALFORMED")
      .schema(lSchema)
      .csv(infile)

    println("schema: ")
    df.printSchema()
    println("df="+df.show())

    println("sorting...")
    val df2 = df.sort(asc("timestamp"))
    println("done!")
    
    df2.take(20).foreach(println)
    
    spark.stop
  }


}



