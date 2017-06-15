import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, LongType, DoubleType, TimestampType}


object gtraceReader {

	val taskSchema = StructType(Array(
			StructField("timestamp", LongType, false),
			StructField("missing", IntegerType, true),
			StructField("jobid", LongType, false),
			StructField("taskix", LongType, false),
			StructField("machineid", LongType, true),
			StructField("evtype", IntegerType, false),
			StructField("username", StringType, true),
			StructField("sclass", IntegerType, true),
			StructField("priority", IntegerType, true),
			StructField("cpureq", DoubleType, true),
			StructField("ramreq", DoubleType, true),
			StructField("hdreq", DoubleType, true),
			StructField("constraint", IntegerType, true)));
			
	def readTaskEvents(spark: SparkSession, filename: String): Dataset[Row] = {
			return spark.read
				  .option("sep",",")
				  .option("nullValue","NULL")
				  .option("mode","DROPMALFORMED")
				  .schema(taskSchema)
				  .csv(filename)
	}

}