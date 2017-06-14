import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, LongType, DoubleType, TimestampType}


object gtraceReader {

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
			StructField("constraint", IntegerType, false)));
			
	def readTaskEvents(spark: SparkSession, filename: String): Dataset[Row] = {
			return spark.read
				  .option("sep",",")
				  .option("nullValue","NULL")
				  .option("mode","DROPMALFORMED")
				  .schema(taskSchema)
				  .csv(filename)
	}

}