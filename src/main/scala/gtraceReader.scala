import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, LongType, DoubleType, TimestampType}


object gtraceReader {

  
  /**
   * The job events table contains the following fields:
   *   1. timestamp
   *   2. missing info
   *   3. job ID
   *   4. event type
   *   5. user name
   *   6. scheduling class
   *   7. job name
   *   8. logical job name
   * 
   */
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

	/**
	 * The task events table contains the following fields: 
	 *   1. timestamp
	 *   2. missing info
	 *   3. job ID
	 *   4. task index - within the job
	 *   5. machine ID
	 *   6. event type
	 *   7. user name
	 *   8. scheduling class
	 *   9. priority
	 *   10. resource request for CPU cores
	 *   11. resource request for RAM
	 *   12. resource request for local disk space
	 *   13. different-machine constraint
	 */
	val jobSchema = StructType(Array(
			StructField("timestamp", LongType, false),
			StructField("missing", IntegerType, true),
			StructField("jobid", LongType, false),
			StructField("evtype", IntegerType, false),
			StructField("username", StringType, true),
			StructField("sclass", IntegerType, true),
			StructField("jobname", StringType, true),
			StructField("logicaljobname", StringType, true)));

	/**
	 * read in the task events file(s)
	 */
	def readTaskEvents(spark: SparkSession, filename: String): Dataset[Row] = {
			return spark.read
					.option("sep",",")
				  .option("nullValue","NULL")
				  .option("mode","DROPMALFORMED")
				  .schema(taskSchema)
				  .csv(filename)
	}

	/**
	 * read in the job events file(s)
	 * 
	 * This immediately drops those extra string fields that we don't plan to use.
	 * Probably this stops the program from trying to save them iof at all,
	 * but I'm not sure.
	 */
	def readJobEvents(spark: SparkSession, filename: String): Dataset[Row] = {
			return spark.read
					.option("sep",",")
					.option("nullValue","NULL")
					.option("mode","DROPMALFORMED")
					.schema(jobSchema)
					.csv(filename)
					.drop("jobname")
					.drop("logicaljobname")
	}

	
}