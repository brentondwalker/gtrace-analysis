# gtrace-analysis
spark code for analyzing google cluster traces

# Build
This is an sbt project.  You may need a newer version of sbt than what Ubuntu provides by default.  I am using sbt 0.13.11.

To build, cd to the project director, and do: ```sbt package```

This will produce a jar file: `target/scala-2.11/gtrace-analysis_2.11-1.0.jar`.

# Run

## Submit to cluster
I run this by submiting it to a standalone cluster.  In the following command you need to fill in the `<your_master>` and `<path>` parts to match your system.

```
bin/spark-submit --master spark://<your_master>:7077 <path>/gtrace-analysis/target/scala-2.11/gtrace-analysis_2.11-1.0.jar <path>/task_events/part-00495-of-00500.csv.gz <path>/job_events/part-00495-of-00500.csv.gz
```

## local mode
In order to run in local mode, some code needs to be changed to set the master to `local[4]` or whatever you want to use.  Making this more flexible and automated is a TODO item.


## Interactively with sbt console
This kind-of works too.  You must cd to the project directory and do:
```
sbt console
```
Of course this does not run the main() routine, or have any command line arguments set.  Suppose you want to run in local mode and you have two data files in the project directory: `task_events/part-00494-of-00500.csv.gz` and `job_events/part-00484-of-00500.csv.gz`.  Then you could run the following in the console:
```
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, LongType, DoubleType, TimestampType}
import org.apache.spark.storage.StorageLevel._

val spark = utils.createSparkSession("example", "local[2]")
import spark.implicits._

val task_infile = "task_events/part-00494-of-00500.csv.gz"
val job_infile = "job_events/part-00484-of-00500.csv.gz"

val task_event_df = gtraceReader.readTaskEvents(spark, task_infile).persist(MEMORY_AND_DISK);
println("taskdf="+task_event_df.show())

val job_event_df = gtraceReader.readJobEvents(spark, job_infile).persist(MEMORY_AND_DISK);
println("jobdf="+job_event_df.show())

val taskdf = EventDataTransformer.transformTaskData(spark, task_event_df)
taskdf.show()

val jobdf = EventDataTransformer.transformJobData(spark, job_event_df)
jobdf.show()
```

