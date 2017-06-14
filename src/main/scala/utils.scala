import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.unix_timestamp
import org.apache.spark.storage.StorageLevel._


/**
 * Some utility functions that will be convenient to factor out
 * because the code is re-used all over.  This way we can change it
 * in one place.
 */
object utils {

  def createSparkSession(appName: String): SparkSession = {
    return SparkSession.builder
      .appName("gtrace-analysis")
      .config("LogLevel", "WARN")
      .getOrCreate()
  }
    
}


