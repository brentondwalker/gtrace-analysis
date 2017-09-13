import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._ // for `when`
import scala.util.control.Breaks._
import org.apache.spark.sql.expressions.Window
import breeze.linalg.{sum => bsum, DenseMatrix, DenseVector}
import breeze.stats.regression.leastSquares


class IntertimeProcessData extends Serializable {

  var is_arrival:Boolean = true
  
  var raw_increments:Array[Long] = null
  var increments:Array[Double] = null
  
  
  /**
   * take the absolute arrival times, convert it to an inter-arrival process, and
   * then normalize the inter-arrival process to have a desired rate.
   */
  def loadArrivalData(spark:SparkSession, jobds:Dataset[Row], uname:String, lambda:Double = 1.0): IntertimeProcessData = {
    import spark.implicits._
    
    is_arrival = true
    
    val arrivals = jobds.filter("username='"+uname+"'").select("arrive")
    val w = org.apache.spark.sql.expressions.Window.orderBy("arrive")

    // we want to take off the first inter-arrival time because it's relative to zero
    val firstArrival = arrivals.agg(min("arrive")).head().getLong(0)
    val interArrivals = arrivals.withColumn("interarrival",  col("arrive")-lag(col("arrive"), 1, 0).over(w)).filter($"arrive" > firstArrival).sort("arrive")
    
    raw_increments = interArrivals.select("interarrival").collect().map( r => r.getLong(0) )
    val increment_mean:Double = bsum(raw_increments)/raw_increments.length
    
    increments = raw_increments.map( x => x.toDouble/(lambda*increment_mean) )
    
    println("loadArrivalData new mean is "+(increments.sum/increments.length))
    
    return this
  }
  
  
  /**
   * take a service time process and normalize it to have a specific rate.
   */
  def loadServiceData(spark:SparkSession, taskds:Dataset[Row], uname:String, mu:Double = 1.0): IntertimeProcessData = {
    is_arrival = false

    val services = taskds.filter("username='"+uname+"'").filter("fail = 0").filter("taskix = 0").sort("arrive").select("size")
    
    raw_increments = services.collect().map( r => r.getLong(0) )
    val increment_mean:Double = bsum(raw_increments)/raw_increments.length
    increments = raw_increments.map( x => x.toDouble/(mu*increment_mean) )
    
    println("loadServiceData new mean is "+(increments.sum/increments.length))
    
    return this
  }
  
  
  /**
   * After we load the data, we may want to adjust the mean rate again.
   * 
   * I'm not sure this is good scala style, to adjust the values in-place
   * vs returning a new object.
   */
  def renormalize(alpha:Double) = {
    val m = bsum(increments) / increments.length
    increments = increments.map( x => x/(m*alpha) )
    println("renormalize new mean is "+(increments.sum/increments.length))
  }
  
  
  /**
   * function to compute MGF as a function of l-(n-m) for a single value of theta
   */
  def computeMgf(max_l:Integer, theta_arg:Double): Array[Array[Double]] = {
    var theta = theta_arg

    if (theta > 0.0 && is_arrival) {
      println("WARNING: computeMgf(): you should pass in a negative theta for arrival process...fixing it for you")
      theta = -theta_arg
    }
    
    if (theta < 0.0 && !is_arrival) {
      println("WARNING: computeMgf(): you should pass in a positive theta for service process...fixing it for you")
      theta = -theta_arg
    }
    
    val mgf = Array.ofDim[Double](max_l,3)
    var max_valid_l = 0;
    
    val increments_mn = Array.fill[Double](increments.length - max_l)(0.0)
    breakable {
    for ( l <- 0 until max_l ) {
      for ( i <- 0 until increments_mn.length ) { increments_mn(i) += increments(i+l) }
      mgf(l)(0) = l
      mgf(l)(1) = increments_mn.map( x => Math.exp(theta*x)).sum/increments_mn.length
      mgf(l)(2) = Math.log(mgf(l)(1))/theta;
      if (mgf(l)(2).isInfinity) {
          max_valid_l = l-1;
          break
      }
      max_valid_l = l
    }}
    
    return mgf.slice(0,max_valid_l)
  }
  
  

}
