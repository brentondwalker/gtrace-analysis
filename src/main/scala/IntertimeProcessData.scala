import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._ // for `when`
import scala.util.control.Breaks._
import org.apache.spark.sql.expressions.Window
import breeze.linalg.{sum => bsum, DenseMatrix, DenseVector}
import breeze.stats.regression.leastSquares
import scala.math.random
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.row_number

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
    //val firstArrival = arrivals.agg(min("arrive")).head().getLong(0)
    //val interArrivals = arrivals.withColumn("interarrival",  col("arrive")-lag(col("arrive"), 1, 0).over(w)).filter($"arrive" > firstArrival).sort("arrive").select("interarrival")
    val interArrivals = arrivals.withColumn("interarrival",  col("arrive")-lag(col("arrive"), 1, 0).over(w)).sort("arrive").select("interarrival")
    
    raw_increments = interArrivals.collect().map( r => r.getLong(0) ).drop(1)
    val increment_mean:Double = bsum(raw_increments)/raw_increments.length
    
    increments = raw_increments.map( x => x.toDouble/(lambda*increment_mean) )
    
    println("loadArrivalData new mean is "+(increments.sum/increments.length))
    
    return this
  }
  
  
  /**
   * take a service time process and normalize it to have a specific rate.
   * 
   * Earlier versions of this code would only take task zero, if its size
   * exists.  This new version does the same as the python code, and takes
   * the lowest-index task that finishes, even if it was not the lowest-index
   * task submitted.
   * 
   * XXX the task sizes computed by the spark code are sometimes a little
   *     larger than the ones computed by the old python code.  Not always, but
   *     sometimes.  There are also occasionally datapoints missing.
   *     This could happen if there are multiple finish events for a task?
   */
  def loadFirstTaskServiceData(spark:SparkSession, taskds:Dataset[Row], uname:String, mu:Double = 1.0): IntertimeProcessData = {
    is_arrival = false

    //val services = taskds.filter("username='"+uname+"'").filter("fail = 0").filter("taskix = 0").sort("arrive").select("size")
    //taskds.filter("username='"+uname+"'").filter("size >= 0").groupBy("jobid").agg(min("taskix"))
    //val services = taskds.filter("username='"+uname+"'").filter("taskix = 0").filter("size >= 0").sort("arrive").select("size")
    val w = Window.partitionBy("jobid").orderBy(col("taskix")) //.asc())
    val services = taskds.filter("username='"+uname+"'").filter("size >= 0")
                         .withColumn("task_rank", row_number().over(w)).filter("task_rank=1")
                         .sort("arrive")
    
    raw_increments = services.select("size").collect().map( r => r.getLong(0) )
    val increment_mean:Double = bsum(raw_increments)/raw_increments.length
    increments = raw_increments.map( x => x.toDouble/(mu*increment_mean) )
    
    println("loadServiceData new mean is "+(increments.sum/increments.length))
    
    return this
  }
  
  
  /**
   * It is useful to test the algorithms with iid exponential data.
   */
  def generateExponentialData(rate:Double, num:Integer, is_arrival:Boolean): IntertimeProcessData = {
    this.is_arrival = is_arrival
    
    increments = Array.fill[Double](num){0.0}.map( x => -Math.log(random)/rate )
    raw_increments = increments.map( x => (x*1.0e12).toLong )
    
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
    return computeMgf(1, max_l, theta_arg)
  }

  
  /**
   * Method to compute MGF
   * 
   * The MGF computation is different for arrivals and services.  The first
   * thing is the sign of theta used.  The second thing is that A(m,n) is
   * a sum of (n-m) increments, but S(m,n) is a sum of (n-m+1) increments.
   * That is why I do this "service_extra_term" nonsense.
   */
  def computeMgf(min_l:Int, max_l:Int, theta_arg:Double): Array[Array[Double]] = {
    var theta = theta_arg

    if (theta > 0.0 && is_arrival) {
      println("WARNING: computeMgf(): you should pass in a negative theta for arrival process...fixing it for you")
      theta = -theta_arg
    }
    
    if (theta < 0.0 && !is_arrival) {
      println("WARNING: computeMgf(): you should pass in a positive theta for service process...fixing it for you")
      theta = -theta_arg
    }
    
    val mgf = Array.ofDim[Double]((max_l-min_l+1),3)
    var max_valid_l = 0;
    
    val increments_mn = if (is_arrival) Array.fill[Double](increments.length - max_l)(0.0) else Array.fill[Double](increments.length - max_l - 1)(0.0)
    var service_extra_term = 0
    if (!is_arrival) {
      for ( i <- 0 until increments_mn.length ) { increments_mn(i) += increments(0) }
      service_extra_term = 1
    }
    for ( l <- 0 until min_l ) {
      for ( i <- 0 until increments_mn.length ) { increments_mn(i) += increments(i+l+service_extra_term) }
    }
    breakable {
    for ( l <- min_l until max_l ) {
      for ( i <- 0 until increments_mn.length ) { increments_mn(i) += increments(i+l+service_extra_term) }
      mgf(l-min_l)(0) = l
      mgf(l-min_l)(1) = increments_mn.map( x => Math.exp(theta*x)).sum/increments_mn.length
      mgf(l-min_l)(2) = Math.log(mgf(l-min_l)(1))/theta;
      if (mgf(l-min_l)(2).isInfinity) {
          max_valid_l = l-1;
          break
      }
      max_valid_l = l
    }}
    
    return mgf.slice(0,max_valid_l-min_l)
  }
  
  

}
