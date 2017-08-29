import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, LongType, DoubleType, TimestampType}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._ // for `when`
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.sql.expressions.Window
import scala.collection.mutable.ListBuffer

/**
 * This will contain code to compute and analyze moment generating
 * functions of arrival and service process data.
 */

object MgfAnalysis {
  
  case class SigmaRho(sigmaa:Double, rhoa:Double, sigmas:Double, rhos:Double, theta:Double, alpha:Double)
  
  /**
   * returns an array containing (username,numjobs) tuples, sorted by numjobs
   */
  def computeTopUsers(jobds:Dataset[Row]): Array[(String,Long)] = {
    val userJobCounts = jobds.groupBy("username").count().sort(desc("count"));
    return userJobCounts.filter("count > 30000").collect().map( x => (x.getString(0), x.getLong(1)) )
  }
  
  
  /**
   * compute a bunch of feasible envelope parameters for the given processes
   * normalized to the given rates.  These envelope parameters can be used
   * to compute the G|G fork-join bounds.
   */
  def computeEnvelope(spark:SparkSession, jobds:Dataset[Row], uname:String, lambda:Double, mu:Double) = {
    val num_thetas = 10;
    val max_l = 2000;
    val arrivals = getNormalizedArrivals(spark, jobds, uname, lambda);
    val services = getNormalizedServices(spark, jobds, uname, mu);
    
    val theta1 = findIntersectionTheta(spark, arrivals, services, 1)
    val theta1k = findIntersectionTheta(spark, arrivals, services, 1000)
    val theta2k = findIntersectionTheta(spark, arrivals, services, 2000)
    println("theta1="+theta1+"\ttheta1k="+theta1k+"\ttheta2k="+theta2k)
    
    // based on these intersection thetas, perturb them to get a list of
    // many thetas to test.
    // Put them into an RDD.  This will allow us to compute envelopes for
    // many thetas in parallel.
    val theta_list = ListBuffer[Double]() ++ List(theta1, theta1k, theta2k);
    for (i <- 1 to num_thetas) {
        val factor = Math.pow(0.98,i);
        theta_list ++= List(factor*theta1, factor*theta1k, factor*theta2k);
    }
    val thetardd = spark.sparkContext.parallelize(theta_list, theta_list.length);
    
    // collect the arrival and service data for this user and compute
    // the 2D lags array at every possible lag.
    // The size of these arrays will be on the order of max_l * arrivals.length
    val lags_array_arrival = processLagsArray(arrivals.select("interarrivalN").collect().map(r => r.getDouble(0)), max_l)
    val lags_array_service = processLagsArray(services.select("sizeN").collect().map(r => r.getDouble(0)), max_l)
    
    // finally compute a bunch of feasible envelopes.  Each envelope consists of
    // (sigmaa:Double, rhoa:Double, sigmas:Double, rhos:Double, theta:Double, alpha:Double)
    // The alpha is actually computed from the other parts, but is convenient to have there.
    var srLists = thetardd.map( theta => fitSigmaRhoLines(lags_array_arrival, lags_array_service, theta)).reduce(_ ++ _)
  }
  
  
  /**
   * given a theta and the lags array for an arrival and service process, compute a
   * bunch of feasible sigma/rho envelopes.
   */
  def fitSigmaRhoLines(lags_array_arrival:Array[Array[Double]], lags_array_service:Array[Array[Double]], theta:Double): ListBuffer[SigmaRho] = {
    val max_l = lags_array_arrival.length

    val numlines = 10
    
    val srList = ListBuffer[SigmaRho]()
    
    // first need to compute the MGFs for this theta
    val logMgfa = Array[Double](max_l)
    val logMgfs = Array[Double](max_l)
    var max_valid_l = 0;

    for ( l <- 0 until max_l ) {
        logMgfa(l) = Math.log( lags_array_arrival(l).map( x => Math.exp(theta*x)).sum/lags_array_arrival(l).length ) / theta
        logMgfs(l) = Math.log( lags_array_service(l).map( x => Math.exp(theta*x)).sum/lags_array_service(l).length ) / theta
        if (!(logMgfa(l).isInfinity || logMgfs(l).isInfinity)) {
            max_valid_l = l
        }
    }
    
    // try fitting lines
    // first estimate the max and min slopes, and then compute the envelopes for several slopes in between
    var min_rhoa = 1.0e99;
    var max_rhoa = 0.0;
    var min_rhos = 1.0e99;
    var max_rhos = 0.0;
    
    // to keep from computing the slope of noise, compute slope over a widow of 1/10 of the data
    val lwin = Math.round(max_valid_l/10)
    for ( l <- 0 until (max_valid_l-lwin)) {
        var temp_rhoa = (logMgfa(l+lwin) - logMgfa(l)) / (lwin)
        min_rhoa = Math.min(temp_rhoa, min_rhoa)
        max_rhoa = Math.max(temp_rhoa, max_rhoa)
        
        var temp_rhos = (logMgfs(l+lwin) - logMgfs(l)) / (lwin)
        min_rhos = Math.min(temp_rhos, min_rhos)
        max_rhos = Math.max(temp_rhos, max_rhos)
    }
    
    // also require that the minimum rhoa is greater or equal to the minimum rhos
    // i.e. there is no reason to consider infeasible solutions
    min_rhoa = Math.max(min_rhoa, min_rhos)
    
    for ( is <- 0 until numlines ) {
        var rhos = min_rhos + (max_rhos - min_rhos) * ((1.0*is)/numlines)
        
        var sigmas = -1.0e99
        for ( l <- 0 to logMgfs.length ) { sigmas = Math.max( sigmas, (logMgfs(l) - l*rhos)) }
        
        for (ia <- 1 to numlines ) {
            var rhoa = min_rhoa + (max_rhoa - min_rhoa) * ((1.0*ia)/numlines)
            
            var sigmaa = -1.0e99
            for ( l <- 0 to logMgfa.length ) { sigmaa = Math.max( sigmaa, (l*rhos - logMgfs(l))) }
            var alpha = Math.exp(theta*(sigmaa+sigmas)) / ( 1 - Math.exp(-theta*(rhoa-rhos)) );
            srList += SigmaRho(sigmaa, rhoa, sigmas, min_rhos, theta, alpha)
        }
    }
    
    return srList;
  }
  
  
  /**
   * take the absolute arrival times, convert it to an inter-arrival process, and
   * then normalize the inter-arrival process to have a desired rate.
   */
  def getNormalizedArrivals(spark:SparkSession, jobds:Dataset[Row], uname:String, lambda:Double): Dataset[Row] = {
    import spark.implicits._
    
    val arrivals = jobds.filter("username='"+uname+"'").select("arrive")
    val w = org.apache.spark.sql.expressions.Window.orderBy("arrive")

    // we want to take off the first inter-arrival time because it's relative to zero
    val firstArrival = arrivals.agg(min("arrive")).head().getLong(0)
    val interArrivals = arrivals.withColumn("interarrival",  col("arrive")-lag(col("arrive"), 1, 0).over(w)).filter($"arrive" > firstArrival).sort("arrive")
    
    // normalize the interArrivals
    val arrivalMean = interArrivals.agg(mean("interarrival")).head().getDouble(0)
    print("arrivalMean="+arrivalMean+"\n")
    val interArrivalsN = interArrivals.withColumn("interarrivalN", col("interarrival")/(lambda*arrivalMean)).persist(MEMORY_AND_DISK);
    val newArrivalMean = interArrivalsN.agg(avg("interarrivalN")).head().getDouble(0)
    print("newArrivalMean="+newArrivalMean+"\n")
    
    return interArrivalsN;
  }
  
  
  /**
   * take a service time process and normalize it to have a specific rate.
   */
  def getNormalizedServices(spark:SparkSession, taskds:Dataset[Row], uname:String, mu:Double): Dataset[Row] = {
    val services = taskds.filter("username='"+uname+"'").filter("fail = 0").filter("taskix = 0").select("arrive", "size")
    
    val serviceMean = services.agg(mean("size")).head().getDouble(0)
    print("serviceMean="+serviceMean+"\n")
    val servicesN = services.withColumn("sizeN", col("size")/(mu*serviceMean)).persist(MEMORY_AND_DISK);
    val newServiceMean = servicesN.agg(avg("sizeN")).head().getDouble(0)
    print("newServiceMean="+newServiceMean+"\n")
    
    return servicesN;
  }
  
  
  /**
   * For any particular lag l=n-m, we can look at the MGF of A(m,n) and S(m,n) as functions 
   * of theta.  If the system is stable (and we have enough data) then there will be some
   * theta \in (0,1) where the MGFs cross.  We want to pick a theta that is slightly to
   * the left of this crossing point.
   */
  def findIntersectionTheta(spark:SparkSession, interArrivalsN:Dataset[Row], servicesN:Dataset[Row], l:Long) : Double = {
    import spark.implicits._
    
    var l_theta = 1e-10;
    var r_theta = 1.0 - 1e-5;
    var theta = l_theta;
    val intersection_threshold = 0.0001;
    var ct = 0;
    val uta = interArrivalsN.withColumn("l1", sum($"interarrivalN").over(Window.orderBy("arrive").rowsBetween(0,l))).persist(MEMORY_AND_DISK);
    //uta.show()
    val uts = servicesN.withColumn("l1", sum($"sizeN").over(Window.orderBy("arrive").rowsBetween(0,l))).persist(MEMORY_AND_DISK);
    //uts.show()
    // TODO: should remove the first l rows
    
    var rhoa = Math.log( uta.withColumn("temp1", exp(col("l1").multiply(-theta))).agg(mean("temp1")).head().getDouble(0) ) / (-theta)
    var rhos = Math.log( uts.withColumn("temp1", exp(col("l1").multiply(theta))).agg(mean("temp1")).head().getDouble(0) ) / theta
    //println("rhoa="+rhoa+"\trhos="+rhos)
    
    while (Math.abs(rhoa-rhos) > intersection_threshold) {
        rhoa = Math.log( uta.withColumn("temp1", exp(col("l1").multiply(-theta))).agg(mean("temp1")).head().getDouble(0) ) / (-theta)
        rhos = Math.log( uts.withColumn("temp1", exp(col("l1").multiply(theta))).agg(mean("temp1")).head().getDouble(0) ) / theta
        //println("rhoa="+rhoa+"\trhos="+rhos)
        
        val mma = uta.withColumn("temp1", exp(col("l1").multiply(-theta))).agg(mean("temp1")).head().getDouble(0)
        val mms = uts.withColumn("temp1", exp(col("l1").multiply(theta))).agg(mean("temp1")).head().getDouble(0)
        //println("mma="+mma+"\tmms="+mms)
        
        if (mma == 0.0) {
            r_theta = theta;
        } else {
            if (rhos > rhoa) {
                r_theta = theta;
            } else {
                l_theta = theta;
            }
        }
        theta = Math.exp((Math.log(r_theta) + Math.log(l_theta))/2);
        ct = ct + 1;
        //println(ct+"\t"+theta+"\t"+(rhoa-rhos))
    }
    
    uta.unpersist();
    uts.unpersist();
    
    return theta;
  }

  
  /**
   * Take a 1D array of Double, and produce a 2D array with the original array
   * summed up over various lags.  This is to make computing the MGF faster.
   */
  def processLagsArray(data:Array[Double], max_l:Integer): Array[Array[Double]] = {
    val final_length = data.length - max_l;
    val a = Array.ofDim[Double](max_l, final_length);
    
    for ( i <- 0 until final_length ) {
        a(0)(i) = data(i);
    }
    for ( l <- 1 until max_l ) {
        for ( i <- 0 until final_length ) {
            a(l)(i) = a(l-1)(i) + data(i+l);
        }
    }
    
    return a;
  }

  
  
  
  
}