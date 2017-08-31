import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, LongType, DoubleType, TimestampType}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._ // for `when`
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.sql.expressions.Window
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._


/**
 * This will contain code to compute and analyze moment generating
 * functions of arrival and service process data.
 */

object MgfAnalysis {
  
  case class SigmaRho(sigmaa:Double, rhoa:Double, sigmas:Double, rhos:Double, theta:Double, alpha:Double)
  
  /**
   * returns an array containing (username,numjobs) tuples, sorted by numjobs
   */
  def computeTopUsers(jobds:Dataset[Row], threshold:Long): Array[(String,Long)] = {
    val userJobCounts = jobds.groupBy("username").count().sort(desc("count"));
    return userJobCounts.filter("count > "+threshold).collect().map( x => (x.getString(0), x.getLong(1)) )
  }
  
  
  /**
   * compute a bunch of feasible envelope parameters for the given processes
   * normalized to the given rates.  These envelope parameters can be used
   * to compute the G|G fork-join bounds.
   */
  def computeEnvelope(spark:SparkSession, jobds:Dataset[Row], taskds:Dataset[Row], uname:String, lambda:Double, mu:Double): RDD[ListBuffer[SigmaRho]] = {
    val num_thetas = 10;
    val max_l = 2000;
    val arrivals = getNormalizedArrivals(spark, jobds, uname, lambda).persist(MEMORY_AND_DISK);
    val services = getNormalizedServices(spark, taskds, uname, mu).persist(MEMORY_AND_DISK);
    
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
    println("computing arrival lags arrays...")
    //val lags_array_arrival = processLagsArray(arrivals.select("interarrivalN").collect().map(r => r.getDouble(0)), max_l)
    //val cum_array_arrival = computeCumulativeArray(arrivals.select("interarrivalN").collect().map(r => r.getDouble(0)))
    val arrival_increments = arrivals.select("interarrivalN").collect().map(r => r.getDouble(0))
    println("computing service lags arrays...")
    //val lags_array_service = processLagsArray(services.select("sizeN").collect().map(r => r.getDouble(0)), max_l)
    //val cum_array_service = computeCumulativeArray(services.select("sizeN").collect().map(r => r.getDouble(0)))
    val service_increments = services.select("sizeN").collect().map(r => r.getDouble(0))

    // don't need these datasets in memory anymore
    arrivals.unpersist()
    services.unpersist()
    
    // finally compute a bunch of feasible envelopes.  Each envelope consists of
    // (sigmaa:Double, rhoa:Double, sigmas:Double, rhos:Double, theta:Double, alpha:Double)
    // The alpha is actually computed from the other parts, but is convenient to have there.
    println("computing sigma-rho envelopes...")
    //val srLists = thetardd.map( theta => fitSigmaRhoLines(lags_array_arrival, lags_array_service, theta)) //.reduce(_ ++ _)  // XXX this reduce may be causing problems
    val srLists = thetardd.map( theta => fitSigmaRhoLinesInter(arrival_increments, service_increments, theta, 2000)) //.reduce(_ ++ _)
    //var srLists = ListBuffer[SigmaRho]()
    //for ( theta <- theta_list ) {
      //println("\nworking on theta="+theta)
      //srLists ++= fitSigmaRhoLines(lags_array_arrival, lags_array_service, theta)
    //}
    
    
    return srLists;
  }
  
  
  /**
   * given a theta and the lags array for an arrival and service process, compute a
   * bunch of feasible sigma/rho envelopes.
   */
  def fitSigmaRhoLines(lags_array_arrival:Array[Array[Double]], lags_array_service:Array[Array[Double]], theta:Double): ListBuffer[SigmaRho] = {
    val max_l = Math.min(lags_array_arrival.length, lags_array_service.length)
    println("max_l="+max_l)

    val numlines = 10
    
    val srList = ListBuffer[SigmaRho]()
    
    // first need to compute the MGFs for this theta
    val logMgfa = Array.fill(max_l){0.0}
    val logMgfs = Array.fill(max_l){0.0}
    println("logMgfa.length="+logMgfa.length)

    var max_valid_l = 0;

    breakable {
    for ( l <- 0 until max_l ) {
        logMgfa(l) = Math.log( lags_array_arrival(l).map( x => Math.exp(theta*x)).sum/lags_array_arrival(l).length ) / theta
        logMgfs(l) = Math.log( lags_array_service(l).map( x => Math.exp(theta*x)).sum/lags_array_service(l).length ) / theta
        if (logMgfa(l).isInfinity || logMgfs(l).isInfinity) {
            max_valid_l = l-1
            break
        }
    }}
    println("max_valid_l="+max_valid_l)
    if (max_valid_l <= 0) {
      println("WARNING: max_valid_l <= 0")
      return srList
    }
    
    // try fitting lines
    // first estimate the max and min slopes, and then compute the envelopes for several slopes in between
    var min_rhoa = 1.0e99;
    var max_rhoa = 0.0;
    var min_rhos = 1.0e99;
    var max_rhos = 0.0;
    
    // to keep from computing the slope of noise, compute slope over a widow of 1/10 of the data
    val lwin = Math.round(max_valid_l/10)
    if (max_valid_l <= lwin) {
      println("WARNING: max_valid_l <= lwin")
      return srList
    }
    for ( l <- 0 until (max_valid_l-lwin)) {
        var temp_rhoa = (logMgfa(l+lwin) - logMgfa(l)) / (lwin)
        min_rhoa = Math.min(temp_rhoa, min_rhoa)
        max_rhoa = Math.max(temp_rhoa, max_rhoa)
        
        var temp_rhos = (logMgfs(l+lwin) - logMgfs(l)) / (lwin)
        min_rhos = Math.min(temp_rhos, min_rhos)
        max_rhos = Math.max(temp_rhos, max_rhos)
    }
    println("min_rhoa="+min_rhoa+"\tmax_rhoa="+max_rhoa+"\tmin_rhos="+min_rhos+"\tmax_rhos="+max_rhos)
    
    // also require that the minimum rhoa is greater or equal to the minimum rhos
    // i.e. there is no reason to consider infeasible solutions
    min_rhoa = Math.max(min_rhoa, min_rhos)
    
    if ((min_rhoa > max_rhoa) || (min_rhos > max_rhos)) {
      println("WARNING: no feasible solutions here")
      return srList
    }
    
    for ( is <- 0 until numlines ) {
        var rhos = min_rhos + (max_rhos - min_rhos) * ((1.0*is)/numlines)
        
        var sigmas = -1.0e99
        for ( l <- 0 until logMgfs.length ) { sigmas = Math.max( sigmas, (logMgfs(l) - l*rhos)) }
        
        for (ia <- 1 to numlines ) {
            var rhoa = min_rhoa + (max_rhoa - min_rhoa) * ((1.0*ia)/numlines)
            
            var sigmaa = -1.0e99
            for ( l <- 0 until logMgfa.length ) { sigmaa = Math.max( sigmaa, (l*rhos - logMgfs(l))) }
            var alpha = Math.exp(theta*(sigmaa+sigmas)) / ( 1 - Math.exp(-theta*(rhoa-rhos)) );
            srList += SigmaRho(sigmaa, rhoa, sigmas, min_rhos, theta, alpha)
        }
    }
    
    return srList;
  }
  
  
  /**
   * given a theta and the lags array for an arrival and service process, compute a
   * bunch of feasible sigma/rho envelopes.
   * This is a new version of the function that works from the inter-arrival and service arrays.
   */
  def fitSigmaRhoLinesInter(arrivals:Array[Double], services:Array[Double], theta:Double, max_l:Integer): ListBuffer[SigmaRho] = {
    //val max_l = Math.min(lags_array_arrival.length, lags_array_service.length)
    //println("max_l="+max_l)

    val numlines = 10
    
    val srList = ListBuffer[SigmaRho]()
    
    // first need to compute the MGFs for this theta
    val logMgfa = Array.fill(max_l){0.0}
    val logMgfs = Array.fill(max_l){0.0}
    println("logMgfa.length="+logMgfa.length)

    var max_valid_l = 0;
    
    val arrivals_mn = Array.fill[Double](arrivals.length - max_l)(0.0)
    val services_mn = Array.fill[Double](services.length - max_l)(0.0)
    breakable {
    for ( l <- 0 until max_l ) {
      for ( i <- 0 until arrivals_mn.length ) { arrivals_mn(i) += arrivals(i+l) }
      for ( i <- 0 until services_mn.length ) { services_mn(i) += services(i+l) }
      logMgfa(l) = Math.log( arrivals_mn.map( x => Math.exp(theta*x)).sum/arrivals_mn.length ) / theta;
      logMgfs(l) = Math.log( services_mn.map( x => Math.exp(theta*x)).sum/services_mn.length ) / theta;
      if (logMgfa(l).isInfinity || logMgfs(l).isInfinity) {
          max_valid_l = l-1;
          break
      }
    }}
    println("max_valid_l="+max_valid_l)
    if (max_valid_l <= 0) {
      println("WARNING: max_valid_l <= 0")
      return srList
    }
    
    // try fitting lines
    // first estimate the max and min slopes, and then compute the envelopes for several slopes in between
    var min_rhoa = 1.0e99;
    var max_rhoa = 0.0;
    var min_rhos = 1.0e99;
    var max_rhos = 0.0;
    
    // to keep from computing the slope of noise, compute slope over a widow of 1/10 of the data
    val lwin = Math.round(max_valid_l/10)
    if (max_valid_l <= lwin) {
      println("WARNING: max_valid_l <= lwin")
      return srList
    }
    for ( l <- 0 until (max_valid_l-lwin)) {
        var temp_rhoa = (logMgfa(l+lwin) - logMgfa(l)) / (lwin)
        min_rhoa = Math.min(temp_rhoa, min_rhoa)
        max_rhoa = Math.max(temp_rhoa, max_rhoa)
        
        var temp_rhos = (logMgfs(l+lwin) - logMgfs(l)) / (lwin)
        min_rhos = Math.min(temp_rhos, min_rhos)
        max_rhos = Math.max(temp_rhos, max_rhos)
    }
    println("min_rhoa="+min_rhoa+"\tmax_rhoa="+max_rhoa+"\tmin_rhos="+min_rhos+"\tmax_rhos="+max_rhos)
    
    // also require that the minimum rhoa is greater or equal to the minimum rhos
    // i.e. there is no reason to consider infeasible solutions
    min_rhoa = Math.max(min_rhoa, min_rhos)
    
    if ((min_rhoa > max_rhoa) || (min_rhos > max_rhos)) {
      println("WARNING: no feasible solutions here")
      return srList
    }
    
    for ( is <- 0 until numlines ) {
        var rhos = min_rhos + (max_rhos - min_rhos) * ((1.0*is)/numlines)
        
        var sigmas = -1.0e99
        for ( l <- 0 until logMgfs.length ) { sigmas = Math.max( sigmas, (logMgfs(l) - l*rhos)) }
        
        for (ia <- 1 to numlines ) {
            var rhoa = min_rhoa + (max_rhoa - min_rhoa) * ((1.0*ia)/numlines)
            
            var sigmaa = -1.0e99
            for ( l <- 0 until logMgfa.length ) { sigmaa = Math.max( sigmaa, (l*rhos - logMgfs(l))) }
            var alpha = Math.exp(theta*(sigmaa+sigmas)) / ( 1 - Math.exp(-theta*(rhoa-rhos)) );
            srList += SigmaRho(sigmaa, rhoa, sigmas, min_rhos, theta, alpha)
        }
    }
    
    return srList;
  }
  
  
  /**
   * given a sigma-rho envelope, an epsilon, and the parallelism, k, compute
   * the sojourn time bounds in the G|G case for both 1 and k stages.
   */
  def tauQuantiles(sr:SigmaRho, eps:Double, k:Integer): (SigmaRho,(Double,Double)) = {
    if (sr.rhoa > sr.rhos) {
      val quantile_bound = (Math.log(eps) - Math.log(sr.alpha) - sr.theta*sr.rhos)/(-sr.theta);
      val quantile_bound_k = (Math.log(eps) - Math.log(sr.alpha) - Math.log(k.toDouble) - sr.theta*sr.rhos)/(-sr.theta);
      return (sr, (quantile_bound, quantile_bound_k))
    }
    return (sr, (0.0, 0.0))
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
    interArrivalsN.unpersist()
    
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
    servicesN.unpersist()
    
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
  
  
  /**
   * Main routine
   */
  def main(args: Array[String]) {
    var eps = 1.0e-6
    
    if (args.length < 2) {
      println("usage: loadFile <task_file(s)> <job_trace_file(s)>")
      System.exit(0)
    }
    val task_infile = args(0)
    val job_infile = args(1)
    
    val spark = utils.createSparkSession("MgfAnalsis")
    import spark.implicits._
    
    val task_event_ds = gtraceReader.readTaskEvents(spark, task_infile) //.persist(MEMORY_AND_DISK);
    val taskds = EventDataTransformer.transformTaskData(spark, task_event_ds, true).persist(MEMORY_AND_DISK);
    
    val job_event_ds = gtraceReader.readJobEvents(spark, job_infile) //.persist(MEMORY_AND_DISK);
    val jobds = EventDataTransformer.transformJobData(spark, job_event_ds, true).filter("fail==0").persist(MEMORY_AND_DISK);
    
    val topUsers = MgfAnalysis.computeTopUsers(jobds, 10000)
    
    val bound_results = ListBuffer[(Double,(Double,Double))]()
    val mu_values = List(1.2, 1.4, 1.6, 2.0, 3.0, 4.0, 5.0, 6.0)

    for ( uname <- topUsers.map(x=>x._1) ) {
      for ( mu <- mu_values ) {
        println("working on mu="+mu)
        val srlist = MgfAnalysis.computeEnvelope(spark, jobds, taskds, topUsers(0)._1, 1.0, mu)
        val sra = srlist.collect()
        val mintau = sra.reduce(_++_).filter(! _.alpha.isInfinity ).map( sr => tauQuantiles(sr,eps,16) ).filter(_._2._1 > 0.0).reduce( (t1,t2) => if (t1._2._1 < t2._2._1) t1 else t2 )
        bound_results += ((mu, (mintau._2._1, mintau._2._2)))
        println(uname+"\t"+mu+"\t"+mintau._2._1+"\t"+mintau._2._2)
      }
      println("\n\n")
    }    
  }
  
  
  
}