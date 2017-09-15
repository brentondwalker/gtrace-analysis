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
import breeze.linalg.{sum => bsum, DenseMatrix, DenseVector}
import breeze.stats.regression.leastSquares
//import breeze.numerics.{mean => bmean}

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
   * For a number of different thetas we analyze the MGF as a function of l=(n-m).
   * Is it linear?
   * In this one for each theta we take a sliding window over some fraction of
   * of the log-MGF function, do a least-squares regression, and note the slope.
   * Then we return the theta, with the biggest angle between the max and min slopes,
   * along with the max and min slopes themselves.
   */
  def logMgfLinearSpan(spark:SparkSession, data:IntertimeProcessData): Array[(Double,(Double,Double,Double))] = {
    val max_l = 2000;
    val theta_factor = 0.9
    var theta_sign = 1.0
    val num_thetas = 10;
    val fit_fraction = 0.25
    val num_fit_points = (fit_fraction * max_l).toInt
    
    if (data.is_arrival) {
      theta_sign = -1.0
    }
    
    val incr_mean = data.increments.reduce(_+_) / data.increments.length
    
    // we need a list of the different thetas to test.  We will normalize the
    // process to have mean 1.0.  Then We must have 0 < theta < 1.0.  The
    // interesting behavior sometimes happens when theta comes very close to
    // one of those limits.  We can choose our sequence of thetas on a log scale.
    val thetas = ListBuffer[Double]()
    thetas += 0.5
    for ( i <- 2 to num_thetas ) {
      thetas += Math.pow(theta_factor, i)
      thetas += 1.0 - Math.pow(0.5, i)
    }
    val thetardd = spark.sparkContext.parallelize(thetas.sorted, thetas.length);
    
    return thetardd.map( theta => (theta_sign*theta, logMgfLinearSpanRegression(data, theta_sign*theta, max_l, num_fit_points)) ).collect()
  }
  
  /**
   * given an array with the increments of a random process and a theta,
   * try a linear regression on the log-MGF as a function of l=(n-m).
   * This version does the regression repeatedly over a sliding window
   * of a fixed length and looks for the max and min slope estimates.
   * The third thing in the tuple is the angle between the slopes.
   */
  def logMgfLinearSpanRegression(data:IntertimeProcessData, theta:Double, max_l:Integer, num_fit_points:Integer): (Double,Double,Double) = {
    
    // first need to compute the MGFs for this theta
    val logMgf = data.computeMgf(max_l, theta)
                     .map(_(2))  // take the 3rd column
    val max_valid_l = logMgf.length;
    
    if (max_valid_l < (num_fit_points+10)) {
      println("WARNING: max_valid_l < (num_fit_points+10)")
      return (0.0, 0.0, 0.0)
    }
    
    // loop over the windows of the desired size
    // Doing this at every shift of the window is gratuitous, but we're
    // parallelizing it and the data is not very big, so whatever.
    val indep = DenseMatrix.tabulate(num_fit_points,2){ case(i,j) => if (j==0) 1.0 else i.toDouble }
    var min_b:Double = 1.0e99
    var max_b:Double = -1.0e99
    
    for (l_start <- 0 until (max_valid_l - num_fit_points)) {
      // try the regression
      val dep = DenseVector(logMgf.slice(l_start, (l_start+num_fit_points)));
      //println("indep = "+indep.rows+" x "+indep.cols)
      //println("dep = "+dep.length)
      val result = leastSquares(indep, dep)
      
      val b = result.coefficients.data(1)
      min_b = Math.min(min_b, b)
      max_b = Math.max(max_b, b)
    }
    
    // compute the angle between the max and min slopes
    val angle  = Math.atan(max_b) - Math.atan(min_b)
    
    return (min_b, max_b, angle)
  }
  
  
  /**
   * For a number of different thetas we analyze the MGF as a function of l=(n-m).
   * Is it linear?
   * In this one we fit the entire log-MGF using least-squares regression and look for
   * small r^2 values.
   */
  def logMgfLinearity(spark:SparkSession, data:IntertimeProcessData): Array[(Double,Double)] = {
    val max_l = 2000;
    val theta_factor = 0.9
    val num_thetas = 10;
    
    var theta_sign = 1.0
    var colname = "sizeN"
    if (data.is_arrival) {
      theta_sign = -1.0
    }
    
    val incr_mean = data.increments.reduce(_+_) / data.increments.length
    
    // we need a list of the different thetas to test.  We will normalize the
    // process to have mean 1.0.  Then We must have 0 < theta < 1.0.  The
    // interesting behavior sometimes happens when theta comes very close to
    // one of those limits.  We can choose our sequence of thetas on a log scale.
    val thetas = ListBuffer[Double]()
    thetas += 0.5
    for ( i <- 2 to num_thetas ) {
      thetas += Math.pow(theta_factor, i)
      thetas += 1.0 - Math.pow(0.5, i)
    }
    val thetardd = spark.sparkContext.parallelize(thetas.sorted, thetas.length);
    
    println("thetardd="+thetardd)
    thetardd.foreach(println)
    return thetardd.map( theta => (theta_sign*theta, logMgfRegression(data, theta_sign*theta, max_l)) ).collect()
  }
  
  
  /**
   * given an array with the increments of a random process and a theta,
   * try a linear regression on the log-MGF as a function of l=(n-m).
   * If the samples are GI, then the log-MGF will be linear. 
   */
  def logMgfRegression(data:IntertimeProcessData, theta:Double, max_l:Integer): Double = {
    
    // first need to compute the MGFs for this theta
    val logMgf = data.computeMgf(max_l, theta)
                     .map(_(2))  // take the 3rd column
    val max_valid_l = logMgf.length;

    if (max_valid_l <= 10) {
      println("WARNING: max_valid_l <= 10")
      return 0.0
    }
    
    // try the regression
    val indep = DenseMatrix.tabulate(max_valid_l,2){ case(i,j) => if (j==0) 1.0 else i.toDouble }
    val dep = DenseVector(logMgf)
    println("indep = "+indep.rows+" x "+indep.cols)
    println("dep = "+dep.length)
    val result = leastSquares(indep, dep)
    
    // computation of r^2 value in breeze is broken.
    // Do it manually
    val ymean = breeze.stats.mean(dep)
    val sstot = bsum( dep.map( y => breeze.numerics.pow(y-ymean,2) ) )
    val residuals = dep.map( x => -x ) + indep(::,0).map( x => x*result.coefficients.data(0) ) + indep(::,1).map( x => x*result.coefficients.data(1) )
    val ssres = bsum( residuals.map( e => breeze.numerics.pow(e,2)) )
    
    return (1.0 - ssres/sstot)
  }
  
  
  
  /**
   * compute a bunch of feasible envelope parameters for the given processes
   * normalized to the given rates.  These envelope parameters can be used
   * to compute the G|G fork-join bounds.
   */
  def computeEnvelope(spark:SparkSession, arrivals:IntertimeProcessData, services:IntertimeProcessData): RDD[ListBuffer[SigmaRho]] = {
    val num_thetas = 10;
    val max_l = 2000;
    
    val theta1 = findIntersectionTheta(arrivals, services, 1)
    val theta1k = findIntersectionTheta(arrivals, services, 1000)
    val theta2k = findIntersectionTheta(arrivals, services, 2000)
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
    
    // finally compute a bunch of feasible envelopes.  Each envelope consists of
    // (sigmaa:Double, rhoa:Double, sigmas:Double, rhos:Double, theta:Double, alpha:Double)
    // The alpha is actually computed from the other parts, but is convenient to have there.
    println("computing sigma-rho envelopes...")
    val srLists = thetardd.map( theta => fitSigmaRhoLinesInter(arrivals, services, theta, 2000)) //.reduce(_ ++ _)
    
    return srLists;
  }
  
  
  /**
   * given a theta and the lags array for an arrival and service process, compute a
   * bunch of feasible sigma/rho envelopes.
   * This is a new version of the function that works from the inter-arrival and service arrays.
   */
  def fitSigmaRhoLinesInter(arrivals:IntertimeProcessData, services:IntertimeProcessData, theta:Double, max_l:Integer): ListBuffer[SigmaRho] = {
    
    val numlines = 10
    
    val srList = ListBuffer[SigmaRho]()
    
    // first need to compute the MGFs for this theta
    val logMgfa = arrivals.computeMgf(max_l, -theta).map(_(2))
    val logMgfs = services.computeMgf(max_l, theta).map(_(2))
    println("logMgfa.length="+logMgfa.length)

    val max_valid_l = Math.min(logMgfa.length, logMgfs.length);
    
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
        for ( l <- 0 until logMgfs.length ) { sigmas = Math.max( sigmas, (logMgfs(l) - l*rhos) ) }
        // this is the adjustment for the service envelope being a function of (n-m+1)=(l+1)
        // Specifically, if we compute a line
        // M(l) = rhos*l + b
        // but the actual envelope is
        //        rhos*(l+1) + sigmas
        //      = rhos*l + (rhos + sigmas)
        // then we have b = rhos + sigmas
        // to get the real sigmas we need to subtract rhos
        // sigmas = b - rhos
        sigmas -= rhos
        
        for (ia <- 1 to numlines ) {
            var rhoa = min_rhoa + (max_rhoa - min_rhoa) * ((1.0*ia)/numlines)
            
            var sigmaa = -1.0e99
            for ( l <- 0 until logMgfa.length ) { sigmaa = Math.max( sigmaa, (l*rhos - logMgfs(l)) ) }
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
   * For any particular lag l=n-m, we can look at the MGF of A(m,n) and S(m,n) as functions 
   * of theta.  If the system is stable (and we have enough data) then there will be some
   * theta \in (0,1) where the MGFs cross.  We want to pick a theta that is slightly to
   * the left of this crossing point.
   */
  def findIntersectionTheta(arrivals:IntertimeProcessData, services:IntertimeProcessData, l:Long) : Double = {    
    //println("findIntersectionTheta("+l+")")
    
    var l_theta = 1e-10;
    var r_theta = 10.0 - 1e-5;
    var theta = l_theta;
    val intersection_threshold = 0.0001;
    val theta_progress_threshold = 1e-10;
    
    // this could be done faster by decomposing logarithmically
    val arrivals_l:Array[Double] = Array.fill[Double](arrivals.increments.length - l.toInt){0.0}
    val services_l:Array[Double] = Array.fill[Double](services.increments.length - l.toInt - 1){0.0}
    for ( i <- 0 until l.toInt ) {
      for (j <- 0 until arrivals_l.length ) {
        arrivals_l(j) += arrivals.increments(j+i)
      }
    }
    for ( i <- 0 to l.toInt ) {  // NOTE THE DIFFERENCE HERE!!!  "to"
      for (j <- 0 until services_l.length ) {
        services_l(j) += services.increments(j+i)
      }
    }
    
    var rhoa = Math.log( arrivals_l.map( x => Math.exp(-theta*x)).sum/arrivals_l.length ) / (-theta*l)
    var rhos = Math.log( services_l.map( x => Math.exp(theta*x)).sum/services_l.length ) / (theta*(l+1))
    //println("rhoa="+rhoa+"\trhos="+rhos)
    
    var ct = 0;
    var found_intersection = false
    var last_theta = theta
    while ((Math.abs(rhoa-rhos) > intersection_threshold) && (ct < 5000)) {
      val mma = arrivals_l.map( x => Math.exp(-theta*x)).sum/arrivals_l.length
      val mms = services_l.map( x => Math.exp(theta*x)).sum/services_l.length
      
      if (mma == 0.0) {
        println("  mma=0")
        r_theta = theta;
      } else if (mms.isInfinite()) {
        println("  mms=infty")
        r_theta = theta;
      } else {
        rhoa = Math.log( mma ) / (-theta*l);
        rhos = Math.log( mms ) / (theta*(l+1))
        
        if (rhos > rhoa) {
          println("  rhos > rhoa")
          r_theta = theta;
          found_intersection = true
        } else {
          println("  rhos <= rhoa")
          l_theta = theta;
        }
      }
      println(ct+"\t"+theta+"\t"+rhoa+"\t"+rhos+"\t"+(rhoa-rhos)+"\t\t"+l_theta+"\t"+r_theta+"\t"+(r_theta-l_theta))
      theta = Math.exp((Math.log(r_theta) + Math.log(l_theta))/2);
      ct += 1;

      // if we end up in iterations with theta not moving much, then we're not going to get the lines to cross.
      if (Math.abs(theta-last_theta) < theta_progress_threshold) {
          ct = 5000
      }
      last_theta = theta
    }
    if (ct > 4999) {
      println("WARNING: findIntersectionTheta("+l+") bailed out without getting close enough to the intersection.");
    }
    if (! found_intersection) {
      println("WARNING: findIntersectionTheta("+l+") bailed out without finding an intersection.");
    }
    
    return theta;
  }
  
  
  /**
   * For any particular lag l=n-m, we can look at the MGF of A(m,n) and S(m,n) as functions 
   * of theta.
   * 
   * If the system is stable (and we have enough data) then there will be some theta \in (0,1)
   * where rho_S and rho)A cross.  We want to pick a theta that is slightly to the left of
   * this crossing point.
   * 
   * The old version just looked for a crossing point in the log-MGFs.  That is more restrictive
   * than necessary.  The slope of the MGF as a function of l=(n-m) is rho_S and rho_A,
   * respectively.  We only really need to require that rho_S < rho_A, which means the
   * envelopes will cross.
   * 
   * In order to do this, instead of looking at the MGFs for a particular value of l, we need to
   * compute the MGF for a range of l values and then estimate the slope.
   */
  def findIntersectionThetaBySlope(arrivals:IntertimeProcessData, services:IntertimeProcessData, l:Long, l_window:Int) : Double = {    
    //println("findIntersectionThetaBySlope("+l+")")
    
    var l_theta = 1e-10;
    var r_theta = 10.0 - 1e-5;
    var theta = l_theta;
    val intersection_threshold = 0.0001;
    val theta_progress_threshold = 1e-10;
    
    
    var mgfa = arrivals.computeMgf(l.toInt, l.toInt+l_window, -theta).map(_(2))
    var mgfs = services.computeMgf(l.toInt, l.toInt+l_window, theta).map(_(2))
    
    // now use regression (??) to estimate the slope over the window
    var indepa = DenseMatrix.tabulate(mgfa.length,2){ case(i,j) => if (j==0) 1.0 else i.toDouble }  // are these both off by one?
    var depa = DenseVector(mgfa);
    var rhoa = leastSquares(indepa, depa).coefficients.data(1)

    var indeps = DenseMatrix.tabulate(mgfs.length,2){ case(i,j) => if (j==0) 1.0 else (i-1).toDouble }
    var deps = DenseVector(mgfs);
    var rhos = leastSquares(indeps, deps).coefficients.data(1)
    
    //var rhoa = Math.log( arrivals_l.map( x => Math.exp(-theta*x)).sum/arrivals_l.length ) / (-theta)
    //var rhos = Math.log( services_l.map( x => Math.exp(theta*x)).sum/services_l.length ) / theta
    //println("rhoa="+rhoa+"\trhos="+rhos)
    
    var ct = 0;
    var found_intersection = false
    var last_theta = theta
    while ((Math.abs(rhoa-rhos) > intersection_threshold) && (ct < 5000)) {
      mgfa = arrivals.computeMgf(l.toInt, l.toInt+l_window, -theta).map(_(2));
      mgfs = services.computeMgf(l.toInt, l.toInt+l_window, theta).map(_(2));
            
      if (mgfa.length < 10) {
        println("  mgfa.length < 10")
        r_theta = theta;
      } else if (mgfs.length < 10) {
        println("  mgfs.length < 10")
        r_theta = theta;
      } else {

        indepa = DenseMatrix.tabulate(mgfa.length,2){ case(i,j) => if (j==0) 1.0 else i.toDouble }
        depa = DenseVector(mgfa);
        rhoa = leastSquares(indepa, depa).coefficients.data(1);

        indeps = DenseMatrix.tabulate(mgfs.length,2){ case(i,j) => if (j==0) 1.0 else (i-1).toDouble }
        deps = DenseVector(mgfs);
        rhos = leastSquares(indeps, deps).coefficients.data(1)
        
        if (rhos > rhoa) {
          println("  rhos > rhoa")
          r_theta = theta;
          found_intersection = true
        } else {
          println("  rhos <= rhoa")
          l_theta = theta;
        }
      }
      println(ct+"\t"+theta+"\t"+rhoa+"\t"+rhos+"\t"+(rhoa-rhos)+"\t\t"+l_theta+"\t"+r_theta+"\t"+(r_theta-l_theta))
      theta = Math.exp((Math.log(r_theta) + Math.log(l_theta))/2);
      ct += 1;
      
      // if we end up in iterations with theta not moving much, then we're not going to get the lines to cross.
      if (Math.abs(theta-last_theta) < theta_progress_threshold) {
          ct = 5000
      }
      last_theta = theta
    }
    if (ct > 4999) {
      println("WARNING: findIntersectionTheta("+l+") bailed out without getting close enough to the intersection.");
    }
    if (! found_intersection) {
      println("WARNING: findIntersectionTheta("+l+") bailed out without finding an intersection.");
    }
    
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
   * This actually **CAN'T** be used in this project because there are dependency conflicts
   * between spark and breeze and sparkts.
   * 
   * When I import the sparkts jar file directly into the zeppelin notebook, however, everything works.
   */
  def computeArrivalAutocorr(spark:SparkSession, jobds:Dataset[Row], taskds:Dataset[Row], uname:String, lambda:Double, mu:Double) = {
    //print(uname.getString(0)+"\n")
    val arrivals = taskds.filter("username='"+uname+"'").select("arrive").sort("arrive").collect().map(x => x.getLong(0).toDouble)
    val interArrivals = (arrivals drop 1, arrivals).zipped.map(_-_)
    print(arrivals.length+"\t"+interArrivals.length+"\t"+interArrivals(10)+"\n")
    //var ac = UnivariateTimeSeries.autocorr(interArrivals, 100)
    //var ac = com.cloudera.sparkts.UnivariateTimeSeries.autocorr(interArrivals, 100)
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
      val arrivals = new IntertimeProcessData().loadArrivalData(spark, jobds, uname, 1.0)
      val services = new IntertimeProcessData().loadFirstTaskServiceData(spark, taskds, uname, 1.0)
      for ( mu <- mu_values ) {
        println("working on mu="+mu)
        services.renormalize(mu)
        val srlist = MgfAnalysis.computeEnvelope(spark, arrivals, services)
        val sra = srlist.collect()
        val mintau = sra.reduce(_++_).filter(! _.alpha.isInfinity ).map( sr => tauQuantiles(sr,eps,16) ).filter(_._2._1 > 0.0).reduce( (t1,t2) => if (t1._2._1 < t2._2._1) t1 else t2 )
        bound_results += ((mu, (mintau._2._1, mintau._2._2)))
        println(uname+"\t"+mu+"\t"+mintau._2._1+"\t"+mintau._2._2)
      }
      println("\n\n")
    }    
  }
  
  
  
}


/**
 * In the process of debugging I needed to compare to the old versions of some of these routines,
 * and in the process fixed some issues.  Even though this code is obsolete, I'm trying to
 * preserve the fixed versions.
 * 
 * The main thing this fixes is taking only the valid rows of A(m,n) and S(m,n).  Before it would
 * include the rows that were sums of less than l things.
 */
object OldMgfAnalysis {


  def findIntersectionThetaOld(spark:SparkSession, interArrivalsN:Dataset[Row], servicesN:Dataset[Row], l:Long) : Double = {
    import spark.implicits._
    
    println("findIntersectionTheta("+l+")")
    
    var l_theta = 1e-10;
    var r_theta = 1.0 - 1e-5;
    var theta = l_theta;
    val intersection_threshold = 0.0001;
    val theta_progress_threshold = 1e-10;
    val uta = interArrivalsN.withColumn("l1", sum($"interarrivalN").over(Window.orderBy("arrive").rowsBetween(0,l-1))).limit(interArrivalsN.count.toInt-l.toInt).persist(MEMORY_AND_DISK);
    uta.show()
    val uts = servicesN.withColumn("l1", sum($"sizeN").over(Window.orderBy("arrive").rowsBetween(0,l-1))).limit(servicesN.count.toInt-l.toInt).persist(MEMORY_AND_DISK);
    uts.show()
    // TODO: should remove the first l rows
    
    var rhoa = Math.log( uta.withColumn("temp1", exp(col("l1").multiply(-theta))).agg(mean("temp1")).head().getDouble(0) ) / (-theta)
    var rhos = Math.log( uts.withColumn("temp1", exp(col("l1").multiply(theta))).agg(mean("temp1")).head().getDouble(0) ) / theta
    //println("rhoa="+rhoa+"\trhos="+rhos)

    var ct = 0;
    var found_intersection = false
    var last_theta = theta
    while ((Math.abs(rhoa-rhos) > intersection_threshold) && (ct < 5000)) {
        
        val mma = uta.withColumn("temp1", exp(col("l1").multiply(-theta))).agg(mean("temp1")).head().getDouble(0)
        val mms = uts.withColumn("temp1", exp(col("l1").multiply(theta))).agg(mean("temp1")).head().getDouble(0)
        //println("mma="+mma+"\tmms="+mms)
        
        if (mma == 0.0) {
            r_theta = theta;
        } else if (mms.isInfinite()) {
            r_theta = theta;
        } else {
            rhoa = Math.log( uta.withColumn("temp1", exp(col("l1").multiply(-theta))).agg(mean("temp1")).head().getDouble(0) ) / (-theta)
            rhos = Math.log( uts.withColumn("temp1", exp(col("l1").multiply(theta))).agg(mean("temp1")).head().getDouble(0) ) / theta
            //println("rhoa="+rhoa+"\trhos="+rhos)
            
            if (rhos > rhoa) {
                r_theta = theta;
                found_intersection = true
            } else {
                l_theta = theta;
            }
        }
        println(ct+"\t"+theta+"\t"+rhoa+"\t"+rhos+"\t"+(rhoa-rhos)+"\t\t"+l_theta+"\t"+r_theta+"\t"+(r_theta-l_theta))
        theta = Math.exp((Math.log(r_theta) + Math.log(l_theta))/2);
        ct += 1;

        // if we end up in iterations with theta not moving much, then we're not going to get the lines to cross.
        if (Math.abs(theta-last_theta) < theta_progress_threshold) {
          ct = 5000
          println("BAILING OUT for no progress")
        }
        last_theta = theta
    }
    if (ct > 4999) {
      println("WARNING: findIntersectionTheta("+l+") bailed out without getting close enough to the intersection.");
    }
    if (! found_intersection) {
      println("WARNING: findIntersectionTheta("+l+") bailed out without finding an intersection.");
    }
    
    uta.unpersist();
    uts.unpersist();
    
    return theta;
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
    val interArrivalsN = interArrivals.withColumn("interarrivalN", col("interarrival")/(lambda*arrivalMean)).sort("arrive").persist(MEMORY_AND_DISK);
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
    val servicesN = services.withColumn("sizeN", col("size")/(mu*serviceMean)).sort("arrive").persist(MEMORY_AND_DISK);
    val newServiceMean = servicesN.agg(avg("sizeN")).head().getDouble(0)
    print("newServiceMean="+newServiceMean+"\n")
    servicesN.unpersist()
    
    return servicesN;
  }
  
  
}


