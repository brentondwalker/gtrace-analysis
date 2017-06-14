name		:= "gtrace-analysis"
version		:= "1.0"
organization	:= "ikt"
scalaVersion	:= "2.11.8"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.1"
libraryDependencies += "org.apache.spark" %% "spark-sql"  % "2.1.1"
resolvers	+= Resolver.mavenLocal
//fork in run := true

