package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.io.Source

object SparkSQLExample {

  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)
  println("SPARK SQL EXAMPLE");

  def main(args: Array[String]) {
    println("***************INSIDE MAIN");
    val spark = SparkSession
      .builder()
      .appName("CSE512-Phase2")
      .config("spark.some.config.option", "some-value").master("local[*]")
      .getOrCreate()

    paramsParser(spark, args)

    spark.stop()
  }

  private def paramsParser(spark: SparkSession, args: Array[String]): Unit =
  {
    println("***************PARAMS SETUP");
    // because the first argument is the file path
    var paramOffset = 1
    var currentQueryParams = ""
    var currentQueryName = ""
    var currentQueryIdx = -1

    println("ARGS LENGTH = ",args.length)
    println("ARGS1 = ",args(0))
    println("ARGS2 = ",args(1))

   // first parameter is the name of the output flie
   var elements = ""
    while (paramOffset <= args.length)
    {
        elements = args(paramOffset)
        if (paramOffset == args.length || elements.toLowerCase.contains("query"))
        {
          println(elements)
          // Turn in the previous query
          if (currentQueryIdx!= -1) {
            println ("Start processing....")
            queryLoader(spark, currentQueryName, currentQueryParams, args(0)+currentQueryIdx)
          }

          // Start a new query call
          if (paramOffset == args.length) {
            return
          }

          currentQueryName = args(paramOffset)
          currentQueryParams = ""
          currentQueryIdx = currentQueryIdx+1
          println("INDEX" + currentQueryIdx)
        }
        else
        {
          // Keep appending query parameters
          currentQueryParams = currentQueryParams + args(paramOffset) +" "
          println(currentQueryParams)
        }

      paramOffset = paramOffset+1
    }
  }

  private def queryLoader(spark: SparkSession, queryName:String, queryParams:String, outputPath: String): Unit =
  {

    println("queryName = " + queryName + "query params = " + queryParams + "output path = " + outputPath)
    var queryResult:Long = -1
    val queryParam = queryParams.split(" ")

    if (queryName.equalsIgnoreCase("RangeQuery"))
    {
      if(queryParam.length!=2) {
        throw new ArrayIndexOutOfBoundsException("[CSE512] Query "+queryName+" needs 2 parameters but you entered "+queryParam.length)
      }
      // print
      println("PARAM0 : " + queryParam(0) + "   PARAM1 : " + queryParam(1))
      queryResult = SpatialQuery.runRangeQuery(spark, queryParam(0), queryParam(1))
      println("RANGE QUERY RESULTS = " + queryResult)
    }
    else if (queryName.equalsIgnoreCase("RangeJoinQuery"))
    {
      if(queryParam.length!=2) {
        throw new ArrayIndexOutOfBoundsException("[CSE512] Query "+queryName+" needs 2 parameters but you entered "+queryParam.length)
      }
      queryResult = SpatialQuery.runRangeJoinQuery(spark, queryParam(0), queryParam(1))
    }
    else if (queryName.equalsIgnoreCase("DistanceQuery"))
    {
      if(queryParam.length!=3) {
        throw new ArrayIndexOutOfBoundsException("[CSE512] Query "+queryName+" needs 3 parameters but you entered "+queryParam.length)
      }
      queryResult = SpatialQuery.runDistanceQuery(spark, queryParam(0), queryParam(1), queryParam(2))
    }
    else if (queryName.equalsIgnoreCase("DistanceJoinQuery"))
    {
      if(queryParam.length!=3) {
        throw new ArrayIndexOutOfBoundsException("[CSE512] Query "+queryName+" needs 3 parameters but you entered "+queryParam.length)
      }
      queryResult = SpatialQuery.runDistanceJoinQuery(spark, queryParam(0), queryParam(1), queryParam(2))
    }
    else
    {
        throw new NoSuchElementException("[CSE512] The given query name "+queryName+" is wrong. Please check your input.")
    }

    import spark.implicits._
    val resultDf = Seq(queryName, queryResult.toString).toDF()
    resultDf.write.mode(SaveMode.Overwrite).csv(outputPath)
  }

}
