package com.nielsenmedia.timbers
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.types.{StringType, StructField, StructType, _}

object MTimberExpress {

  val spark = SparkSession
    .builder()
    .appName(s"MTimberExpress")
    .config("spark.sql.crossJoin.enabled", "true")
    .config("spark.sql.parquet.binaryAsString", "true")
    .config("spark.sql.caseSensitive", "false")
    .getOrCreate()

  val dateOfLoad = "2019-01-10"
  val APPS_STATS_LOCATION = "s3://us-east-1-nlsn-w-tam-ltam-reporting-lds-prod/lds/appstats"
  val LOGS_LOCATION = s"s3://us-east-1-nlsn-w-tam-ltam-reporting-lds-prod/logs/p001/${dateOfLoad}"
  val OUTPUT_LOCATION = s"s3://us-east-1-nlsn-w-tam-ltam-reporting-lds-prod/logs/mtimber"

  val yarnapps = spark.read.json(s"$APPS_STATS_LOCATION/${dateOfLoad}/appstats_p001_eod_${dateOfLoad}.json")
  yarnapps.createOrReplaceTempView("yarnapps")
  val yarnAppsTimed = spark.sql(" select name, id, finalStatus, finishedTime, CAST(finishedTime / 1000 AS timestamp) as finishTime,startedTime, CAST(startedTime / 1000 AS timestamp) as  startTime, finishedTime-startedTime as timetaken_in_millisec, CAST((finishedTime-startedTime)/60000 as int) as ttmins,CAST((finishedTime-startedTime)/1000 - CAST((finishedTime-startedTime)/60000 as int)*60 as int) as ttsec, memorySeconds, vcoreSeconds from yarnapps")
  yarnAppsTimed.createOrReplaceTempView("yarnAppsTimed")
  val yarnAppsTimedDisplay= spark.sql("select name, id, concat(CAST(ttmins as string), ' mins ', CAST(ttsec as string), ' sec') as ttminsec,startTime, finishTime,  memorySeconds, vcoreSeconds from yarnAppsTimed")

  //  ${jobType}|${jobId}|${dmaCode}|${sscm}|${jobDate}|${clusterId}|${appId}|${container}|${mem_matches}|${sequence}
//  DXG|9878727|514|73|2019-01-09|1547158091068|0001|1547158091068_0001_01_000001|8041|ip-10-236-125-137|9/01/10|22:26:30|Added|broadcast_5_piece0|ip-10-236-122-140.aws-w.nielsencsp.net|36381||40.5|KB|2.1|GB|131496
//  ${jobType}|${jobId}|${dmaCode}|${sscm}|${jobDate}|${clusterId}|${appId}|${container}
// |${logdate}|${logtime}|${added_removed}|${broadcast_rdd}|${node}|${port}|${values_bytes}|${size}|${size_bytes}|${free}|${free_bytes}|${sequence}
  val storageColList = List(("jobType", StringType), ("jobId", StringType), ("dmaCode", StringType), ("sscm", StringType), ("jobDate", StringType), ("clusterId", StringType), ("appId", StringType), ("container", StringType)
  , ("someport", StringType), ("somehost", StringType)
  , ("logdate", StringType), ("logtime", StringType), ("added_removed", StringType), ("broadcast_rdd", StringType), ("node", StringType), ("port", StringType), ("values_bytes", StringType), ("size", StringType), ("size_bytes", StringType), ("free", StringType), ("free_bytes", StringType), ("seq", StringType))
  val storageSchema = StructType(storageColList.map(x => StructField(x._1, x._2, false)))
  val spark_apps_usage = spark.read.option("header", "false").option("delimiter", "|").schema(storageSchema).csv(s"$OUTPUT_LOCATION/${dateOfLoad}/dxg/storage2")
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.types._

  //spark_apps_usage.select($"appId", $"seq", $"jobType", $"sscm", $"dmaCode", unix_timestamp(concat($"logdate", lit(" "), $"logTime")).cast(TimestampType).as("timestamp"),$"node", $"port", $"broadcast_rdd", concat($"size", lit(" "), $"size_bytes"), concat($"free", lit(" "), $"free_bytes") ).where("added_removed = 'Added'").orderBy("appId","node", "port", "seq").show
  spark_apps_usage.select("appId", "seq", "jobType", "sscm", "dmaCode")//, unix_timestamp(concat("logdate", lit(" "), "logTime")).cast(TimestampType).as("timestamp"),"node", "port", "broadcast_rdd", concat("size", lit(" "), "size_bytes"), concat("free", lit(" "), "free_bytes") ).where("added_removed = 'Added'").orderBy("appId","node", "port", "seq").show

  val apps1 = spark_apps_usage
    .withColumn("log_time", functions.concat(lit("201"), spark_apps_usage.col("logdate"), lit(" "), spark_apps_usage.col("logTime")))
    .withColumn("allocated_size", functions.concat(spark_apps_usage.col("size"), lit(" "), spark_apps_usage.col("size_bytes")))
    .withColumn("free_size", functions.concat(spark_apps_usage.col("free"), lit(" "), spark_apps_usage.col("free_bytes")))
    .withColumn("sized", spark_apps_usage.col("size").cast("double"))
    .withColumn("freed", spark_apps_usage.col("free").cast("double"))

  val apps2 = apps1.withColumn("logGenTime" , unix_timestamp(apps1.col("log_time"), "yyyy/MM/dd HH:mm:ss"))
  apps2.createOrReplaceTempView("apps2")

  val apps3 = spark.sql("select *, case size_bytes when 'KB' then sized * 1024 when 'MB' then sized*1024*1024 else sized end as size_in_bytes, case free_bytes when 'MB' then freed * 1024 when 'GB' then freed*1024*1024 else freed end as free_in_kilo_bytes from apps2")
  apps3.createOrReplaceTempView("apps3")
  val apps_anal1 = spark.sql("select container, node, port, jobType, appId, log_time, sum(size_in_bytes), sum(free_in_kilo_bytes) from apps3 group by container, node, port, jobType, appId, log_time")

  val apps_anal2=spark.sql("select jobType, container, count(distinct node, port) as nodes from apps3 group by jobType, container")
  apps_anal2.createOrReplaceTempView("apps_anal2")

  val apps_anal3=spark.sql("select jobType, sscm, dmaCode, count(distinct container) as containers from apps3 group by jobType, sscm, dmaCode")
  apps_anal3.createOrReplaceTempView("apps_anal3")

  val apps_anal4=spark.sql("select jobType, sscm, dmaCode, count(distinct node, port) as containers from apps3 group by jobType, sscm, dmaCode")
  apps_anal4.createOrReplaceTempView("apps_anal4")

  spark.sql("select jobType, max(containers) from apps_anal4 group by jobType").show
  spark.sql("select jobType, dmaCode, sscm from apps_anal4 where containers=45").show
  spark.sql("select jobType, dmaCode, node, port, logTime, sum(size_in_bytes)/(1024*1024) tot_size_inMB, sum(free_in_kilo_bytes)/(1024*1024) tot_free_inGBs from apps3 group by jobType,dmaCode, node, port, logTime").orderBy(desc("tot_size_inMB"))


  val clusterId = spark_apps_usage.select("clusterId").distinct.collect.mkString
  println(s"Yarn - Spark applications container wise timed memory allocation on cluster $clusterId")


  ///us-east-1-nlsn-w-tam-ltam-reporting-lds-prod/lds/appstats/2019-01-10


  val dataExtractionColList = List(("coll", StringType), ("DMAtext", StringType), ("DMACode", StringType), ("MeterCollDate", StringType), ("StartTime", StringType), ("EndTime", StringType), ("timeTaken", IntegerType))
  val dataExtractionSchema = StructType(dataExtractionColList.map(x => StructField(x._1, x._2, false)))
  val dataExtractionTiming = spark.read.option("header", "false").option("delimiter", ",").schema(dataExtractionSchema).csv(s"$APPS_STATS_LOCATION/${dateOfLoad}/dataextractions_run_times.csv")
  dataExtractionTiming.createOrReplaceTempView("dataExtractionTiming")
val grabixByDMAColList = List(("coll", StringType), ("DMAtext", StringType), ("DMACode", StringType), ("MeterCollDate", StringType), ("StartTime", StringType), ("EndTime", StringType), ("timeTaken", IntegerType))
  val grabixByDMASchema = StructType(grabixByDMAColList.map(x => StructField(x._1, x._2, false)))
  val grabixByDMATiming = spark.read.option("header", "false").option("delimiter", ",").schema(grabixByDMASchema).csv(s"$APPS_STATS_LOCATION/${dateOfLoad}/dmas_run_times.csv")
  grabixByDMATiming.createOrReplaceTempView("grabixByDMATiming")
val grabixByStationColList = List(("coll", StringType), ("DMAtext", StringType), ("DMACode", StringType), ("MeterCollDate", StringType), ("Station", StringType), ("StartTime", StringType), ("EndTime", StringType), ("timeTaken", IntegerType))
  val grabixByStationSchema = StructType(grabixByStationColList.map(x => StructField(x._1, x._2, false)))
  val grabixByStationTiming = spark.read.option("header", "false").option("delimiter", ",").schema(grabixByStationSchema).csv(s"$APPS_STATS_LOCATION/${dateOfLoad}/stations_run_times.csv")
  grabixByStationTiming.createOrReplaceTempView("grabixByStationTiming")
  val grabixStationByDMATiming = spark.sql("select DMACode, sum(timeTaken) as dmaTimeTaken, count(1) as numberOfStations from grabixByStationTiming group by DMACode")
  grabixStationByDMATiming.createOrReplaceTempView("grabixStationByDMATiming")
  val timetaken = spark.sql("select de.DMACode , de.timeTaken as dataExtractionTimeTaken, gd.timeTaken as grabixByDMATimeTaken, gs.numberOfStations, gs.dmaTimeTaken as dmaStationTimeTaken from dataExtractionTiming de, grabixByDMATiming gd, grabixStationByDMATiming gs where de.DMACode=gd.DMACode and gd.DMACode=gs.DMACode order by de.DMACode")

}


