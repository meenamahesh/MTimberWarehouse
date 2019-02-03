package com.nielsenmedia.timbers

import org.apache.spark.sql.SparkSession

object MTimberDepot extends App{

  val dateOfLoad = "2019-01-23" //args(0)
  val LOGS_LOCATION = s"s3://us-east-1-nlsn-w-tam-ltam-reporting-lds-prod/lds/logs/${dateOfLoad}"
  val OUTPUT_LOCATION = s"s3://us-east-1-nlsn-w-tam-ltam-reporting-lds-prod/logs/mtimber"
//  val LOGS_LOCATION = "s3://us-east-1-nlsn-w-tam-ltam-reporting-lds-prod/lds/logs/2019-01-15/logs/dxg/sent/DXG_9888784_514_73_2019-01-15_application_1547672528559_0002.log"

  val spark = SparkSession
    .builder()
    .appName(s"MTimberDepot")
    .config("spark.sql.crossJoin.enabled", "true")
    .config("spark.sql.parquet.binaryAsString", "true")
    .config("spark.sql.caseSensitive", "false")
    .getOrCreate()
//  val log

  val dxg_logs = spark.sparkContext.wholeTextFiles(s"${LOGS_LOCATION}/dxg/")
  val dxg_timber_est = dxg_logs.map(x=> logExtraction(x, "estimate", ""))
  if(!dxg_timber_est.isEmpty)dxg_timber_est.saveAsTextFile(s"$OUTPUT_LOCATION/${dateOfLoad}/dxg/estimate2")
  val dxg_timber_sto = dxg_logs.map(x=> logExtraction(x, "storage", ""))
  if(!dxg_timber_sto.isEmpty())dxg_timber_sto.saveAsTextFile(s"$OUTPUT_LOCATION/${dateOfLoad}/dxg/storage2")

  val sc86_logs = spark.sparkContext.wholeTextFiles(s"${LOGS_LOCATION}/sc/86/")
  val sc_86_timber_est = sc86_logs.map(x=> logExtraction(x, "estimate", "86"))
  if(!sc_86_timber_est.isEmpty)sc_86_timber_est.saveAsTextFile(s"$OUTPUT_LOCATION/${dateOfLoad}/sc/86/estimate2")
  val sc_86_timber_sto = sc86_logs.map(x=> logExtraction(x, "storage", "86"))
  if(!sc_86_timber_sto.isEmpty)sc_86_timber_sto.saveAsTextFile(s"$OUTPUT_LOCATION/${dateOfLoad}/sc/86/storage2")

  val sc87_logs = spark.sparkContext.wholeTextFiles(s"${LOGS_LOCATION}/sc/87/")
  val sc_87_timber_est = sc87_logs.map(x=> logExtraction(x, "estimate", "87"))
  if(!sc_87_timber_est.isEmpty)sc_87_timber_est.saveAsTextFile(s"$OUTPUT_LOCATION/${dateOfLoad}/sc/87/estimate2")
  val sc_87_timber_sto = sc87_logs.map(x=> logExtraction(x, "storage", "87"))
  if(!sc_87_timber_sto.isEmpty) sc_87_timber_sto.saveAsTextFile(s"$OUTPUT_LOCATION/${dateOfLoad}/sc/87/storage2")

  def logExtraction(x :(String, String), storage:String, sc_cad:String) : String = {
    import io.krakens.grok.api.{Grok, GrokCompiler}
    import org.apache.commons.io.FilenameUtils
    import scala.collection.JavaConverters._
    import java.text.SimpleDateFormat
    import java.util.Calendar

    val scCadMapping = Map("86"->"Daily", "87" -> "NSI Weekly", "88" -> "Cal Weekly", "89" -> "NSI Monthly", "90" -> "Cal Monthly")
    val grokcomp = GrokCompiler.newInstance()
    grokcomp.registerDefaultPatterns()

    //"Container: container_1546639822896_0125_01_000012 on ip-10-236-120-123.aws-w.nielsencsp.net_8041"
    val grokcont: Grok = grokcomp.compile("Container: container_%{GREEDYDATA:container} on %{GREEDYDATA:ip}.aws-w.nielsencsp.net_%{NUMBER:port}")

    //   19/01/16 21:41:42 INFO memory.MemoryStore: Block broadcast_1227_piece0 stored as bytes in memory (estimated size 5.4 KB, free 2.0 GB)
    //   19/01/16 21:41:56 INFO memory.MemoryStore: Block rdd_3641_5 stored as values in memory (estimated size 762.7 KB, free 2.0 GB)
    val grokestimate: Grok = grokcomp.compile("%{DATE_US:logdate} %{TIME:logtime} INFO memory.MemoryStore: Block %{WORD:broadcast_rdd} stored as %{WORD:values_bytes} in memory \\(estimated size %{NUMBER:size} %{WORD:size_bytes}, free %{NUMBER:free} %{WORD:free_bytes}\\)")
//    val grokestimate: Grok = grokcomp.compile("%{DATE_US:logdate} %{TIME:logtime} INFO memory.MemoryStore: %{WORD:added_removed} %{WORD:broadcast_rdd} stored as %{WORD:values_bytes_node} in %{WORD:port} \\(estimated size %{NUMBER:size} %{WORD:size_bytes}, free %{NUMBER:free} %{WORD:free_bytes}\\)")

    //   19/01/16 21:41:42 INFO storage.BlockManagerInfo: Added broadcast_1227_piece0 in memory on ip-10-236-126-8.aws-w.nielsencsp.net:44593 (size: 5.4 KB, free: 6.9 GB)
    //   19/01/16 21:41:52 INFO storage.BlockManagerInfo: Removed broadcast_1227_piece0 on ip-10-236-125-58.aws-w.nielsencsp.net:37577 in memory (size: 5.4 KB, free: 2.1 GB)
    val grokstore_add: Grok = grokcomp.compile("%{DATE_US:logdate} %{TIME:logtime} INFO storage.BlockManagerInfo: %{WORD:added_removed} %{WORD:broadcast_rdd} in memory on %{GREEDYDATA:node}:%{NUMBER:port} \\(size: %{NUMBER:size} %{WORD:size_bytes}, free: %{NUMBER:free} %{WORD:free_bytes}\\)")
    val grokstore_remove: Grok = grokcomp.compile("%{DATE_US:logdate} %{TIME:logtime} INFO storage.BlockManagerInfo: %{WORD:added_removed} %{WORD:broadcast_rdd} on %{GREEDYDATA:node}:%{NUMBER:port} in memory \\(size: %{NUMBER:size} %{WORD:size_bytes}, free: %{NUMBER:free} %{WORD:free_bytes}\\)")

    val filename = FilenameUtils.getBaseName(x._1)
    //val filename = "ScRef_9887786_application_1547672528559_0001.log" //"SC_9887784_810_71_2019-01-15_2019-01-15_application_1547672528559_0093.log"//"DXG_9888853_602_3_2019-01-15_application_1547672528559_0131.log"
    val fileSect = filename.replace(".log","").split("_")
    val format = new SimpleDateFormat("y-M-d")
    val scDate=format.format(Calendar.getInstance().getTime())

    //DXG_9888853_602_3_2019-01-15_application_1547672528559_0131.log
    //job_type jobId dma sscm date applicationId
    //ScRef_9887786_application_1547672528559_0001.log
    //SC_9887784_810_71_2019-01-15_2019-01-15_application_1547672528559_0093.log
    val (jobType, jobId, dmaCode, sscm, jobDate, clusterId, appId) = fileSect(0) match {
      case "DXG" => {
//        println("data extractor with grabix")
        (fileSect(0), fileSect(1), fileSect(2), fileSect(3), fileSect(4), fileSect(6), fileSect(7))
      }
      case "SC" => {
//        println("")
        ("SC".concat(scCadMapping(sc_cad)), fileSect(1), fileSect(2), fileSect(3), fileSect(5), fileSect(7), fileSect(8))
      }
      case "ScRef" => {
//        println("SCRef to NLTV")
        (fileSect(0), fileSect(1), "", "", scDate, fileSect(3), fileSect(4))
      }
    }

    import scala.collection.mutable.ListBuffer
    var timberestimate = new ListBuffer[String]()
    var timberusage = new ListBuffer[String]()

    val content = x._2
    var container = ""
    var sequence = 0
    content.split("\n").foreach(line =>{
      sequence += 1
      if(line.contains("Container:")){
        val gc = grokcont.`match`(line)
        val capc = gc.capture()
        val cont_matches = capc.asScala
        container = cont_matches.values.mkString("|")
      }
      if("estimate".equalsIgnoreCase(storage) && line.contains("memory.MemoryStore:")){
        val gm = grokestimate.`match`(line)
        val cap = gm.capture()
        if(!cap.isEmpty) {
          val mem_matches = cap.asScala//.values.mkString("|")
          //%{DATE_US:logdate} %{TIME:logtime} INFO memory.MemoryStore: Block %{WORD:broadcast_rdd} stored as %{WORD:values_bytes} in memory \(estimated size %{NUMBER:size} %{WORD:size_bytes}, free %{NUMBER:free} %{WORD:free_bytes}
          val (logdate, logtime, added_removed, broadcast_rdd, node, port, values_bytes, size, size_bytes, free, free_bytes) = (mem_matches("logdate").toString, mem_matches("logtime").toString, "Block"
            , mem_matches("broadcast_rdd").toString, "", "", mem_matches("values_bytes").toString, mem_matches("size").toString, mem_matches("size_bytes").toString, mem_matches("free").toString, mem_matches("free_bytes").toString)
          timberusage += s"${jobType}|${jobId}|${dmaCode}|${sscm}|${jobDate}|${clusterId}|${appId}|${container}|${logdate}|${logtime}|${added_removed}|${broadcast_rdd}|${node}|${port}|${values_bytes}|${size}|${size_bytes}|${free}|${free_bytes}|${sequence}"
//          timberusage += s"${jobType}|${jobId}|${dmaCode}|${sscm}|${jobDate}|${clusterId}|${appId}|${container}|${mem_matches}|${sequence}"
        }
      }
      if("storage".equalsIgnoreCase(storage) && line.contains("storage.BlockManagerInfo:")){
        val gsa = grokstore_add.`match`(line)
        val capsa = gsa.capture()
        if(!capsa.isEmpty) {
//          val store_add_matches = capsa.asScala.values.mkString("|")
          val store_addition = capsa.asScala
//          /%{DATE_US:logdate} %{TIME:logtime} INFO storage.BlockManagerInfo: %{WORD:added_removed} %{WORD:broadcast_rdd} in memory on %{GREEDYDATA:node}:%{NUMBER:port} \(size: %{NUMBER:size} %{WORD:size_bytes}, free: %{NUMBER:free} %{WORD:free_bytes}\)
          val (logdate, logtime, added_removed, broadcast_rdd, node, port, values_bytes, size, size_bytes, free, free_bytes) = (store_addition("logdate").toString, store_addition("logtime").toString, store_addition("added_removed").toString
            , store_addition("broadcast_rdd").toString, store_addition("node").toString, store_addition("port").toString, "", store_addition("size").toString, store_addition("size_bytes").toString, store_addition("free").toString, store_addition("free_bytes").toString)
          timberusage += s"${jobType}|${jobId}|${dmaCode}|${sscm}|${jobDate}|${clusterId}|${appId}|${container}|${logdate}|${logtime}|${added_removed}|${broadcast_rdd}|${node}|${port}|${values_bytes}|${size}|${size_bytes}|${free}|${free_bytes}|${sequence}"
        }
        val gsr = grokstore_remove.`match`(line)
        val capsr = gsr.capture()
        if(!capsr.isEmpty) {
//          val store_remove_matches = capsr.asScala.values.mkString("|")
          val store_remove = capsr.asScala
          val (logdate, logtime, added_removed, broadcast_rdd, node, port, values_bytes, size, size_bytes, free, free_bytes) = (store_remove("logdate").toString, store_remove("logtime").toString, store_remove("added_removed").toString
            , store_remove("broadcast_rdd").toString, store_remove("node").toString, store_remove("port").toString, "", store_remove("size").toString, store_remove("size_bytes").toString, store_remove("free").toString, store_remove("free_bytes").toString)
          timberusage += s"${jobType}|${jobId}|${dmaCode}|${sscm}|${jobDate}|${clusterId}|${appId}|${container}|${logdate}|${logtime}|${added_removed}|${broadcast_rdd}|${node}|${port}|${values_bytes}|${size}|${size_bytes}|${free}|${free_bytes}|${sequence}"
        }
      }
    })
    //(timberestimate.toList.mkString("\n"), timberusage.toList.mkString("\n"))
    timberusage.toList.mkString("\n")
  }

//  dxg_logs.map(x=> logExtraction(x, "estimate", "")).collect
//  dxg_logs.map(x=> logExtraction(x, "storage", "")).collect
}
