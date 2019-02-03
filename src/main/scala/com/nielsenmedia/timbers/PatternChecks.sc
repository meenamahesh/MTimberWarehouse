val logfile = "file:///Users/mame7006/Documents/mm_bkp/logs/u002/2019-01-03/sc/apps/86/SC_8166810_532_71_2019-01-03_2019-01-03_application_1546639822896_0125.log"
val container_log= "Container: container_1547672528559_0002_01_000009 on ip-10-236-121-189.aws-w.nielsencsp.net_8041"
val container_grok_pattern="Container: container_%{GREEDYDATA:container} on %{GREEDYDATA:ip}.aws-w.nielsencsp.net_%{NUMBER:port}"

//   19/01/16 21:41:42 INFO memory.MemoryStore: Block broadcast_1227_piece0 stored as bytes in memory (estimated size 5.4 KB, free 2.0 GB)
//   19/01/16 21:41:56 INFO memory.MemoryStore: Block rdd_3641_5 stored as values in memory (estimated size 762.7 KB, free 2.0 GB)
val mem_log = "19/01/05 00:03:23 INFO memory.MemoryStore: Block broadcast_139_piece0 stored as bytes in memory (estimated size 24.1 KB, free 2.1 GB)"
val mem_log2 = "19/01/16 21:41:56 INFO memory.MemoryStore: Block rdd_3641_5 stored as values in memory (estimated size 762.7 KB, free 2.0 GB)"
val mem_log_pattern="%{GREEDYDATA:logdate} %{TIME:logtime} INFO memory.MemoryStore: Block %{WORD:broadcast} stored as bytes in memory \\(estimated size %{NUMBER:size} %{WORD:size_bytes}, free %{NUMBER:free} %{WORD:free_bytes}\\)"
val mem_log_pattern2="%{DATE_US:logdate} %{TIME:logtime} INFO memory.MemoryStore: Block %{WORD:broadcast} stored as %{WORD:values_bytes} in memory \\(estimated size %{NUMBER:size} %{WORD:size_bytes}, free %{NUMBER:free} %{WORD:free_bytes}\\)"
import java.text.SimpleDateFormat
import java.util.Calendar

import io.krakens.grok.api.{Grok, GrokCompiler}
//import org.elasticsearch.grok.Grok
val grokcomp = GrokCompiler.newInstance()
grokcomp.registerDefaultPatterns()
val grok: Grok = grokcomp.compile(mem_log_pattern2)
import scala.collection.JavaConversions._
import scala.collection.mutable
val gm = grok.`match`(mem_log)
val cap = gm.capture()
import scala.collection.JavaConverters._
val mem_matches = cap.asScala

//val matches = gm.capture().toList
//for(matched <- matches) println(matched)
grok.`match`(mem_log2).capture().asScala

//DXG_9888853_602_3_2019-01-15_application_1547672528559_0131.log
//job_type jobId dma sscm date applicationId
//ScRef_9887786_application_1547672528559_0001.log
//SC_9887784_810_71_2019-01-15_2019-01-15_application_1547672528559_0093.log
val format = new SimpleDateFormat("y-M-d")
val scDate=format.format(Calendar.getInstance().getTime())
scDate
val scCad="86"
val scCadMapping = Map("86"->"Daily", "87" -> "Cal Weekly", "88" -> "NSI Weekly", "89" -> "Cal Monthly", "90" -> "NSI Monthly")

//var (jobType, jobId, dmaCode, sscm, jobDate, containerId, appId)  = ("", "", "", "", "", "", "")

val filename = "ScRef_9887786_application_1547672528559_0001.log" //"SC_9887784_810_71_2019-01-15_2019-01-15_application_1547672528559_0093.log"//"DXG_9888853_602_3_2019-01-15_application_1547672528559_0131.log"
val fileSect = filename.replace(".log","").split("_")
val (jobType, jobId, dmaCode, sscm, jobDate, clusterId, appId) = fileSect(0) match {
  case "DXG" => {
    println("data extractor with grabix")
    (fileSect(0), fileSect(1), fileSect(2), fileSect(3), fileSect(4), fileSect(6), fileSect(7))
  }
  case "SC" => {
    println("")
    ("SC".concat(scCadMapping(scCad)), fileSect(1), fileSect(2), fileSect(3), fileSect(5), fileSect(7), fileSect(8))
  }
  case "ScRef" => {
    println("SCRef to NLTV")
    (fileSect(0), fileSect(1), "", "", scDate, fileSect(3), fileSect(4))
  }
}

val grokcont: Grok = grokcomp.compile("Container: container_%{GREEDYDATA:container} on %{GREEDYDATA:ip}.aws-w.nielsencsp.net_%{NUMBER:port}")

var container = ""
val gc = grokcont.`match`(container_log)
val capc = gc.capture()
import scala.collection.JavaConverters._
val cont_matches = capc.asScala
container = cont_matches.values.mkString("||~||")
println(cont_matches.values.mkString(","))

val grokstore_add: Grok = grokcomp.compile("%{DATE_US:logdate} %{TIME:logtime} INFO storage.BlockManagerInfo: %{WORD:added_removed} %{WORD:broadcast_rdd} in memory on %{GREEDYDATA:node}:%{NUMBER:port} \\(size: %{NUMBER:size} %{WORD:size_bytes}, free: %{NUMBER:free} %{WORD:free_bytes}\\)")
val grokstore_remove: Grok = grokcomp.compile("%{DATE_US:logdate} %{TIME:logtime} INFO storage.BlockManagerInfo: %{WORD:added_removed} %{WORD:broadcast_rdd} on %{GREEDYDATA:node}:%{NUMBER:port} in memory \\(size: %{NUMBER:size} %{WORD:size_bytes}, free: %{NUMBER:free} %{WORD:free_bytes}\\)")

val gsa = grokstore_add.`match`("19/01/16 21:41:42 INFO storage.BlockManagerInfo: Added broadcast_1227_piece0 in memory on ip-10-236-126-8.aws-w.nielsencsp.net:44593 (size: 5.4 KB, free: 6.9 GB)")
val capsa = gsa.capture()
if(!capsa.isEmpty) {
  val store_add_matches = capsa.asScala.values.mkString("|")
  val store_addition = capsa.asScala
  capsa.get("broadcast_rdd").toString
  val (logdate, logtime, added_removed, broadcast_rdd, node, port, values_bytes, size, size_bytes, free, free_bytes) = (store_addition("logdate").toString, store_addition("logtime").toString, store_addition("added_removed").toString
    , store_addition("broadcast_rdd").toString, store_addition("node").toString, store_addition("port").toString, "", store_addition("size").toString, store_addition("size_bytes").toString, store_addition("free").toString, store_addition("free_bytes").toString)
  s"${logdate}|${logtime}|${added_removed}|${broadcast_rdd}|${node}|${port}|${values_bytes}|${size}|${size_bytes}|${free}|${free_bytes}"

  //  println(store_add_matches)
}
val gsr = grokstore_remove.`match`("19/01/16 21:41:52 INFO storage.BlockManagerInfo: Removed broadcast_1227_piece0 on ip-10-236-125-58.aws-w.nielsencsp.net:37577 in memory (size: 5.4 KB, free: 2.1 GB)")
val capsr = gsr.capture()
if(!capsr.isEmpty) {
  val store_remove_matches = capsr.asScala.values.mkString("|")
  println(store_remove_matches)
}