import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import java.text.SimpleDateFormat
import java.util.Properties
import org.apache.spark.sql.functions.rank
import org.apache.spark.sql.expressions.Window
import java.sql.Timestamp


//////////////////////////////////////////FIBRE RING Logic Start////////////////////////////////////
val df_fibrering=spark.read.format("csv").option("header","true").load("fibre_ring.csv").select($"RingID",$"css_sequence_in_ring",$"enode_b_sapid",$"Takeoff east_ag1_sapid",$"Takeoff west_ag1_sapid")

val fibre_ring=df_fibrering.select($"RingID",$"css_sequence_in_ring".cast("Integer"),$"enode_b_sapid").unionAll(df_fibrering.select($"RingID",lit(0).as("css_sequence_in_ring"),$"Takeoff east_ag1_sapid").distinct).unionAll(df_fibrering.groupBy($"RingID",$"Takeoff west_ag1_sapid").agg((max($"css_sequence_in_ring")+1).cast("Integer").as("css_sequence_in_ring")).select($"RingID",$"css_sequence_in_ring".cast("Integer"),$"Takeoff west_ag1_sapid"))

val df_fibre_ring=fibre_ring.join(df_topology,$"enode_b_sapid"===$"SAPID").select($"RingID",collect_list($"enode_b_sapid").over(Window.partitionBy($"RingID").orderBy($"css_sequence_in_ring")).as("saplist"),collect_list($"type").over(Window.partitionBy($"RingID").orderBy($"css_sequence_in_ring")).as("typelist")).groupBy($"RingID").agg(max($"saplist").as("value"),max($"typelist").as("type")).filter(size($"value")>1).select($"RingID",$"value",$"type",size($"value").as("sz")).
withColumn("typedesc",when($"type"(0)==="INFRA" || $"type"(size($"type")-1)==="INFRA",lit("INFRA")).otherwise(when($"type"(0)==="BACKHAUL" || $"type"(size($"type")-1)==="BACKHAUL",lit("BACKHAUL")).otherwise(lit("UNKNOWN")))).drop("type").
withColumn("start",$"value"(0)).
withColumn("end",$"value"(size($"value")-1)).
withColumn("value",explode($"value")).filter($"value"!==$"start").filter($"value"!==$"end")
broadcast(df_fibre_ring)
val df_topology_ring=df_topology.join(df_fibre_ring,$"SAPID"===$"value","LEFT_OUTER").withColumn("RFO_DESC",when($"value".isNotNull,when($"typedesc"==="INFRA",concat(lit("Infra Dependant Site: "),$"start",lit(" and "),$"end",lit(" nodes are down in "),$"Ringid", lit(" RingID"))).otherwise(concat(lit("Fibre Dependant Site: "),$"start",lit(" and "),$"end",lit(" nodes are down in "),$"Ringid", lit(" RingID")))).otherwise($"RFO_DESC")).withColumn("type",when($"typedesc"!=="UNKNOWN",$"typedesc").otherwise($"type")).drop("RingID","value","sz","typedesc","start","end")
println("RING Topology correlation completed")
//////////////////////////////////////////FIBRE RING Logic End////////////////////////////////////

//////////////////////////////////////////FIBRE SPUR Logic Start////////////////////////////////////
val spur_file=spark.read.format("csv").option("header","true").load("fibre_spur.csv").select($"CSS Ring id".as("ringid"),$"enode_b_sapid".as("sapid_c"),$" Node Sequence_in_ring".cast("Integer").as("sequence"),$"Take-off Site sapid".as("sapid_p")).sort("ringid","sequence").filter($"sequence"!==0)
val df_spur=spur_file.select($"ringid",$"sapid_c".as("spur_sap"),$"sequence").unionByName(spur_file.select($"ringid",$"sapid_p".as("spur_sap"),lit(0).as("sequence"))).distinct
val df_fibre_spur=df_spur.join(df_topology_ring,$"spur_sap"===$"sapid").
select($"ringid",
  collect_list($"spur_sap").over(Window.partitionBy($"RingID").orderBy($"sequence")).as("saplist"),
  collect_list($"type").over(Window.partitionBy($"RingID").orderBy($"sequence")).as("typelist"),
  collect_list($"rfo_desc").over(Window.partitionBy($"RingID").orderBy($"sequence")).as("rfolist")).
groupBy($"RingID").agg(
  max($"saplist").as("value"),
  max($"typelist").as("typelist"),
  max($"rfolist").as("rfolist")).filter(size($"value")>1).
withColumn("typedesc",$"typelist"(0)).
withColumn("parent",$"value"(0)).
withColumn("parent_rfo",when($"rfolist"(0).contains("Dependant Site"),$"rfolist"(0))).
drop("typelist").
select(explode($"value").as("value"),$"typedesc",$"parent",$"parent_rfo").filter($"value"!==$"parent")
broadcast(df_fibre_spur)

val df_topology_spur=df_topology_ring.join(df_fibre_spur,$"SAPID"===$"value","LEFT_OUTER").withColumn("RFO_DESC",when($"value".isNotNull,when($"parent_rfo".isNotNull,$"parent_rfo").otherwise(when($"typedesc"==="INFRA",concat(lit("Infra Dependant Site: Parent Site("),$"parent",lit(") is Down"))).otherwise(concat(lit("Fibre Dependant Site: "),$"parent",lit(" is Down"))))).otherwise($"RFO_DESC")).withColumn("type",when($"typedesc"!=="UNKNOWN",$"typedesc").otherwise($"type")).drop("value","typedesc","parent","parent_rfo")
println("SPUR Topology correlation completed")
/*
/////////////////////////////////////////Old way of Fibre Spur correlation//////////////////////////////////////////////////
val fibre_spur=spark.read.format("csv").option("header","true").load("fibre_spur.csv").select($"CSS Ring id".as("ringid"),$"enode_b_sapid".as("sapid_c"),$" Node Sequence_in_ring".as("sequence"),$"Take-off Site sapid".as("sapid_p")).sort("ringid","sequence").filter($"sequence"!=="0").withColumn("lst",collect_list($"sapid_c").over(Window.partitionBy($"RingID",$"sapid_p").orderBy($"sequence"))).groupBy($"RingID",$"sapid_p").agg(max($"lst").as("value")).select($"RingID",concat($"sapid_p",lit(","),concat_ws(",",$"value")).as("value")).drop("Ringid")
val ndn1_fib=fibre_spur.join(nodedown_cp2.unionByName(df_ag1).select($"SAPID".as("sap")),$"value".contains(col("sap"))).withColumn("id",split(regexp_replace($"value",$"sap",lit("~")),"~")(0)).withColumn("child",split(regexp_replace($"value",$"sap",lit("~")),"~")(1)).withColumn("no",length($"id")-length(regexp_replace($"id",",",""))).drop("id")
val ndnmin_fib=ndn1_fib.groupBy($"value").agg(min($"no").as("no"))
val df_cp15=ndn1_fib.join(ndnmin_fib,Seq("value","no")).select($"sap".as("parent"),explode(split($"child",",")).as("child")).filter(length($"child")>0)
broadcast(df_cp15)
val cp15=nodedown_cp14.join(df_cp15,$"SAPID"===$"child","LEFT_OUTER").withColumn("type",when($"child".isNotNull,lit("BACKHAUL")).otherwise("UNKNOWN")).withColumn("RFO_DESC",when($"child".isNotNull,concat(lit("Fibre/Infra Dependant Site: Parent Site("),$"parent",lit(") is Down")))).select($"ALARM_ID",$"SAPID",$"AGEING",$"site_category",$"type",$"RFO",$"AlarmProbableCause",substring($"ORIGINAL_EVENT_TIME",1,16).cast("timestamp").as("ORIGINAL_EVENT_TIME"),lit("").as("LINKED_ALARM"),$"RFO_DESC")
val nodedown_cp15=cp15.filter($"TYPE"==="UNKNOWN")
*/
println("fibre child completed")
//////////////////////////////////////////FIBRE SPUR Logic End////////////////////////////////////

//////////////////////////////////////////MW HopDependency Logic Start////////////////////////////////////
mw_tree=spark.read.textFile("HopDependencylist.csv").cache
val ndn1=mw_tree.join(df_topology_spur.select($"SAPID".as("sap"),$"type",$"rfo_desc"),$"value".contains(col("sap"))).withColumn("id",split(regexp_replace($"value",$"sap",lit("~")),"~")(0)).withColumn("child",split(regexp_replace($"value",$"sap",lit("~")),"~")(1)).withColumn("no",length($"id")-length(regexp_replace($"id",",",""))).drop("id")
val ndnmin=ndn1.groupBy($"value").agg(min($"no").as("no"))
val df_cp16=ndn1.join(ndnmin,Seq("value","no")).select($"sap".as("parent"),$"type".as("typedesc"),when($"rfo_desc".contains("Dependant Site"),$"rfo_desc").as("parent_rfo"),explode(split($"child",",")).as("child")).filter(length(trim($"child"))>0).distinct
broadcast(df_cp16)

val df_topology_mw=df_topology_spur.join(df_cp16,$"SAPID"===$"child","LEFT_OUTER").
withColumn("type",when($"typedesc"==="INFRA","INFRA").otherwise(when($"typedesc"==="BACKHAUL","BACKHAUL").otherwise($"type"))).
withColumn("RFO_DESC",when($"parent_rfo".isNotNull,$"parent_rfo").otherwise(when($"typedesc"==="INFRA",concat(lit("Infra Dependant Site: Parent Site("),$"parent",lit(") is Down"))).otherwise(when($"typedesc"==="BACKHAUL",concat(lit("MW Dependant Site: Parent Site("),$"parent",lit(") is Down"))).otherwise($"RFO_DESC")))).drop("parent","child","typedesc","parent_rfo").cache
df_topology_mw.count
