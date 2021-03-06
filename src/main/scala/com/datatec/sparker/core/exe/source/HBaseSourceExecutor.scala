package com.datatec.sparker.core.exe.source

import com.datatec.sparker.core.exe.Executor
import com.datatec.sparker.core.exe.ExecutorHelper
import scala.collection.Map
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import com.datatec.sparker.core.exe.BaseExecutor

class HBaseSourceExecutor  extends BaseExecutor{
  
  private val fullFormat: String = "com.datatec.sparker.sql.source.hbase"
  
  
  val dbSplitter: String = ":"
  
  
  override def process(shellParams: Map[String,String], sparkSession: SparkSession){
     //for((key,value) <- _configParams)println(key + ":" + value)
     //参数替换，如果某个参数的值是value,则shell脚本中的参数名是-sparker.param.value=realvalue
     val _cfg = _configParams.map { f =>{
          (f._1, shellParams.getOrElse(s"sparker.params.${f._2}", f._2))
        }
     }.toMap
     
     val hbaseTableName = _cfg("hbaseTableName")
     
     val reader = sparkSession.read
    
     reader.option("hbase_table_name", hbaseTableName)
     reader.options(_cfg)
     var df = reader.format(fullFormat).load()
    
     val numPartitions = _cfg.getOrElse("numPartitions", "-1").toInt
     
     //重新分区
     if(numPartitions != -1){
          df = df.repartition(numPartitions)
     }
    
     //是否需要持久化
     val isPersist = _cfg.getOrElse("isCache", "false").toBoolean
     if(isPersist) df = df.persist(StorageLevel.MEMORY_AND_DISK)
     
     val outputTable = _cfg.get("outTableName")
     outputTable match{
       case Some(outTableName) => df.createOrReplaceTempView(outTableName)
       case None => 
     }
    
    
  }
  
}