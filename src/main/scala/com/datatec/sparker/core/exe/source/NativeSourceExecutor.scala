package com.datatec.sparker.core.exe.source

import com.datatec.sparker.core.exe.Executor
import com.datatec.sparker.core.exe.ExecutorHelper
import scala.collection.Map
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import com.datatec.sparker.core.exe.BaseExecutor

class NativeSourceExecutor  extends BaseExecutor{
  
  
  
  override def process(shellParams: Map[String,String], sparkSession: SparkSession){
     //for((key,value) <- _configParams)println(key + ":" + value)
    
     //参数替换，如果某个参数的值是value,则shell脚本中的参数名是-sparker.param.value=realvalue
     val _cfg = _configParams.map { f =>{
           (f._1, shellParams.getOrElse(s"sparker.params.${f._2}", f._2))
        }
     }.toMap
    
     val numPartitions = _cfg.getOrElse("numPartitions", "-1").toInt
     val sourcePath = _cfg("path")
     val format = _cfg("format")
     
     var df = sparkSession.read.format(format).options(
         (_cfg - "format" - "path" - "outTableName")).load(sourcePath)
     
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