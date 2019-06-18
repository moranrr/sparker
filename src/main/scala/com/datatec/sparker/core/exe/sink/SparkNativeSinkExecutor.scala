package com.datatec.sparker.core.exe.sink

import org.apache.spark.sql.SparkSession
import scala.collection.Map
import org.apache.spark.sql.SaveMode
import com.datatec.sparker.core.exe.BaseExecutor


class SparkNativeSinkExecutor extends BaseExecutor{
  
     override def process(shellParams: Map[String,String], sparkSession: SparkSession){
         
          
         //参数替换，如果某个参数的值是value,则shell脚本中的参数名是-sparker.param.value=realvalue
         val _cfg = _configParams.map { f =>{
              (f._1, shellParams.getOrElse(s"sparker.params.${f._2}", f._2))
            }
         }.toMap
     
         val inputTable = _cfg("inputTableName")
         val outputFileNum = _cfg.getOrElse("fileNum", "-1").toInt
         val partitionBy = _cfg.getOrElse("partitionBy", "")
         
         var newTableDF = sparkSession.table(inputTable)
         
         if (outputFileNum != -1) {
             newTableDF = newTableDF.repartition(outputFileNum)
         }
         val outTarget = _cfg("sinkInfo")
         val options = _cfg - "sinkInfo" - "saveModel" - "format"
        
         val dbtable = if (options.contains("dbtable")) options("dbtable") else outTarget
         val mode = _cfg.getOrElse("saveModel", "ErrorIfExists")
         val format = _cfg("format")
         
         var tempDf = if (!partitionBy.isEmpty) {
             newTableDF.write.partitionBy(partitionBy.split(","): _*)
         } else {
             newTableDF.write
         }
    
         
         
         if (format == "jdbc") {
             val configParams = new scala.collection.mutable.HashMap[String,String]
             configParams ++= options
             if (options.contains("dbtable")) configParams("dbtable") = outTarget
             tempDf = tempDf.options(configParams).mode(SaveMode.valueOf(mode)).format(format)
             tempDf.save()
         }else if(format == "hive"){
            tempDf = tempDf.mode(SaveMode.valueOf(mode))
            tempDf.saveAsTable(outTarget)
         }else {
             tempDf = tempDf.options(options).mode(SaveMode.valueOf(mode)).format(format)
             tempDf.save(outTarget)
        }
     }
}