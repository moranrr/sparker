package com.datatec.sparker.core.exe.sink

import scala.collection.Map

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import com.datatec.sparker.core.exe.BaseExecutor


class ExcelSinkExecutor extends BaseExecutor{
    
     private val fullFormat: String = "com.crealytics.spark.excel"
    
     override def process(shellParams: Map[String,String], sparkSession: SparkSession){
         
          
         //参数替换，如果某个参数的值是value,则shell脚本中的参数名是-sparker.param.value=realvalue
         val _cfg = _configParams.map { f =>{
              (f._1, shellParams.getOrElse(s"sparker.params.${f._2}", f._2))
            }
         }.toMap
     
         val inputTable = _cfg("inputTableName")
         val outTarget = _cfg("sinkInfo")
         val options = _cfg - "sinkInfo" - "saveModel" - "format"
         
         var newTableDF = sparkSession.table(inputTable)
         
         val mode = _cfg.getOrElse("saveModel", "ErrorIfExists")
         
         val writer = newTableDF.write.mode(SaveMode.valueOf(mode))
                                     .options(options)
                                     .format(fullFormat)
         
         writer.save(outTarget)
         /*newTableDF.write
          .format("com.crealytics.spark.excel")
          .option("dataAddress", "'My Sheet'!B3:C35")
          .option("useHeader", "true")
          .option("dateFormat", "yy-mmm-d") // Optional, default: yy-m-d h:mm
          .option("timestampFormat", "mm-dd-yyyy hh:mm:ss") // Optional, default: yyyy-mm-dd hh:mm:ss.000
          .mode("append") // Optional, default: overwrite.
          .save("Worktime2.xlsx")*/
     }
}