package com.datatec.sparker.core.exe.source

import com.datatec.sparker.core.exe.Executor
import com.datatec.sparker.core.exe.ExecutorHelper
import scala.collection.Map
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.types._
import com.datatec.sparker.core.exe.BaseExecutor

class ExcelSourceExecutor  extends BaseExecutor{
  
  private val fullFormat: String = "com.crealytics.spark.excel"
  
  override def process(shellParams: Map[String,String], sparkSession: SparkSession){
     //for((key,value) <- _configParams)println(key + ":" + value)
     //参数替换，如果某个参数的值是value,则shell脚本中的参数名是-sparker.param.value=realvalue
     val _cfg = _configParams.map { f =>{
          (f._1, shellParams.getOrElse(s"sparker.params.${f._2}", f._2))
        }
     }.toMap
     
     /*
      * val df = spark.read
        .format("com.crealytics.spark.excel")
        .option("dataAddress", "'My Sheet'!B3:C35") // Optional, default: "A1"
        .option("useHeader", "true") // Required
        .option("treatEmptyValuesAsNulls", "false") // Optional, default: true ;
        .option("inferSchema", "false") // Optional, default: false,if ture and not set schema use excerptSize(10 rows) to refer schema,if false  all cols is string
        .option("addColorColumns", "true") // Optional, default: false
        .option("timestampFormat", "MM-dd-yyyy HH:mm:ss") // Optional, default: yyyy-mm-dd hh:mm:ss[.fffffffff]
        .option("maxRowsInMemory", 20) // Optional, default None. If set, uses a streaming reader which can help with big files
        .option("excerptSize", 10) // Optional, default: 10. If set and if schema inferred, number of rows to infer schema from
        .option("workbookPassword", "pass") // Optional, default None. Requires unlimited strength JCE for older JVMs
        .schema(myCustomSchema) // Optional, default: Either inferred schema, or all columns are Strings
        .load("Worktime.xlsx")
      */
     val numPartitions = _cfg.getOrElse("numPartitions", "-1").toInt
     val sourcePath = _cfg("path")
     val registerTableSchema = _cfg.getOrElse("schema","")
     
     
     var reader = sparkSession.read.format(fullFormat).options(
         (_cfg - "format" - "path" - "outTableName"))
         
     
     if(registerTableSchema != "") {
         val fieldsStr = registerTableSchema.trim.drop(1).dropRight(1)
         val fieldsArray = fieldsStr.split(",").map(_.trim)
         
         val schema = {
             val fields = fieldsArray.map{ fildString =>
               val splitedField = fildString.split("\\s+", -1)
               val relatedType =  splitedField(1).toLowerCase() match  {
                  case "string" =>
                    StringType
                  case "int" =>
                    IntegerType
                  case "long" =>
                    IntegerType
                  case "double" =>
                    DoubleType
                }
                StructField(splitedField(0),relatedType,true)
             }
             
             StructType(fields)
         }
         reader.schema(schema)
     }
     
     var df = reader.load(sourcePath)
     
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