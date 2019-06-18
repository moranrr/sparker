package com.datatec.sparker.core.exe.impl

import com.datatec.sparker.core.exe.Executor
import com.datatec.sparker.core.exe.ExecutorHelper
import scala.collection.Map
import org.apache.spark.sql.{DataFrame, Row}
import net.liftweb.{json => SJSon}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.{ScalaSourceCodeCompiler, ScriptCacheKey}
import scala.collection.JavaConversions._
import org.apache.spark.sql.Encoders
import com.datatec.sparker.core.exe.BaseExecutor

/**
 * @ClassName:  ScalaExecutor   
 * @Description:TODO
 * @author: Chao.Qian1
 * @date:   2019年6月11日 下午4:11:02   
 *
 */
class ScalaExecutor  extends BaseExecutor{
  
  
  override def process(shellParams: Map[String,String], sparkSession: SparkSession){
     //for((key,value) <- _configParams)println(key + ":" + value)
    
     //参数替换，如果某个参数的值是value,则shell脚本中的参数名是-sparker.param.value=realvalue
     val _cfg = _configParams.map { f =>{
           (f._1, shellParams.getOrElse(s"sparker.params.${f._2}", f._2))
        }
     }.toMap
    
     
     val _script = _cfg("script")//code
     val schema = _cfg.get("schema")
     val _ignoreOldColumns = _cfg.getOrElse("ignoreOldColumns","false").toBoolean
    
     val df = sparkSession.table(_cfg("inputTable"))
    
     def loadScript(script: String) = {
       if (script.startsWith("file:/") || script.startsWith("hdfs:/")) {
           sparkSession.sparkContext.textFile(script).collect().mkString("\n")
       }
       else script
     }
     
     val scala_script = loadScript(_script)
    
     //对于每一行都执行一次自定义的处理代码
     val newRdd = df.rdd.map { row =>
          val maps =  { 
              val executor = ScalaSourceCodeCompiler.getExecute(ScriptCacheKey("doc",scala_script))
              executor.execute(row.getValuesMap(row.schema.fieldNames))
          }
          //是否忽略旧的列
          val newMaps = if (_ignoreOldColumns) maps 
                   else row.schema.fieldNames.map(f => (f, row.getAs(f))).toMap ++ maps

          newMaps
     }
     
      var newDF = 
      if (schema != None) {
          val schemaScript = loadScript(schema.getOrElse(""))
          
          val rowRdd = newRdd.map { newMaps =>
            val schemaExecutor = ScalaSourceCodeCompiler.getExecute(ScriptCacheKey("schema", schemaScript))
            val st = schemaExecutor.schema().get
            //筛选掉不在自定义schema中的列
            Row.fromSeq(st.fieldNames.map { fn =>
               if (newMaps.containsKey(fn)) newMaps(fn) else null
            }.toSeq)
          }
          
          val schemaExecutor = ScalaSourceCodeCompiler.getExecute(ScriptCacheKey("schema", schemaScript))
          sparkSession.createDataFrame(rowRdd, schemaExecutor.schema().get)
      } else {
          val jsonRdd = newRdd.map { newMaps =>
            implicit val formats = SJSon.Serialization.formats(SJSon.NoTypeHints)
            SJSon.Serialization.write(newMaps)
          }
          sparkSession.read.json(sparkSession.createDataset(jsonRdd)(Encoders.STRING))
     }
     
     val numPartitions = _cfg.getOrElse("numPartitions", "-1").toInt
     //重新分区
     if(numPartitions != -1){
          newDF = newDF.repartition(numPartitions)
     }
     
     //是否需要持久化
     val isPersist = _cfg.getOrElse("isCache", "false").toBoolean
     if(isPersist) newDF = newDF.persist(StorageLevel.MEMORY_AND_DISK)
     
     val outputTable = _cfg.get("outTableName")
     outputTable match{
       case Some(outTableName) => newDF.createOrReplaceTempView(outTableName)
       case None => 
     }
  }
  
}