package com.datatec.sparker.core.exe.impl

import scala.collection.Map

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import com.datatec.sparker.core.exe.Executor
import com.datatec.sparker.core.exe.ExecutorHelper
import com.datatec.sparker.core.exe.BaseExecutor
import com.datatec.sparker.utils.ParshSBASQL

/**
 * 
 * @ClassName:  HSqlExecutor   
 * @Description:TODO
 * @author: Chao.Qian1
 * @date:   2019年6月12日 下午4:11:02   
 *
 */
class HSqlExecutor  extends BaseExecutor{
  
  
  
  override def process(shellParams: Map[String,String], sparkSession: SparkSession){
    
     //参数替换，如果某个参数的值是value,则shell脚本中的参数名是-sparker.param.value=realvalue
     val _cfg = _configParams.map { f =>{
           (f._1, shellParams.getOrElse(s"sparker.param.${f._2}", f._2))
        }
     }.toMap
     
     val script = _cfg("sql")
     val sql = {
       if (script.startsWith("file:/") || script.startsWith("hdfs:/")) {
           sparkSession.sparkContext.textFile(script).collect().mkString("\n")
       }
       else script
     }
     
     val sql_script = translateSQL(sql, shellParams)
     
     val sqls = ParshSBASQL.parshSqlFile(sql_script)
     
     //如果有多少sql语句，则依次执行
     if(sqls.length > 1){
        sqls.foreach(sql => sparkSession.sql(sql))
     }else{
         var df = sparkSession.sql(sql_script)
         
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
  
}