package com.datatec.sparker.core.exe.sink

import org.apache.spark.sql.SparkSession
import scala.collection.Map
import org.apache.spark.sql.SaveMode
import java.util
import org.apache.log4j.Logger
import org.apache.spark.sql.SQLContext._
import org.elasticsearch.spark.sql._
import scala.collection.JavaConversions._
import com.datatec.sparker.core.exe.BaseExecutor

/**
 *  
 * @author: Chao.Qian1
 * @date:   2019年6月12日 下午4:11:02   
 *
 */
class ElasticsearchSinkExecutor extends BaseExecutor{
  
     override def process(shellParams: Map[String,String], sparkSession: SparkSession){
         
          
         //参数替换，如果某个参数的值是value,则shell脚本中的参数名是-sparker.param.value=realvalue
         val _cfg = _configParams.map { f =>{
              (f._1, shellParams.getOrElse(s"sparker.params.${f._2}", f._2))
            }
         }.toMap
     
         try {
              val inputTableName = _cfg("inputTableName")
              val savaTarget = _cfg("sinkInfo")
              val esconf = _cfg.filter(prop => prop._1.startsWith("es."))
              var newTableDF = sparkSession.table(inputTableName)
              newTableDF.saveToEs(savaTarget,esconf)
             
            } catch {
              case e: Exception => e.printStackTrace()
            }
     }
}