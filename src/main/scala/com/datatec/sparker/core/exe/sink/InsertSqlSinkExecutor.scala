package com.datatec.sparker.core.exe.sink

import org.apache.spark.sql.SparkSession
import scala.collection.Map
import org.apache.spark.sql.SaveMode
import org.apache.log4j.Logger
import org.apache.spark.sql.SQLContext._
import org.elasticsearch.spark.sql._
import scala.collection.JavaConversions._
import com.datatec.sparker.utils.jdbc.SQLUtil
import java.util.Properties
import org.apache.spark.sql.{DataFrame, SaveMode}
import java.util.List
import java.util.ArrayList
import scala.collection.mutable.ArrayBuffer
import com.datatec.sparker.utils.jdbc.JDBCHelper
import com.datatec.sparker.utils.jdbc.MultiPoolHelper
import com.datatec.sparker.utils.jdbc.ConnectionBean
import com.datatec.sparker.core.exe.BaseExecutor

/**
 *  
 * @author: Chao.Qian1
 * @date:   2019年6月12日 下午4:11:02   
 *
 */
class InsertSqlSinkExecutor extends BaseExecutor{
  
     override def process(shellParams: Map[String,String], sparkSession: SparkSession){
         
          
         //参数替换，如果某个参数的值是value,则shell脚本中的参数名是-sparker.param.value=realvalue
         val _cfg = _configParams.map { f =>{
              (f._1, shellParams.getOrElse(s"sparker.params.${f._2}", f._2))
            }
         }.toMap
     
         val script = _cfg.getOrElse("sinkInfo", _cfg.getOrElse("sql", ""))
         
         val sql_script = {
           if (script.startsWith("file:/") || script.startsWith("hdfs:/")) {
               sparkSession.sparkContext.textFile(script).collect().mkString("\n")
           }
           else script
         }
         //替换参数
         val sql = translateSQL(sql_script, shellParams)
         
         val inputTableName = _cfg.getOrElse("inputTableName", "")
         val driver =  _cfg("driver")
         val url = _cfg("url")
         val userName = _cfg.getOrElse("user", "")
         val password = _cfg.getOrElse("password", "")
         val poolsize = _cfg.getOrElse("poolsize", "1").toInt
         val batchSize = _cfg.getOrElse("batchsize", "50000").toInt
         
         if(!"".equals(inputTableName.trim())){
    	      var newTableDF = sparkSession.table(inputTableName)
    	      //insert table columns
    	      val cols = SQLUtil.parshSqlColumns(sql)
    	      
    	      val urls = url.split("\\|")
    	      
    	      //如果只有一个URL
    	      if(urls.length == 1){
    	          //保存
        	      newTableDF.foreachPartition(iterator =>{
        	        val list = ArrayBuffer[List[Object]]()
        	        import scala.collection.JavaConverters._
        	        iterator.foreach(row => {
        	          val insertParams =  for(col <- cols) yield  row.getAs[Object](col) 
        	          list+=insertParams.toList
        	        })
        	        var connBean:ConnectionBean   = null;
        	        try{
        	           connBean = MultiPoolHelper.getInstance().getConnection(driver, url,userName,password, poolsize)
        	           val jdbcHelper:JDBCHelper = JDBCHelper.getInstance();
        	           jdbcHelper.executeBatch(sql, list.asJava, connBean, batchSize)
        	        }catch {
        	            
                        case e: Exception => {e.printStackTrace();throw e}
                        
                    }   finally{
        	            MultiPoolHelper.getInstance().release(url,connBean)
        	        }
        	        
        	      })
    	      }else if(urls.length > 1){
    	           //如果有多个URL，则average平均写入每个URL
        	      newTableDF.foreachPartition(iterator =>{
            	        val list = ArrayBuffer[List[Object]]()
            	        
            	        import scala.collection.JavaConverters._
            	        iterator.foreach(row => {
            	           val insertParams =  for(col <- cols) yield  row.getAs[Object](col) 
            	           list+=insertParams.toList
            	        })
            	        //得到每个URL的连接
            	        val connBeanList = new ArrayBuffer[ConnectionBean]
            	        val connMap = new scala.collection.mutable.HashMap[String,ConnectionBean] 
            	        try{
                  	        urls.foreach(url => {
                  	            var connBean = MultiPoolHelper.getInstance().getConnection(driver, url,userName,password, poolsize)
                    	          connBeanList+=connBean
                    	          connMap+=((url,connBean))
                  	        })
            	            val jdbcHelper:JDBCHelper = JDBCHelper.getInstance();
            	            jdbcHelper.executeBatch(sql, list.asJava, connBeanList, batchSize)
            	        }catch {
                            case e: Exception => {e.printStackTrace();throw e}
                        } finally{
                            urls.foreach(url =>  MultiPoolHelper.getInstance().release(url,connMap.getOrElse(url, null)))
                        }
        	        
        	      })
    	      }
          }else{
                //直接执行SQL
                var connBean:ConnectionBean   = null;
    	        try{
    	            connBean = MultiPoolHelper.getInstance().getConnection(driver, url,userName,password, poolsize)
    	            val jdbcHelper:JDBCHelper = JDBCHelper.getInstance();
    	            jdbcHelper.executeSql(sql, connBean)
    	        }catch {
                    case e: Exception => {e.printStackTrace();throw e}
                } finally{
    	          MultiPoolHelper.getInstance().release(url,connBean)
    	        }
          }
     }
}