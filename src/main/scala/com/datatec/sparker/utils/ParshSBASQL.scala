package com.datatec.sparker.utils

import com.datatec.sparker.model.Dispatcher
import scala.collection.mutable.ArrayBuffer
import com.datatec.sparker.common.ShortNameMapping
import com.datatec.sparker.core.exe.Executor
import com.datatec.sparker.utils.ParshCommonUtil._
import org.apache.spark.sql.SaveMode

/**
 * @author: Chao.Qian1
 * @date:   2019年6月10日 上午10:11:02   
 *
 */
object ParshSBASQL {
    
    
  def parshSqlFile(sqlStr: String) : ArrayBuffer[String] = {
      val lineArr = sqlStr.split("\n").map(l => l.trim()).filter(line => line!="")
      var jobdesc: String = null
      val sqlArray = new ArrayBuffer[String]()
      val result = new ArrayBuffer[String]()
      lineArr.foreach(line => {
      if(line.length() < 2 || !line.substring(0, 2).equals("--")){
          result += line
          if(line.endsWith(";")){
            jobdesc = result.mkString(" ")
            sqlArray += jobdesc
            result.clear()
          }
      }
    })
    if(result.length > 0 && sqlArray.length==0)  {
        jobdesc = result.mkString(" ")
        sqlArray += jobdesc
    }
    sqlArray
  }

  def dispatcher(sqlStr: String) : Tuple2[scala.collection.mutable.Map[String,String],ArrayBuffer[Dispatcher]] = {
    val lineArr = sqlStr.split("\n").map(l => l.trim()).filter(line => line!="")
    var jobdesc: String = null
    val result = new ArrayBuffer[String]()
    val dispatcherArray = new ArrayBuffer[Dispatcher]()
    val configs = scala.collection.mutable.HashMap[String,String]()
    
    lineArr.foreach(line => {
      if(line.length() < 2 || !line.substring(0, 2).equals("--")){
          result += line
          if(line.endsWith(";")){
            jobdesc = result.mkString(" ")
            val sqlWords = jobdesc.split(" ").filter(word => word!="").map(word => word.trim())
            //if set globle params
            if("set".equalsIgnoreCase(sqlWords(0))){
                configs ++= getProperties(jobdesc,"set")
            }else{
                val dispatcher = genDispatcher(jobdesc,sqlWords)
                dispatcherArray += dispatcher
            }
            result.clear()
          }
      }
    })
    (configs,dispatcherArray)
  }
  
  private def genDispatcher(jobdesc: String, sqlWords : Array[String]):Dispatcher = {
    val keyword = sqlWords(0)
    
    if("load".equalsIgnoreCase(keyword)){
        
      val idx = sqlWords(1).indexOf(".")
      val jobType = sqlWords(1).substring(0,idx).toLowerCase()
      val executor = jobType match {
        case "json" => nativeFileSource(jobdesc, sqlWords, "json")
        case "parquet" => nativeFileSource(jobdesc,sqlWords,"parquet")
        case "text" => nativeFileSource(jobdesc,sqlWords, "text")
        case "csv" => nativeFileSource(jobdesc,sqlWords, "csv")
        case "jdbc" => jdbcSource(jobdesc,sqlWords)
        case "hbase" => hbaseSource(jobdesc,sqlWords)
        case "excel" => excelSource(jobdesc,sqlWords)
      }
      
      new Dispatcher(executor, jobType)
      
    }else if ("save".equalsIgnoreCase(keyword)){
        
        val saveModel = sqlWords(1).toLowerCase() match{
            case "overwrite" => "Overwrite"
            case "append" => "Append"
            case "ignore" => "Ignore"
            case "errorIfExists" => "ErrorIfExists"
            case _ => null
        }
        
        var inputTable = sqlWords(2)
        if(saveModel == null) inputTable = sqlWords(1)
        val sinkType = getOutPutJobType(sqlWords)
        
        val infoBi = indexOf(jobdesc, "\"", 1)
        val infoEi = indexOf(jobdesc, "\"", 2)
        val sinkInfo = jobdesc.substring(infoBi + 1, infoEi)
        
        val configParams = scala.collection.mutable.Map("inputTableName" -> inputTable,
                "format" -> sinkType,"sinkInfo" -> sinkInfo)
        
        if(saveModel != null) configParams("saveModel") = saveModel
        
        val paramstr = jobdesc.substring(infoEi+1)
        configParams ++= getProperties(paramstr)
        
        
        val executor = sink(sinkType,configParams)
        executor.initialize(configParams - "implClass")
        
        new Dispatcher(executor, s"${sinkType}_sink")
      
    }else if("execute".equalsIgnoreCase(keyword)){
        
        val idx = sqlWords(1).indexOf(".")
        val jobType = sqlWords(1).substring(0,idx).toLowerCase()
        val executor = jobType match {
          case "sql" => hsql(jobdesc, sqlWords)
          case "scala" => scalacode(jobdesc, sqlWords)
          case "jdbcsql" => jdbcsql(jobdesc, sqlWords)
        }
        new Dispatcher(executor, jobType)
    }else if("check".equalsIgnoreCase(keyword)){
        
        val executor = check(jobdesc)
        new Dispatcher(executor, keyword)
    }else{
        null
    }
  }
  
  private def scalacode(jobdesc : String, sqlWords : Array[String]) :Executor = {
     val pathBi = indexOf(jobdesc, "\"", 1)
     val pathEi = indexOf(jobdesc, "\"", 2)
     val inputTableName = jobdesc.substring(pathBi + 1, pathEi)
     val configParams = scala.collection.mutable.Map("inputTable" -> inputTableName)
    
     getTempTableName(sqlWords) match{
       case Some(temptable) =>  configParams("outTableName")=temptable
       case None => 
     }
    
     val paramstr = jobdesc.substring(pathEi+1)
     configParams ++= getProperties(paramstr)
    
     val executor = Class.forName(ShortNameMapping.forName("check")).newInstance().asInstanceOf[Executor]
     executor.initialize(configParams)
     executor
  }
  
  /**
   * execute "insert overwrite table xxx select xcx from ffff " as hivetable;
   */
  private def hsql(jobdesc : String, sqlWords : Array[String]) :Executor = {
    val pathBi = indexOf(jobdesc, "\"", 1)
    val pathEi = indexOf(jobdesc, "\"", 2)
    val sql = jobdesc.substring(pathBi + 1, pathEi)
    val configParams = scala.collection.mutable.Map("sql" -> sql)
    
    val tmptablesection = jobdesc.substring(pathEi+1)
    
    getTempTableName(tmptablesection.split(" ").filter(w => w != "")) match{
      case Some(temptable) =>  configParams("outTableName")=temptable
      case None => 
    }
    
    val paramstr = jobdesc.substring(pathEi+1)
    configParams ++= getProperties(paramstr)
    
    val executor = Class.forName(ShortNameMapping.forName("hsql")).newInstance().asInstanceOf[Executor]
    executor.initialize(configParams)
    executor
  }
  
  /**
   * Execute sql with jdbc as the data source
   */
  private def jdbcsql(jobdesc : String, sqlWords : Array[String]) :Executor = {
      val pathBi = indexOf(jobdesc, "\"", 1)
      val pathEi = indexOf(jobdesc, "\"", 2)
      val sql = jobdesc.substring(pathBi + 1, pathEi)
      val configParams = scala.collection.mutable.Map("sql" -> sql)
    
      val paramstr = jobdesc.substring(pathEi+1)
      configParams ++= getProperties(paramstr)
    
      val executor = Class.forName(ShortNameMapping.forName("jdbcsql")).newInstance().asInstanceOf[Executor]
      executor.initialize(configParams)
      executor
  }
  

  /**
   * load csv.'/path/csvfilepath' with head="true" & delimiter="," & encoding="utf-8" as temp_table
   */
  private def nativeFileSource(jobdesc : String, sqlWords : Array[String], _type: String) :Executor = {
     val pathBi = indexOf(jobdesc, "\"", 1)
     val pathEi = indexOf(jobdesc, "\"", 2)
     val path = jobdesc.substring(pathBi + 1, pathEi)
     val configParams = scala.collection.mutable.Map("path" -> path,"format" -> _type)
    
    getTempTableName(sqlWords) match{
      case Some(temptable) =>  configParams("outTableName")=temptable
      case None => 
    }
    
    val paramstr = jobdesc.substring(pathEi+1)
    configParams ++= getProperties(paramstr)
    
    val executor = Class.forName(ShortNameMapping.forName(s"${_type}_source")).newInstance().asInstanceOf[Executor]
    executor.initialize(configParams)
    executor
  }
  
  /**
   * load jdbc."select * from table1" with driver="com.mysql.jdbc.Driver" & url="jdbc:mysql://192.168.150.242:3306/17win_activity?autoReconnect=true&user=activity&password=activity" & user = "" & password="" as temp_table
   */
  private def jdbcSource(jobdesc : String, sqlWords : Array[String]) :Executor = {
    val pathBi = indexOf(jobdesc, "\"", 1)
    val pathEi = indexOf(jobdesc, "\"", 2)
    val table = jobdesc.substring(pathBi + 1, pathEi)
    val configParams = scala.collection.mutable.Map("dbtable" -> table,"format" -> "jdbc")
    
    getTempTableName(sqlWords) match{
      case Some(temptable) =>  configParams("outTableName")=temptable
      case None => 
    }
    
    val paramstr = jobdesc.substring(pathEi+1)
    configParams ++= getProperties(paramstr)
    
    val executor = Class.forName(ShortNameMapping.forName("jdbc_source")).newInstance().asInstanceOf[Executor]
    executor.initialize(configParams)
    executor
  }
  
  
  private def esSink(sqlWords : String) :Executor = {
    null
  }
  
  /**
   * load hbase."dcs/table1" with hbase.zookeeper.quorum=127.0.0.1:2181 & hbase.zookeeper.property.clientPort=2181&zookeeper.znode.parent=/hbase-secure&field.type.filed=LongType
   */
  private def hbaseSource(jobdesc : String, sqlWords : Array[String]) :Executor = {
    val pathBi = indexOf(jobdesc, "\"", 1)
    val pathEi = indexOf(jobdesc, "\"", 2)
    val table = jobdesc.substring(pathBi + 1, pathEi)
    val configParams = scala.collection.mutable.Map("hbaseTableName" -> table)
    
    getTempTableName(sqlWords) match{
      case Some(temptable) =>  configParams("outTableName")=temptable
      case None => 
    }
    
    val paramstr = jobdesc.substring(pathEi+1)
    configParams ++= getProperties(paramstr)
    
    val executor = Class.forName(ShortNameMapping.forName("hbase_source")).newInstance().asInstanceOf[Executor]
    executor.initialize(configParams)
    executor
  }
  
  /**
   * 处理excel
   */
  private def excelSource(jobdesc : String, sqlWords : Array[String]) :Executor = {
    val pathBi = indexOf(jobdesc, "\"", 1)
    val pathEi = indexOf(jobdesc, "\"", 2)
    val table = jobdesc.substring(pathBi + 1, pathEi)
    val configParams = scala.collection.mutable.Map("path" -> table)
    
    getTempTableName(sqlWords) match{
      case Some(temptable) =>  configParams("outTableName")=temptable
      case None => 
    }
    
    val paramstr = jobdesc.substring(pathEi+1)
    configParams ++= getProperties(paramstr)
    
    val executor = Class.forName(ShortNameMapping.forName("excel_source")).newInstance().asInstanceOf[Executor]
    executor.initialize(configParams)
    executor
  }
  
  /**
   * Sink Executor
   */
  private def sink(sinkType: String,configParams:scala.collection.mutable.Map[String,String]) :Executor = {
      if(configParams.contains("implClass")){
           Class.forName(configParams("implClass")).newInstance().asInstanceOf[Executor]
      }else{
          
            Class.forName(ShortNameMapping.forName(s"${sinkType}_sink")).newInstance().asInstanceOf[Executor]
      }
  }
  
   private def check(jobdesc : String) :Executor = {
     val pathBi = indexOf(jobdesc, "\"", 1)
     val pathEi = indexOf(jobdesc, "\"", 2)
     val checkScript = jobdesc.substring(pathBi + 1, pathEi)
     val configParams = scala.collection.mutable.Map("sql" -> checkScript)
    
     val paramstr = jobdesc.substring(pathEi+1)
     configParams ++= getProperties(paramstr)
     
     val sqlWords = paramstr.split("\\s+").filter(w => w.trim()!="")
     
     configParams("operator")=sqlWords(0)
     configParams("rstTarget")=sqlWords(1)
     configParams("result")=if(sqlWords(2).endsWith(";")) sqlWords(2).dropRight(1) else sqlWords(2)
    
     val executor = Class.forName(ShortNameMapping.forName("check")).newInstance().asInstanceOf[Executor]
     executor.initialize(configParams)
     executor
  }
  
  
  def main(args: Array[String]): Unit = {
      val jobdesc = HDFSOperator.readFile("file://E:\\1.sql")
      val dispatcherArray = parshSqlFile(jobdesc)
      dispatcherArray.foreach(f => {
        println("---------------"+f)
      })
   }
  
}