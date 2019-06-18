package com.datatec.sparker.core

import com.datatec.sparker.utils.ParamsUtil
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.apache.spark.SparkConf
import java.util.{List => JList, Map => JMap}
import scala.collection.JavaConversions._
import org.apache.spark.sql.SparkSession
import java.lang.reflect.Modifier
import com.datatec.sparker.utils.HDFSOperator
import com.datatec.sparker.core.exe.Executor
import com.datatec.sparker.core.exe.source.NativeSourceExecutor
import com.datatec.sparker.common.ShellConfigConstants._
import com.datatec.sparker.utils.ParshSBASQL

object StartApp {
  val logger = LoggerFactory.getLogger(classOf[StartApp])
  
  def main(args: Array[String]): Unit = {
    
    val params = new ParamsUtil(args)
    
    require(params.hasParam(SPARKER_NAME), "Application name should be set")
    require(params.hasParam(JOBFILE_PATH), "Spaker job config file path should be set")
    
    logger.info("create Runtime...")
    
    //解析作业文件
    val jobFilePath = params.getParam(JOBFILE_PATH)
    
    val jobConfigStr = HDFSOperator.readFile(jobFilePath)
    
    val tupe = ParshSBASQL.dispatcher(jobConfigStr)
    //作业文件中的全局参数
    val configs = tupe._1
    
    // 合并shell和作业文件中的参数，如果参数同名，shell优先级 > jobfile
    val tempParams: JMap[String, String] =  configs ++ params.getParamsMap
    
    val sparkConf = new SparkConf()
    //入参中如果有spark或者hive开头的配置，则放到sparkconf中
    tempParams.filter(f =>
      f._1.toString.startsWith("spark.") || f._1.toString.startsWith("hive.")
    ).foreach { f =>
      sparkConf.set(f._1.toString, f._2.toString)
    }
    
    if(params.getParam(SPARKER_MASTER) != null){
      sparkConf.setMaster(params.getParam(SPARKER_MASTER))
    }
    
    sparkConf.setAppName(params.getParam(SPARKER_NAME, "SsparkkkkerAPP"))
    
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    
    // build SparkSession
    val builder = SparkSession.builder().config(sparkConf)
                                
                                
    if(params.getBooleanParam(ENABLE_HIVESUPPORT, true)){
        builder.enableHiveSupport()
    }
    
    val sparkSession = builder.getOrCreate()
                                
    
    if (params.hasParam(UDF_CLASSES)) {
      params.getParam(UDF_CLASSES).split(",").foreach { clzz =>
        registerUDF(sparkSession,clzz)
      }
    }
    
    val isLoggerParams = params.getBooleanParam(SPAKER_LOG_PARAMS, false)
    
    
    
    val dispatchers = tupe._2
    
    dispatchers.foreach(dispatcher => {
        logger.info(s"=================executor job ${dispatcher._type}===========================")
        val exe = dispatcher._job
        //打印参数
        if(isLoggerParams) exe.showParams()
        exe.execute(tempParams, sparkSession)
    })
    
    sparkSession.stop
  }
  
  
  def registerUDF(sparkSession: SparkSession, clzz: String) = {
    logger.info(s"register functions.....${clzz}")
    Class.forName(clzz).getMethods.foreach { f =>
      try {
        if (Modifier.isStatic(f.getModifiers)) {
          f.invoke(null, sparkSession.udf)
        }
      } catch {
        case e: Exception =>
          e.printStackTrace()
      }
    }

  }
}

class StartApp