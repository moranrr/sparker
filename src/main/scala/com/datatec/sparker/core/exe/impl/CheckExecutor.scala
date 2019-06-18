package com.datatec.sparker.core.exe.impl

import scala.collection.Map

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import com.datatec.sparker.core.exe.Executor
import com.datatec.sparker.core.exe.ExecutorHelper
import com.datatec.sparker.email.EmailSender
import com.datatec.sparker.core.exe.BaseExecutor

/**
 * 
 * @ClassName:  CheckExecutor   
 * @Description:TODO
 * @author: Chao.Qian1
 * @date:   2019年6月14日 下午10:11:02   
 *
 */
class CheckExecutor  extends BaseExecutor{
  
  
  
  override def process(_commonParams: Map[String,String], sparkSession: SparkSession){
    
     //参数替换，如果某个参数的值是value,则shell脚本中的参数名是-sparker.param.value=realvalue
     val _cfg = _configParams.map { f =>{
           (f._1, _commonParams.getOrElse(s"sparker.param.${f._2}", f._2))
        }
     }.toMap
     
     val script = _cfg("sql")
     
     val sql_script = translateSQL(script, _commonParams)
     
     var df = sparkSession.sql(sql_script)
     
     val checkValue = df.collect()(0).get(0).asInstanceOf[Long]
     
     val rstTarget=_cfg("rstTarget").toLong
     
     val operator = _cfg("operator")
     
     val checkResult = operator match{
         case ">=" => checkValue >= rstTarget
         case "<=" => checkValue <= rstTarget
         case "=" => checkValue == rstTarget
         case ">" => checkValue > rstTarget
         case "<" => checkValue < rstTarget
         case "!=" => checkValue != rstTarget
         case _ => true
     }
     
     if(checkResult) {
         val result = _cfg("result").toLowerCase()
         result match {
             case "abort" => sys.error(s"[${sql_script}] result is ${checkValue} ${operator} ${rstTarget}")
             case "warn" =>  {
                 val param2Mail = _cfg.map(f => f._1 + " : " +f._2)
                 val content = param2Mail.mkString("\n") + "\n" + s"Result Value IS: ${checkValue}"
                 sendEmail(_commonParams, "数据质量检测警告", content)
             }
             case _ => 
         }
     }
     
  }
  
}