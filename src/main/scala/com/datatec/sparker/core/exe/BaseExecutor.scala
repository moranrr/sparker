package com.datatec.sparker.core.exe

import scala.collection.Map
import org.slf4j.LoggerFactory
import org.apache.spark.sql.SparkSession
import com.datatec.sparker.email.EmailSender
import scala.util.control.Breaks.{break, breakable}

/**
 * @author: Chao.Qian1
 * @date:   2019年6月12日 上午10:11:02   
 *
 */
abstract class BaseExecutor  extends Executor with ExecutorHelper{
    
    val logger = LoggerFactory.getLogger(classOf[BaseExecutor])
    
    protected var _configParams: Map[String,String] = _
  
    override  def initialize(configParams: Map[String,String]){
      this._configParams = configParams
    }
    
    override  def execute(_commonParams: Map[String,String], sparkSession: SparkSession) {
        var retryNum = _configParams.getOrElse("retry", _commonParams.getOrElse("retry", "1")).toInt
        //如果用户设置了重试次数小于等于0  则重新赋值为1
        if(retryNum <= 0)  retryNum = 1
        
        val filureStrategy = _configParams.getOrElse("filureStrategy", "abort").toLowerCase()
        
        breakable{
            while(retryNum > 0){
                try{
                    //println(s"======================================try : ${retryNum}")
                    retryNum -= 1
                    process(_commonParams, sparkSession)
                    break
                } catch {
                    case e: Exception => if(retryNum == 0){
                       filureStrategy match {
                           case "continue" => e.printStackTrace();sendEmail(_commonParams, "spaker作业执行出错！！！！",null)
                           case _ => throw e
                       }
                    }
                }
            }
        }
    }
    
    override def showParams(){
       logger.info("--------============parameter is :=============---------")
       for((key,value) <- _configParams) {
           logger.info(s"| ${key}    |    ${value}  |" )
           logger.info("------------------------------------")
       }
    }
    
    
    protected def sendEmail(_commonParams: Map[String,String],title: String,content: String){
        try{
             val sender = _commonParams.getOrElse("mailSender",_configParams.getOrElse("mailSender",""))
             val pwd = _commonParams.getOrElse("password",_configParams.getOrElse("password",""))
             val smtpHost = _commonParams.getOrElse("smtpHost",_configParams.getOrElse("smtpHost",""))
             val emailAddress = _commonParams.getOrElse("toMails",_configParams.getOrElse("toMails",""))
             if(sender != "" && pwd != "" && smtpHost!="" && emailAddress!=""){
                 val mailSender = EmailSender(sender,pwd,smtpHost)
                 val param2Mail = _configParams.map(f => f._1 + " : " +f._2)
                 var content1 = content
                 if(content == null || "" == content)
                     content1 = param2Mail.mkString("<br/>\n")
                 
                 mailSender.sendMail(emailAddress.split(","), title, content1)
             }
        } catch {
                case e: Exception => e.printStackTrace()
        }
    }
    
}