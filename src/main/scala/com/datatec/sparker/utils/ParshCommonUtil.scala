package com.datatec.sparker.utils

import scala.util.control.Breaks._
import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
import scala.Char
import java.util.Properties
import java.util.Map.Entry
import scala.collection.JavaConversions._

object ParshCommonUtil {
  
  
  /**
   * 截取以第一个字符chr开始和后一个字符chr结束的内容
   * 转义符为"\\"
   */
  def getCommentFE(str: String,chr:String) : String = {
    val charArray = str.toCharArray()
    var bi = indexOf(str, chr, 1)
    var ei = indexOf(str, chr, 2)
    if(ei == -1) ei = str.length
    str.substring(bi+1, ei)
  }
  
  /**
   * 字符chr在字符串str中第times出现的位置
   */
  def indexOf(str:String, chr:String, times:Int) : Int = {
    val charArray = str.toCharArray()
    var frq = 0
    var idx = 0
    for(char <- charArray){
      if(char.toString().equalsIgnoreCase(chr) &&  (idx == 0 || !charArray(idx-1).toString().equals("\\"))){
        frq += 1
        if(frq == times){
          return idx
        }
      }
      idx += 1
    }
    
    -1
  }
  
  /**
   * xxx as temp_table
   * 从后向前找到as，然后取后面的一个单词
   */
  def getTempTableName(sqlWords : Array[String]) : Option[String] = {
    var index = sqlWords.length - 1
    while(index >= 0){
      if("as".equalsIgnoreCase(sqlWords(index))){
         val temptable= sqlWords(index + 1)
         if(temptable.endsWith(";")){
            return Some(temptable.replace(";", ""))
         }else{
            return Some(temptable)
         }
      }
      index -= 1
    }
    None
  }
  
  
  def getOutPutJobType(sqlWords : Array[String]) : String = {
      var length = sqlWords.length
      
      for (i  <- 0 until length){
          if("as".equalsIgnoreCase(sqlWords(i))){
              var jobType = sqlWords(i + 1)
              val idx = jobType.indexOf(".")
              if(idx != -1){
                  jobType = jobType.substring(0,idx).toLowerCase()
              }
              return jobType
          }
      }
      
     ""
  }
  
  /**
   * with head=true  &delimiter=,&ncoding=utf-8  &    oth=" a bc d "
   */
  def getProperties(jobdesc: String, flag:String = "with") : Map[String,String] = {
    val withIdx = jobdesc.indexOf(flag)
    val configParams = scala.collection.mutable.Map[String,String]()
    if(withIdx == -1){
      configParams
    }else{
      val paramstr = jobdesc.substring(withIdx + 4)
      
     
      val charArray = paramstr.toCharArray()
      var idx = 0
      var key:String = null
      var value:String = null
      //是否在引号内
      var flag = false
      val tempArray = new ArrayBuffer[String]()
      
      for(char <- charArray){
        //如果当前字符是双引号，并且前一个字符不是转义符
        if(char.toString() == "\"" && !charArray(idx-1).toString().equals("\\")){
          //如果之前出现过一个引号，并且这个是结束的引号，则截取value
          if(flag){
            value=tempArray.mkString("").trim()
            if(key != null) configParams(key) = value
            key=null
            value=null
            tempArray.clear()
          }
          flag = !flag
        } else if(!flag && char.toString() == "="){
          //如果是等号，并且不在引号内，则前面的字符是key
          key = tempArray.mkString("").trim()
          tempArray.clear()
        }else if(!flag && char.toString() == "&" ){
          //如果当前字符是&，并且不在引号内，则前面的字符是value
          value=tempArray.mkString("").trim()
          if(key != null) configParams(key) = value
          key=null
          value=null
          tempArray.clear()
        }else if(idx == charArray.length -1){
          //如果当前是结尾
          if(char.toString != ";") tempArray += char.toString
          
          value=tempArray.mkString("").trim()
          if(key != null) configParams(key) = value
          key=null
          value=null
          tempArray.clear()
        }else if (!flag && char.toString() == " " && key!=null && tempArray.length > 0 ){
            value=tempArray.mkString("").trim()
            configParams(key) = value
            key=null
            value=null
            tempArray.clear()
        } else{
          tempArray += char.toString
        }
        
        idx+=1
      }
      
      configParams.get("paramsFile") match{
          case Some(filePath) => {
              val prop = new Properties()
              prop.load(HDFSOperator.readAsInputStream(filePath))
              val fileParamMap:Map[String,String] = prop.toMap
              fileParamMap ++ configParams - "paramsFile"
          }
          case None =>  configParams
      }
      
    }
    /*val sigleParams = paramstr.split("&").filter(w => w != "")
    
    val configParams = scala.collection.mutable.Map[String,String]()
    
    sigleParams.foreach(p => {
      //withpath ' with a=1 & b="1 2 3" & c =  true as fff
       val eqIdx = p.indexOf("=")
       if(eqIdx > 0){
         val key = p.substring(0, eqIdx).split(" ")
         val value = getCommentFE(p.substring(eqIdx+1), "\"").trim()
         configParams(key(key.length - 1)) = value
       }
    })
    
    configParams*/
      
  }
  
   def main(args: Array[String]): Unit = {
      val jobdesc = HDFSOperator.readFile("file://E:\\1.sql")
      val pathBi = indexOf(jobdesc, "\"", 1)
      val pathEi = indexOf(jobdesc, "\"", 2)
      val table = jobdesc.substring(pathBi + 1, pathEi)
      

      val paramstr = jobdesc.substring(pathEi+1)
      val map = getProperties(paramstr)
      
      for((key,value) <- map)println(key + ":" + value)
      
   }
}