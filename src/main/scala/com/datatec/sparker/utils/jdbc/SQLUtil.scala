package com.datatec.sparker.utils.jdbc

import java.util

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer


/**
 * 解析sql語句
 * @author: Chao.Qian1
 * @date:   2018年6月10日 上午10:11:02   
 *
 */
object SQLUtil {



    /**
   * 解析sql中占位符 ？ 对应的列名
   * 
   * 
   */
 def parshSqlColumns(sql:String):ArrayBuffer[String] = {
    val charArray = sql.toCharArray()
    var colArray = ArrayBuffer[String]()
    var i = 0
    for(char <- charArray){
      if(char.toString == "?"){
        //向前查询第一个不是空格的字符的位置
         val ffct  =  parshFront(i, charArray, "")
         //1、如果字符是等号
         if(ffct._2.equals("=")){
           //从等号开始向前查询第一个不是空格的字符的位置
           val fsct1 = parshFront(ffct._1, charArray, "")
           //从上面查询的字符起 向前查询第一个空格的位置
           val fsct2 = parshFront(fsct1._1, charArray, " ")
           if(!fsct2._2.equals("NONE")){
	           val col = sql.substring(fsct2._1, fsct1._1+1)
	           colArray+=col.trim()
           }
         }
         //2、values(?,?,?...)
         else if(ffct._2.equals("(")){
           //查找'('前面的一个单词
            //从等号开始向前查询第一个不是空格的字符的位置
           val fsct1 = parshFront(ffct._1, charArray, "")
           //从上面查询的字符起 向前查询第一个空格的位置
           val fsct2 = parshFront(fsct1._1, charArray, " ")
           if(!fsct2._2.equals("NONE")){
	           val statval = sql.substring(fsct2._1, fsct1._1+1)
	           //println(statval)
	           //如果是values，区别是函数的情况to_date(?,'yyyy-MM-dd hh24:mi:ss')
	           if("values".equalsIgnoreCase(statval.trim())){
	               val backCloseBracketsPos = foundValuesCloseBrackets(i, charArray)
	               val placeholders = sql.substring(ffct._1+1, backCloseBracketsPos).split(",").map(str => str.trim())
	                 
	               val frontCloseP = parshFront(ffct._1, charArray, ")")
		           val frontOpenP = parshFront(frontCloseP._1, charArray, "(")
		           if(!frontOpenP._2.equals("NONE")){
			           val insertCols = sql.substring(frontOpenP._1+1, frontCloseP._1).split(",").map(str => str.trim())
			           var k = 0
			           for(phold <- placeholders){
			              if(phold.equals("?") || phold.contains("?")){
			                colArray+=insertCols(k)
			                k+=1
			              }else if(!phold.endsWith(")")){
			                k+=1
			              }
			           }
		           }
	           }
           }
         }else{
           //如果不是等号也不是括号，那就对应oracle merge into的select ? as asl1,? as col2, ? col3 from table的情况
           val fbct1  = parshBackWard(i, charArray, "")
           if(!fbct1._2.equals(",") && !fbct1._2.equals(")") && !fbct1._2.equals("NONE")){
               var j = fbct1._1-1
	           if(fbct1._2.equals("a")){
	             //处理占位符后as的情况
	             val fbct2 = parshBackWard(fbct1._1, charArray, "s")
	             val fbct3 = parshBackWard(fbct1._1, charArray, " ")
	             if(fbct2._1 - fbct1._1 == 1 && fbct3._1 - fbct2._1 == 1){
	               j = fbct3._1
	             }
	           } 
	           
	           val col = parshCol(j,charArray,sql)
	           colArray+=col.trim()
           }
         }
      }
      i+=1
    }
    
    colArray
  }
  
  /**
   * 查找values的闭括号结束位置
   */
  def foundValuesCloseBrackets(i:Int, charArray:Array[Char]):Int = {
       //查找占位符之后出现的第一个开括号
	   val backOpenBrackets = parshBackWard(i, charArray, "(")
	   //查找占位符之后出现的第一个闭括号
	   val backCloseBrackets = parshBackWard(i, charArray, ")")
	   //如果后面没有开括号，或者开括号出现的位置在闭括号之后
	   if(backOpenBrackets._2.equals("NONE") || backOpenBrackets._1 > backCloseBrackets._1){
	      backCloseBrackets._1
	   }else{
	     //否则标识values的括号里嵌套着括号，继续向后查找
	     foundValuesCloseBrackets(backCloseBrackets._1,charArray)
	   }
  }
 
  def parshCol(i:Int,charArray:Array[Char],sql:String):String = {
    
    val commaT = parshBackWard(i, charArray, ",")
    val spaceT = parshBackWard(i, charArray, " ")
    if(commaT._1 > spaceT._1){
      sql.substring(i, spaceT._1)
    }else{
      sql.substring(i, commaT._1)
    }
  }
  
  def parshFront(i:Int,charArray:Array[Char],schar:String ):Tuple2[Int,String] = {
    val j = i-1
    if(i == 0)  {
        (0,"NONE")
    }else{
      val char = charArray(j).toString
     
      schar match{
        case "" => if(char.trim().equals("")){ parshFront(j, charArray,schar) }else{ (j, char) }
        case _ => if(schar.equals(char)){ (j, char) }else{parshFront(j, charArray,schar)}
      }
    }
    
  }
  
  def parshBackWard(i:Int,charArray:Array[Char],schar:String ):Tuple2[Int,String] = {
    val j = i+1
    if(j == charArray.length)  {
        (i,"NONE")
    }else{
      val char = charArray(j).toString
      schar match{
        case "" => if(char.trim().equals("")){ parshBackWard(j, charArray,schar) }else{ (j, char) }
        case _ => if(schar.equals(char)){ (j, char) }else{parshBackWard(j, charArray,schar)}
      }
    } 
  }
}