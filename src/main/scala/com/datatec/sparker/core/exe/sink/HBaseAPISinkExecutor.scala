package com.datatec.sparker.core.exe.sink

import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, Result}
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import java.util
import org.apache.spark.sql.types._
import scala.collection.Map

import org.apache.log4j.Logger
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.hadoop.hbase.util.Bytes
import org.joda.time.DateTime
import org.apache.spark.util.SparkerUtils
import org.apache.hadoop.hbase.TableName
import com.datatec.sparker.utils.HBaseUtil
import com.datatec.sparker.core.exe.BaseExecutor

class HBaseAPISinkExecutor extends BaseExecutor{
  
  
     
     override def process(shellParams: Map[String,String], sparkSession: SparkSession){
         
          
         //参数替换，如果某个参数的值是value,则shell脚本中的参数名是-sparker.param.value=realvalue
         val _cfg = _configParams.map { f =>{
              (f._1, shellParams.getOrElse(s"sparker.params.${f._2}", f._2))
            }
         }.toMap
         
         import scala.collection.JavaConversions._
         
      
        
        val rowkey = _cfg.getOrElse("rowkey", "rowkey")
        val family = _cfg.getOrElse("family", "info")
        
        val outputTableName = _cfg("sinkInfo")
        
        
        val inputTableName = _cfg("inputTableName")
        val outputFileNum = _cfg.getOrElse("fileNum", "-1").toInt
        var newTableDF = sparkSession.table(inputTableName)
        if (outputFileNum != -1) {
            newTableDF = newTableDF.repartition(outputFileNum)
        }
        
        val fields = newTableDF.schema.toArray
        val rowkeyIndex = fields.zipWithIndex.filter(f => f._1.name == rowkey).head._2
        val otherFields = fields.zipWithIndex.filter(f => f._1.name != rowkey)
    
        val inserCount = sparkSession.sparkContext.longAccumulator("fooCount")
        
        newTableDF.foreachPartition(it => {
            val conn = HBaseUtil.getConn(_cfg)
            
            val insertTable = conn.getTable(TableName.valueOf(outputTableName))
 
            var list = new java.util.ArrayList[Put]
            
            it.foreach(row => {
                val put = new Put(Bytes.toBytes(row.getString(rowkeyIndex)))
                inserCount.add(1)
                otherFields.foreach { field =>
                    //field._2是下标
                    if (row.get(field._2) != null) {
                          val structFiled = field._1
                          val datatype = if (_cfg.contains(s"field.type.${structFiled.name}")) {
                              //StringType
                              val name = _cfg(s"field.type.${structFiled.name}")
                              name match {
                                 case "FloatType" => DataTypes.StringType
                                 case "DoubleType" => DataTypes.DoubleType
                                 case "LongType" => DataTypes.LongType
                                 case "IntegerType" => DataTypes.IntegerType
                                 case "BooleanType" => DataTypes.BooleanType
                                 case "DateType" => DataTypes.DateType
                                 case "BinaryType" => DataTypes.BinaryType
                                 case _ => DataTypes.StringType
                              }
                          } else structFiled.dataType
                          
                          val colValue = datatype match {
                            case StringType => Bytes.toBytes(row.getString(field._2))
                            case FloatType => Bytes.toBytes(row.getFloat(field._2))
                            case DoubleType => Bytes.toBytes(row.getDouble(field._2))
                            case LongType => Bytes.toBytes(row.getLong(field._2))
                            case IntegerType => Bytes.toBytes(row.getInt(field._2)) 
                            case BooleanType => Bytes.toBytes(row.getBoolean(field._2))
                            case DateType => Bytes.toBytes(new DateTime(row.getDate(field._2)).getMillis)
                            case TimestampType => Bytes.toBytes(new DateTime(row.getTimestamp(field._2)).getMillis)
                            case BinaryType => row.getAs[Array[Byte]](field._2)
                            //            case ArrayType => Bytes.toBytes(row.getList(field._2).mkString(","))
                            //            case DecimalType.BigIntDecimal => Bytes.toBytes(row.getDecimal(field._2))
                            case _ => Bytes.toBytes(row.getString(field._2))
                          }
                
                
                          put.addColumn(Bytes.toBytes(family), Bytes.toBytes(structFiled.name), colValue)
                    }
                }
                
                list.add(put)
                if (inserCount.value % 10000 == 0) {
                    insertTable.put(list)
                    list.clear()
                }
            })
            
            insertTable.put(list)
            
            insertTable.close()
            conn.close()
        })
     }
}