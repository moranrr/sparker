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
import com.datatec.sparker.core.exe.BaseExecutor

class HBaseSinkExecutor extends BaseExecutor{
  
  
     val dbSplitter: String = ":"
     
     override def process(shellParams: Map[String,String], sparkSession: SparkSession){
         
          
         //参数替换，如果某个参数的值是value,则shell脚本中的参数名是-sparker.param.value=realvalue
         val _cfg = _configParams.map { f =>{
              (f._1, shellParams.getOrElse(s"sparker.params.${f._2}", f._2))
            }
         }.toMap
         
         import scala.collection.JavaConversions._
         
      
         val hbaseConf = HBaseConfiguration.create()
         if (_cfg.containsKey("zk") || _cfg.containsKey("hbase.zookeeper.quorum")) {
            hbaseConf.set("hbase.zookeeper.quorum", _cfg.getOrElse("zk", _cfg.getOrElse("hbase.zookeeper.quorum", "127.0.0.1:2181")))
         }
      
         hbaseConf.set("zookeeper.znode.parent", _cfg.getOrElse("zookeeper.znode.parent", "/hbase-unsecure"))
         hbaseConf.set("hbase.zookeeper.property.clientPort", _cfg.getOrElse("hbase.zookeeper.property.clientPort", "2181"))
        
        val rowkey = _cfg.getOrElse("rowkey", "rowkey")
        val family = _cfg.getOrElse("family", "info")
        
        val outputTableName = _cfg("sinkInfo")
        
        hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, outputTableName)
        
        val job = Job.getInstance(hbaseConf)
        job.setOutputFormatClass(classOf[TableOutputFormat[String]])
        val jobConfig = job.getConfiguration
        val tempDir = SparkerUtils.createTempDir()
        if (jobConfig.get("mapreduce.output.fileoutputformat.outputdir") == null) {
            jobConfig.set("mapreduce.output.fileoutputformat.outputdir", tempDir.getPath + "/outputDataset")
        }
        
        val inputTableName = _cfg("inputTableName")
        val outputFileNum = _cfg.getOrElse("fileNum", "-1").toInt
        var newTableDF = sparkSession.table(inputTableName)
        if (outputFileNum != -1) {
            newTableDF = newTableDF.repartition(outputFileNum)
        }
        
        val fields = newTableDF.schema.toArray
        val rowkeyIndex = fields.zipWithIndex.filter(f => f._1.name == rowkey).head._2
        val otherFields = fields.zipWithIndex.filter(f => f._1.name != rowkey)
    
        val rdd = newTableDF.rdd //df.queryExecution.toRdd
        
        //HFileOutputFormat.configureIncrementalLoad (job, table)
    
        rdd.map(row => {
    
          val put = new Put(Bytes.toBytes(row.getString(rowkeyIndex)))
          otherFields.foreach { field =>
            if (row.get(field._2) != null) {
              val st = field._1
              val datatype = if (_cfg.contains(s"field.type.${st.name}")) {
                  //StringType
                  val name = _cfg(s"field.type.${st.name}")
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
              } else st.dataType
              
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
    
    
    
              put.addColumn(Bytes.toBytes(family), Bytes.toBytes(field._1.name), colValue)
            }
          }
          (new ImmutableBytesWritable, put)
        }).saveAsNewAPIHadoopDataset(job.getConfiguration)
     }
}