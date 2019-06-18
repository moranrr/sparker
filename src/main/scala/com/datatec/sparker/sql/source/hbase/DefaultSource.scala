package com.datatec.sparker.sql.source.hbase

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.RelationProvider
 
 
class DefaultSource extends RelationProvider {
  def createRelation(sqlContext: SQLContext, parameters: Map[String, String]) = {
    HBaseRelation(parameters)(sqlContext)
  }
}