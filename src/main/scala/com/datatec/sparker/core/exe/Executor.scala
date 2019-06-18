package com.datatec.sparker.core.exe

import scala.collection.Map
import org.apache.spark.sql.SparkSession

trait Executor {
  
  def initialize(configParams: Map[String,String])
  
  protected def process(shellParams: Map[String,String], sparkSession: SparkSession)
  
  def execute(shellParams: Map[String,String], sparkSession: SparkSession)
  
  def showParams()
  
}