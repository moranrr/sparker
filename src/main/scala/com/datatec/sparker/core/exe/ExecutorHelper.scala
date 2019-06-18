package com.datatec.sparker.core.exe

import scala.collection.Map
import com.datatec.sparker.common.ShellConfigConstants._

trait ExecutorHelper {
  
  def translateSQL(_sql: String, params: Map[String,String]) = {
    var sql: String = _sql
    params.filter(_._1.startsWith(SQL_PARAMS_PREFIX)).foreach { p =>
      val key = p._1.split("\\.").last
      sql = sql.replaceAll(":" + key, p._2)
    }
    sql
  }
}