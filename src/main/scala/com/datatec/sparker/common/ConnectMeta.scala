package com.datatec.sparker.common

import java.util.concurrent.ConcurrentHashMap

object ConnectMeta {
  //dbName -> (format->jdbc,url->....)
  private val dbMapping = new ConcurrentHashMap[DBMappingKey, Map[String, String]]()

  def options(key: DBMappingKey, _options: Map[String, String]) = {
    dbMapping.put(key, _options)
  }

  def options(key: DBMappingKey) = {
    if (dbMapping.containsKey(key)) {
      Option(dbMapping.get(key))
    } else None
  }

  def presentThenCall(key: DBMappingKey, f: Map[String, String] => Unit) = {
    if (dbMapping.containsKey(key)) {
      val item = dbMapping.get(key)
      f(item)
      Option(item)
    } else None
  }
}

case class DBMappingKey(format: String, db: String)