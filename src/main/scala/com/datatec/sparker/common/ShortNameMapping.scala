package com.datatec.sparker.common

object ShortNameMapping {
  
   private val executorNameMap: Map[String, String] = Map[String, String](
    "hbase_source" -> "com.datatec.sparker.core.exe.source.HBaseSourceExecutor",
    "json_source" -> "com.datatec.sparker.core.exe.source.NativeSourceExecutor",
    "csv_source" -> "com.datatec.sparker.core.exe.source.NativeSourceExecutor",
    "parquet_source" -> "com.datatec.sparker.core.exe.source.NativeSourceExecutor",
    "jdbc_source" -> "com.datatec.sparker.core.exe.source.NativeSourceExecutor",
    "text_source" -> "com.datatec.sparker.core.exe.source.NativeSourceExecutor",
    "excel_source" -> "com.datatec.sparker.core.exe.source.ExcelSourceExecutor",
    "hsql" -> "com.datatec.sparker.core.exe.impl.HSqlExecutor",
    "scalacode" -> "com.datatec.sparker.core.exe.impl.ScalaExecutor",
    "jdbcsql" -> "com.datatec.sparker.core.exe.sink.InsertSqlSinkExecutor",
    "json_sink" -> "com.datatec.sparker.core.exe.sink.SparkNativeSinkExecutor",
    "csv_sink" -> "com.datatec.sparker.core.exe.sink.SparkNativeSinkExecutor",
    "parquet_sink" -> "com.datatec.sparker.core.exe.sink.SparkNativeSinkExecutor",
    "jdbc_sink" -> "com.datatec.sparker.core.exe.sink.SparkNativeSinkExecutor",
    "text_sink" -> "com.datatec.sparker.core.exe.sink.SparkNativeSinkExecutor",
    "hive_sink" -> "com.datatec.sparker.core.exe.sink.SparkNativeSinkExecutor",
    "sql_sink" -> "com.datatec.sparker.core.exe.sink.InsertSqlSinkExecutor",
    "es_sink" -> "com.datatec.sparker.core.exe.sink.ElasticsearchSinkExecutor",
    "excel_sink" -> "com.datatec.sparker.core.exe.sink.ExcelSinkExecutor",
    "hbase_sink" -> "com.datatec.sparker.core.exe.sink.HBaseSinkExecutor",
    "console_sink" -> "com.datatec.sparker.core.exe.sink.ConsoleSinkExecutor",
    "check" -> "com.datatec.sparker.core.exe.impl.CheckExecutor"
    
  )

  def forName(shortName: String): String = {
    if (executorNameMap.contains(shortName)) {
      executorNameMap(shortName)
    } else {
      shortName
    }
  }
}