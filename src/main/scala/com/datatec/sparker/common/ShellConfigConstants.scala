package com.datatec.sparker.common

object ShellConfigConstants {
    /**
     * 在yarn中显示的任务名称
     */
    val SPARKER_NAME = "sparker.name"
    
    /**
     * 定义任务的文件路径
     * 本地文件以"file://"开头，hdfs文件以"hdfs://"开头
     */
    val JOBFILE_PATH = "sparker.job.file.path"
  
    val SPARKER_MASTER = "sparker.master"
    
    /**
     * 自定义函数的全路径类名，多个用逗号分隔
     */
    val UDF_CLASSES = "sparker.udf.clzznames"
    
    val SPARKER_PARAMS_PREFIX = "sparker.params."
    
    val SQL_PARAMS_PREFIX    = "sparker.sql.params."
    
    /**
     * 是否打印executor参数
     */
    val SPAKER_LOG_PARAMS    = "sparker.log.params"
    
    val ENABLE_HIVESUPPORT = "sparker.enableHiveSupport"
}