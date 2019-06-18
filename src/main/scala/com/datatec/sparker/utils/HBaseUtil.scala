package com.datatec.sparker.utils

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.ConnectionFactory


object HBaseUtil {
  
    def getConn(_cfg: Map[String, String]) = {
         val hbaseConf = HBaseConfiguration.create()
         
         import scala.collection.JavaConversions._
         if (_cfg.containsKey("zk") || _cfg.containsKey("hbase.zookeeper.quorum")) {
            hbaseConf.set("hbase.zookeeper.quorum", _cfg.getOrElse("zk", _cfg.getOrElse("hbase.zookeeper.quorum", "127.0.0.1:2181")))
         }
      
         hbaseConf.set("zookeeper.znode.parent", _cfg.getOrElse("zookeeper.znode.parent", "/hbase-unsecure"))
         hbaseConf.set("hbase.zookeeper.property.clientPort", _cfg.getOrElse("hbase.zookeeper.property.clientPort", "2181"))
         
         ConnectionFactory.createConnection(hbaseConf)
    }
}