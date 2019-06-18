package com.datatec.sparker.udf

import org.apache.spark.sql.UDFRegistration
import java.util.UUID

object Functions {
  
    def uuid(uDFRegistration: UDFRegistration) = {
        uDFRegistration.register("uuid", () => {
           UUID.randomUUID().toString.replace("-", "")
        })
   }
}