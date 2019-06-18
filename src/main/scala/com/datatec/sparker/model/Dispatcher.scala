package com.datatec.sparker.model

import com.datatec.sparker.core.exe.Executor

class Dispatcher (job: Executor, typee: String){
   var next: Dispatcher = null
   val _job = job
   val _type = typee
}
