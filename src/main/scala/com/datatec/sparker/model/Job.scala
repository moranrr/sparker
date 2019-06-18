package com.datatec.sparker.model

import scala.collection.Map
import com.datatec.sparker.core.exe.Executor

case class Job (executor: Executor,params: Map[String, String])