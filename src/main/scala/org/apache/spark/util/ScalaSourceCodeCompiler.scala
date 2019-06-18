package org.apache.spark.util

import com.google.common.cache.{CacheBuilder, CacheLoader}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StructType

import scala.tools.nsc.GenericRunnerSettings
import scala.tools.nsc.interpreter.IMain
import scala.tools.reflect.ToolBox

/**
 * 自定义scala 脚本模板类
 */
trait ScalaScriptTemplateClass {
  
  def execute(rawLine: String): Map[String, Any] = {
    Map[String, Any]()
  }
  
  def execute(rowmap: Map[String, Any]): Map[String, Any] = {
    Map[String, Any]()
  }
  
  def schema(): Option[StructType] = {
    None
  }
  
  def execute(context: SQLContext): Unit = {

  }
}


case class ScriptCacheKey(prefix: String, code: String)

object ScalaSourceCodeCompiler {
   def generateUDScalaClass(scriptCacheKey: ScriptCacheKey) = {
    val startTime = System.nanoTime()
    val function1 = if (scriptCacheKey.prefix == "rawLine") {
      s"""
         |override  def execute(rawLine:String):Map[String,Any] = {
         | ${scriptCacheKey.code}
         |}
         """.stripMargin
    } else ""

    val function2 = if (scriptCacheKey.prefix == "doc") {
      s"""
         |override  def execute(rowmap:Map[String,Any]):Map[String,Any] = {
         | ${scriptCacheKey.code}
         |}
        """.stripMargin
    } else ""


    val function3 = if (scriptCacheKey.prefix == "schema") {
      s"""
         |override  def schema():Option[StructType] = {
         | ${scriptCacheKey.code}
         |}
       """.stripMargin
    } else ""

    val function4 = if (scriptCacheKey.prefix == "context") {
      s"""
         |override  def execute(context: SQLContext): Unit = {
         |
                                                           | ${scriptCacheKey.code}
         |}
         """.stripMargin
    } else ""


    val wrapper =
      s"""
         |import org.apache.spark.util.ScalaScriptTemplateClass
         |import org.apache.spark.sql.SQLContext
         |import org.apache.spark.sql.types._
         |class StreamingProUDF_${startTime} extends ScalaScriptTemplateClass {
         |
         | ${function1}
         |
         | ${function2}
         |
         | ${function3}
         |
         | ${function4}
         |
         |}
         |new StreamingProUDF_${startTime}()
            """.stripMargin

    val result = compileCode(wrapper)

    result.asInstanceOf[ScalaScriptTemplateClass]
  }
   
    def compileCode(code: String): Any = {
        import scala.reflect.runtime.universe._
        val cm = runtimeMirror(Utils.getContextOrSparkClassLoader)
        val toolbox = cm.mkToolBox()
        val tree = toolbox.parse(code)
        val ref = toolbox.compile(tree)()
        ref
    }
    
    private val scriptCache = CacheBuilder.newBuilder()
        .maximumSize(10000)
        .build(
          new CacheLoader[ScriptCacheKey, ScalaScriptTemplateClass]() {
            override def load(scriptCacheKey: ScriptCacheKey): ScalaScriptTemplateClass = {
              val startTime = System.nanoTime()
              val res = generateUDScalaClass(scriptCacheKey)
              val endTime = System.nanoTime()
              def timeMs: Double = (endTime - startTime).toDouble / 1000000
              res
            }
          })

     def getExecute(scriptCacheKey: ScriptCacheKey): ScalaScriptTemplateClass = {
        scriptCache.get(scriptCacheKey)
     }
}