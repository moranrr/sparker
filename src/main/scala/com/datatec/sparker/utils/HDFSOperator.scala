package com.datatec.sparker.utils

import java.io.{BufferedReader, ByteArrayOutputStream, InputStream, InputStreamReader}
import java.io.File

import org.apache.hadoop.fs.FileSystem
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path,FSDataInputStream,FileStatus,FSDataOutputStream}
import org.apache.hadoop.io.IOUtils

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.{break, breakable}
import java.io.FileInputStream

object HDFSOperator {

  def readFile(path: String): String = {
    if(path.startsWith("file://")){
      FileUtils.readFileToString(new File(path.replace("file://", "")))
      
    }else{
      val fs = FileSystem.get(new Configuration())
      var br: BufferedReader = null
      var line: String = null
      val result = new ArrayBuffer[String]()
      try {
        br = new BufferedReader(new InputStreamReader(fs.open(new Path(path))))
        line = br.readLine()
        while (line != null) {
          result += line
          line = br.readLine()
        }
      } finally {
        if (br != null) br.close()
      }
      result.mkString("\n")
    }

  }

  def getFileStatus(path: String) = {
    val fs = FileSystem.get(new Configuration())
    val file = fs.getFileStatus(new Path(path))
    file
  }


  def readAsInputStream(fileName: String): InputStream = {
      if(fileName.startsWith("file://")){
          new FileInputStream(new File(fileName.replace("file://", "")))
      }else{
        val fs = FileSystem.get(new Configuration())
        val src: Path = new Path(fileName)
        var in: FSDataInputStream = null
        try {
          in = fs.open(src)
        } catch {
          case e: Exception =>
            if (in != null) in.close()
        }
        return in
      }
  }


  def readBytes(fileName: String): Array[Byte] = {
    val fs = FileSystem.get(new Configuration())
    val src: Path = new Path(fileName)
    var in: FSDataInputStream = null
    try {
      in = fs.open(src)
      val byteArrayOut = new ByteArrayOutputStream()
      IOUtils.copyBytes(in, byteArrayOut, 1024, true)
      byteArrayOut.toByteArray
    } finally {
      if (null != in) in.close()
    }
  }

  def listModelDirectory(path: String): Seq[FileStatus] = {
    val fs = FileSystem.get(new Configuration())
    fs.listStatus(new Path(path)).filter(f => f.isDirectory)
  }

  def listFiles(path: String): Seq[FileStatus] = {
    val fs = FileSystem.get(new Configuration())
    fs.listStatus(new Path(path))
  }

  def saveBytesFile(path: String, fileName: String, bytes: Array[Byte]) = {

    var dos: FSDataOutputStream = null
    try {

      val fs = FileSystem.get(new Configuration())
      if (!fs.exists(new Path(path))) {
        fs.mkdirs(new Path(path))
      }
      dos = fs.create(new Path(new java.io.File(path, fileName).getPath), true)
      dos.write(bytes)
    } catch {
      case ex: Exception =>
        println("file save exception")
    } finally {
      if (null != dos) {
        try {
          dos.close()
        } catch {
          case ex: Exception =>
            println("close exception")
        }
        dos.close()
      }
    }

  }

  def saveStream(path: String, fileName: String, inputStream: InputStream) = {

    var dos: FSDataOutputStream = null
    try {

      val fs = FileSystem.get(new Configuration())
      if (!fs.exists(new Path(path))) {
        fs.mkdirs(new Path(path))
      }
      dos = fs.create(new Path(new java.io.File(path, fileName).getPath), true)
      IOUtils.copyBytes(inputStream, dos, 4 * 1024 * 1024)
    } catch {
      case ex: Exception =>
        println("file save exception")
    } finally {
      if (null != dos) {
        try {
          dos.close()
        } catch {
          case ex: Exception =>
            println("close exception")
        }
        dos.close()
      }
    }

  }

  def ceateEmptyFile(path: String) = {
    val fs = FileSystem.get(new Configuration())
    val dos = fs.create(new Path(path))
    dos.close()
  }

  def saveFile(path: String, fileName: String, iterator: Iterator[(String, String)]) = {

    var dos: FSDataOutputStream = null
    try {

      val fs = FileSystem.get(new Configuration())
      if (!fs.exists(new Path(path))) {
        fs.mkdirs(new Path(path))
      }
      dos = fs.create(new Path(path + s"/$fileName"), true)
      iterator.foreach { x =>
        dos.writeBytes(x._2 + "\n")
      }
    } catch {
      case ex: Exception =>
        println("file save exception")
    } finally {
      if (null != dos) {
        try {
          dos.close()
        } catch {
          case ex: Exception =>
            println("close exception")
        }
        dos.close()
      }
    }

  }

  def getFilePath(path: String) = {
    new Path(path).toString
  }

  def copyToHDFS(tempLocalPath: String, path: String, cleanTarget: Boolean, cleanSource: Boolean) = {
    val fs = FileSystem.get(new Configuration())
    if (cleanTarget) {
      fs.delete(new Path(path), true)
    }
    fs.copyFromLocalFile(new Path(tempLocalPath),
      new Path(path))
    if (cleanSource) {
      FileUtils.forceDelete(new File(tempLocalPath))
    }

  }

  def copyToLocalFile(tempLocalPath: String, path: String, clean: Boolean) = {
    val fs = FileSystem.get(new Configuration())
    val tmpFile = new File(tempLocalPath)
    if (tmpFile.exists()) {
      FileUtils.forceDelete(tmpFile)
    }
    fs.copyToLocalFile(new Path(path), new Path(tempLocalPath))
  }

  def deleteDir(path: String) = {
    val fs = FileSystem.get(new Configuration())
    fs.delete(new Path(path), true)
  }

  def isDir(path: String) = {
    val fs = FileSystem.get(new Configuration())
    fs.isDirectory(new Path(path))
  }

  def isFile(path: String) = {
    val fs = FileSystem.get(new Configuration())
    fs.isFile(new Path(path))
  }

  def fileExists(path: String) = {
    val fs = FileSystem.get(new Configuration())
    fs.exists(new Path(path))
  }

  def createDir(path: String) = {
    val fs = FileSystem.get(new Configuration())
    fs.mkdirs(new Path(path))
  }


  def iteratorFiles(path: String, recursive: Boolean) = {
    val fs = FileSystem.get(new Configuration())
    val files = ArrayBuffer[String]()
    _iteratorFiles(fs, path, files)
    files
  }

  def _iteratorFiles(fs: FileSystem, path: String, files: ArrayBuffer[String]): Unit = {
    val p = new Path(path)
    val file = fs.getFileStatus(p)
    if (fs.exists(p)) {

      if (file.isFile) {
        files += p.toString
      }
      else if (file.isDirectory) {
        val fileStatusArr = fs.listStatus(p)
        if (fileStatusArr != null && fileStatusArr.length > 0) {
          for (tempFile <- fileStatusArr) {
            _iteratorFiles(fs, tempFile.getPath.toString, files)
          }
        }
      }
    }
  }
  
  def main(args: Array[String]): Unit = {
   /* val str = readFile("file://D:\\workspace_scala210\\hs-fp\\src\\main\\scala\\cn\\com\\servyou\\hs\\Main.scala")
    val arra = str.split("\n")
    println(arra.length)
    arra.foreach(line => println(line))*/
    
    val str = " load json. 'hdfs://user/gdmp/ i.o.txt ' as tav;"
    val sqlWords = str.split(" ").map(w =>  w.trim()).filter(w => w!="")
    val idx = sqlWords(1).indexOf(".")
    val _type = sqlWords(1).substring(0,idx)
    
    var index = sqlWords.length - 1
    
    while(index >= 0){
      if("as".equalsIgnoreCase(sqlWords(index))){
         val temptable= sqlWords(index + 1)
         if(temptable.endsWith(";")){
            println(temptable.replace(";", ""))
         }else{
           println(temptable)
         }
      }
      index -= 1
    }
  }


}
