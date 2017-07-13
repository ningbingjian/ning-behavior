package cn.ning.util

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, IOException}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.hadoop.io.{IOUtils, NullWritable, SequenceFile, Text}
import org.apache.hadoop.io.SequenceFile.{Reader => SeqReader, Writer => SeqWriter}
import org.apache.hadoop.io.compress.SnappyCodec
import java.io.OutputStream

/**
  * Created by ning on 2017/6/20.
  */
object DfsUtil {
  val configuration = new Configuration()
  lazy val fs = FileSystem.get(configuration)
  def mkDir(dir:String):Boolean={
    val p = new Path(dir)
    if(!fs.exists(p)){
      return fs.mkdirs(p)
    }
    false
  }
  def exists(path:String):Boolean={
    fs.exists(new Path(path))
  }
  def getSeqWriter(opts:SequenceFile.Writer.Option*):SequenceFile.Writer={
    SequenceFile.createWriter(configuration,opts:_*)
  }
  def getSeqWriter(path:String,keyClazz:Class[_],valueClazz:Class[_]):SequenceFile.Writer={
    val opts = Seq(
      SeqWriter.keyClass(keyClazz),
      SeqWriter.valueClass(valueClazz),
      SeqWriter.compression(SequenceFile.CompressionType.BLOCK),
      SeqWriter.file(new Path(path))
    )
    getSeqWriter(opts:_*)
  }
  def appendSeqFile(writer:SequenceFile.Writer,key:Any,value:Any)={
    writer.append(key,value)
  }
  def getSeqReader(path:String,opts:SequenceFile.Reader.Option*):SequenceFile.Reader = {
    val comOpts =  Seq(
      SeqReader.file(new Path(path))
    ) ++ opts
    new SeqReader(configuration,comOpts:_*)
  }
  def close(c:java.io.Closeable)={
      c.close()
  }
  def closeQuiet(c:java.io.Closeable)={
    try{
      c.close()
    }catch {
      case _ =>
    }finally {
    }

  }

  def openFile(p:String,append:Boolean = true): FSDataOutputStream ={
    val path = new Path(p)
    if(fs.exists(path)){
      if(append){
        fs.append(path)
      }else{
        fs.delete(path, true)
        fs.create(path)
      }
    }else{
      fs.create(path)
    }
  }
  def write(ips:ByteArrayInputStream,ops:OutputStream,close:Boolean = true): Unit ={
    IOUtils.copyBytes(ips, ops, 1024, close)
  }
  /* Append the given string to the given file */ @throws[IOException]
  def appendFile(p: Path, s: String) {
    assert(fs.exists(p))
    val is  = new ByteArrayInputStream(s.getBytes)
    val os  = fs.append(p)
    IOUtils.copyBytes(is, os, s.length, true)
  }

  /* Write the given string to the given file */ @throws[IOException]
  def writeFile(path: String, s: String) {
    val p = new Path(path)
    if (fs.exists(p)) fs.delete(p, true)
    val is  = new ByteArrayInputStream(s.getBytes)
    val os  = fs.create(p)
    IOUtils.copyBytes(is, os, s.length, true)
  }

  def readFileBuffer(path: String): Array[Byte] = {
    val fileName = new Path(path)
    val os  = new ByteArrayOutputStream
    try{
      val in  = fs.open(fileName)
      try {
        IOUtils.copyBytes(fs.open(fileName), os, 1024, true)
        return os.toByteArray
      }finally {
        close(in)
      }
    }finally {
      close(os)
    }
  }

  @throws[IOException]
  def readFile(fileName: String): String = {
    val buf  = readFileBuffer(fileName)
    new String(buf, 0, buf.length)
  }
  def deleteQuiet(path:String):Boolean = {
    try{
      fs.delete(new Path(path),true)
    }catch{
      case _ => false
    }

  }


  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir","D:\\tools\\hadoop-v2\\hadoop-2.6.0")
    val path = "/user/ning/spark/2/t.seq";
    val ops = openFile(path,false)
    val ips1 = new ByteArrayInputStream("babababa\n".getBytes("UTF-8"))
    val ips2 = new ByteArrayInputStream("babababa\n".getBytes("UTF-8"))
    write(ips1,ops,false)
    write(ips2,ops,false)
    closeQuiet(ops)
    closeQuiet(ips1)
    closeQuiet(ips2)
    println(readFile(path))
  }
}
