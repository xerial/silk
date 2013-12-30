//--------------------------------------
//
// Sigar.scala
// Since: 2013/12/29 17:17
//
//--------------------------------------

package xerial.silk.sigar

import java.util.UUID
import java.io.{FileOutputStream, File}

import scala.sys.process._
import xerial.core.log.Logger


class Sigar extends Logger {

  val sigardir = "/xerial/silk/native";
  val natives = List(
    sigardir+"/sigar.jar",
    sigardir+"/libsigar-amd64-linux.so",

    sigardir+"/libsigar-amd64-freebsd-6.so",
    sigardir+"/libsigar-amd64-linux.so",
    sigardir+"/libsigar-amd64-solaris.so",
    sigardir+"/libsigar-ia64-hpux-11.sl",
    //      sigardir+"/libsigar-ia64-linux.so",
    sigardir+"/libsigar-pa-hpux-11.sl",
    sigardir+"/libsigar-ppc64-aix-5.so",
    sigardir+"/libsigar-ppc64-linux.so",
    sigardir+"/libsigar-ppc-aix-5.so",
    sigardir+"/libsigar-ppc-linux.so",
    sigardir+"/libsigar-s390x-linux.so",
    sigardir+"/libsigar-sparc64-solaris.so",
    sigardir+"/libsigar-sparc-solaris.so",
    sigardir+"/libsigar-universal64-macosx.dylib",
    sigardir+"/libsigar-universal-macosx.dylib",
    sigardir+"/libsigar-x86-freebsd-5.so",
    sigardir+"/libsigar-x86-freebsd-6.so",
    sigardir+"/libsigar-x86-linux.so",
    sigardir+"/libsigar-x86-solaris.so",
    //      sigardir+"/log4j.jar",
    sigardir+"/sigar-amd64-winnt.dll",
    //      sigardir+"/sigar.jar",
    sigardir+"/sigar-x86-winnt.dll",
    sigardir+"/sigar-x86-winnt.lib"
  )

  val uuid = UUID.randomUUID().toString()
  val s_tmpdir = System.getProperty("java.io.tmpdir") + "/" + uuid
  val tmpdir = new File(s_tmpdir)
  tmpdir.deleteOnExit()
  if(!tmpdir.exists()){
    tmpdir.mkdirs()
  }
  for(t <- natives){
    val foo = t.split("/");
    val lib = foo(foo.length-1);
    val extracted_file = new File(tmpdir+"/"+lib);
    extracted_file.deleteOnExit();

    try{
      val reader = getClass.getResourceAsStream(t)
      val writer = new FileOutputStream(extracted_file)
      var buffer = new Array[Byte](8192);

      /*
            Iterator
              .continually(reader.read(buffer))
              .takeWhile(_ != -1)
              .map(len => writer.write(buffer, 0, len))
      */

      try{
        var len = -1
        // DO NOT USE 'break'
        while({len = reader.read(buffer); len != -1}){
          writer.write(buffer,0,len)
        }
      }
      finally{
        if(writer != null){
          writer.close()
        }
        if(reader != null){
          reader.close()
        }
      }
    }
    catch{
      case t: Throwable => {
        t.printStackTrace();
      }
      //case t: Throwable => {t.printStackTrace(); return "error 1";}

    }
  }

  def loadAverage(): String = {
    try{
      val result = ("java -Djava.library.path="+s_tmpdir+" -jar "+s_tmpdir+"/sigar.jar uptime").!!
      val t2 = result.dropRight(1).split(" ") // chop the '\n'
      val la = t2(t2.length-3)+" "+t2(t2.length-2)+" "+t2(t2.length-1)
      return la
      //      System.out.println("#"+la+"#");
    }
    catch{
      case t:Throwable => {
        t.printStackTrace()
        return "error: loadAverage"
      }
    }

    // Get the system load average
    //return "TEST"
    /*
    val sigar = new Sigar()
    var las = new Array[Double](3)
    for(i <- 0 to 2){
      las(i) = -1.0
    }

    try{
      las = sigar.getLoadAverage()
    }
    catch{
      case e: SigarException => e.printStackTrace()
    }
    return las.mkString(", ")
    */
  }

  def free(): String = {
    try{
      val result = ("java -Djava.library.path="+s_tmpdir+" -jar "+s_tmpdir+"/sigar.jar free").!!
      val t1 = result.dropRight(1).split("\n")(1)// chop & split
      val t2 = t1.split(" ")
      // Mem: total used free
      val freeKB = t2(t2.length-1)
      val freeGB = "%.2f".format(freeKB.toDouble/(1024*1024))
      freeGB
    }
    catch{
      case e:Exception => {
        error(e)
        "error: free"
      }
    }
  }

}