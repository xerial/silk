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
import xerial.core.util.Shell
import org.hyperic.sigar.Sigar
import xerial.core.io.{Resource, IOUtil}
import java.nio.file.{Path, Files}


object SigarUtil {

  private val sigarResourceDir = "/xerial/silk/native"

  lazy val sigar = {
    val tmpdir = new File(System.getProperty("java.io.tmpdir"))
    val sigarDir = IOUtil.createTempDir(tmpdir, "sigar")
    sigarDir.mkdirs()


    val res = Resource.find("/xerial/silk/native/libsigar-universal64-macosx.dylib")
    val sigarFile = new File(sigarDir, f"${UUID.randomUUID().getMostSignificantBits}%x.dll")
    sigarFile.deleteOnExit()
    IOUtil.withResource(res.get.openStream()) { s => Files.copy(s, sigarFile.toPath) }
    System.load(sigarFile.getAbsolutePath)
    new Sigar
  }

  def loadAverage(): Array[Double] = {
    sigar.getLoadAverage
  }

  def freeMemory(): Long = {
    sigar.getMem.getFree
  }


}


class SigarUtil extends Logger {


//
//  val natives = List(
//    sigardir+"/sigar.jar",
//    sigardir+"/libsigar-amd64-linux.so",
//
//    sigardir+"/libsigar-amd64-freebsd-6.so",
//    sigardir+"/libsigar-amd64-linux.so",
//    sigardir+"/libsigar-amd64-solaris.so",
//    sigardir+"/libsigar-ia64-hpux-11.sl",
//    //      sigardir+"/libsigar-ia64-linux.so",
//    sigardir+"/libsigar-pa-hpux-11.sl",
//    sigardir+"/libsigar-ppc64-aix-5.so",
//    sigardir+"/libsigar-ppc64-linux.so",
//    sigardir+"/libsigar-ppc-aix-5.so",
//    sigardir+"/libsigar-ppc-linux.so",
//    sigardir+"/libsigar-s390x-linux.so",
//    sigardir+"/libsigar-sparc64-solaris.so",
//    sigardir+"/libsigar-sparc-solaris.so",
//    sigardir+"/libsigar-universal64-macosx.dylib",
//    sigardir+"/libsigar-universal-macosx.dylib",
//    sigardir+"/libsigar-x86-freebsd-5.so",
//    sigardir+"/libsigar-x86-freebsd-6.so",
//    sigardir+"/libsigar-x86-linux.so",
//    sigardir+"/libsigar-x86-solaris.so",
//    //      sigardir+"/log4j.jar",
//    sigardir+"/sigar-amd64-winnt.dll",
//    //      sigardir+"/sigar.jar",
//    sigardir+"/sigar-x86-winnt.dll",
//    sigardir+"/sigar-x86-winnt.lib"
//  )
//
//  val uuid = UUID.randomUUID().toString()
//  val s_tmpdir = System.getProperty("java.io.tmpdir") + "/" + uuid
//  val tmpdir = new File(s_tmpdir)
//  tmpdir.deleteOnExit()
//  if(!tmpdir.exists()){
//    tmpdir.mkdirs()
//  }
//  for(t <- natives){
//    val foo = t.split("/");
//    val lib = foo(foo.length-1);
//    val extracted_file = new File(tmpdir+"/"+lib);
//    extracted_file.deleteOnExit();
//
//    try{
//      val reader = getClass.getResourceAsStream(t)
//      val writer = new FileOutputStream(extracted_file)
//      var buffer = new Array[Byte](8192);
//
//      /*
//            Iterator
//              .continually(reader.read(buffer))
//              .takeWhile(_ != -1)
//              .map(len => writer.write(buffer, 0, len))
//      */
//
//      try{
//        var len = -1
//        // DO NOT USE 'break'
//        while({len = reader.read(buffer); len != -1}){
//          writer.write(buffer,0,len)
//        }
//      }
//      finally{
//        if(writer != null){
//          writer.close()
//        }
//        if(reader != null){
//          reader.close()
//        }
//      }
//    }
//    catch{
//      case t: Throwable => {
//        t.printStackTrace();
//      }
//      //case t: Throwable => {t.printStackTrace(); return "error 1";}
//
//    }
//  }


}