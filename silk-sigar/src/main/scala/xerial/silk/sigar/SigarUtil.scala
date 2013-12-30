//--------------------------------------
//
// SigarUtil.scala
// Since: 2013/12/29 17:17
//
//--------------------------------------

package xerial.silk.sigar

import java.util.UUID
import java.io.{FileOutputStream, File}

import org.hyperic.sigar.Sigar
import java.nio.file.{Path, Files}



object SigarUtil {

  def nativeLibName :String = {
    val os = OSInfo.getOSName
    val arch = OSInfo.getArchName

    def unsupported : Nothing = {
      throw new UnsupportedOperationException(s"Sigar is not supported in os:$os arch:$arch")
    }

    os match {
      case "Windows" => arch match {
        case OSInfo.X86_64 => "sigar-amd64-winnt.dll"
        case OSInfo.X86 => "sigar-x86-winnt.dll"
        case _ => unsupported
      }
      case "Mac" => arch match {
        case OSInfo.X86 => "libsigar-universal-macosx.dylib"
        case OSInfo.X86_64 => "libsigar-universal64-macosx.dylib"
        case _ => unsupported
      }
      case "Linux" => arch match {
        case OSInfo.X86 => "libsigar-x86-linux.so"
        case OSInfo.X86_64 => "libsigar-amd64-linux.so"
        case OSInfo.IA64 => "libsigar-ia64-linux.so"
        case OSInfo.PPC => "libsigar-ppc-linux.so"
        case "ppc64" => "libsigar-ppc64-linux.so"
        case "s390x" => "libsigar-s390x-linux.so"
        case _ => unsupported
      }
      case "Solaris" => arch match {
        case OSInfo.X86 => "libsigar-x86-sparc.so"
        case OSInfo.X86_64 => "libsigar-amd64-sparc.so"
        case "sparc" => "libsigar-amd64-sparc.so"
        case "sparc64" => "libsigar-sparc64-linux.so"
        case _ => unsupported
      }
      case "AIX" => arch match {
        case OSInfo.PPC => "libsigar-ppc-aix-5.so"
        case "ppc64" => "libsigar-ppc64-aix-5.so"
        case _ => unsupported
      }
      case "FreeBSD" => arch match {
        case OSInfo.X86_64 => "libsigar-amd64-freebsd-6.so"
        case OSInfo.X86 => "libsigar-x86-freebsd-6.so"
        case _ => unsupported
      }
      case "HP UX" => arch match {
        case OSInfo.IA64 => "libsigar-ia64-hpux-11.sl"
        case "pa" => "libsigar-pa-hpux-11.sl"
        case _ => unsupported
      }
      case _ => unsupported
    }
  }




  private def createTempDir(dir: File, prefix: String): File = {
    def newDirName = new File(dir, prefix + System.currentTimeMillis())
    def loop : File = {
      val d = newDirName
      if(!d.exists() && d.mkdirs)
        d
      else
        loop
    }
    loop
  }

  /**
   * Returns Sigar API
   */
  lazy val sigar = {

    val tmpdir = new File(System.getProperty("java.io.tmpdir"))
    val sigarDir = createTempDir(tmpdir, "sigar")
    sigarDir.mkdirs()

    val sigarResourceDir = "/xerial/silk/sigar/native"
    val res = this.getClass.getResource(s"${sigarResourceDir}/${nativeLibName}")
    if(res == null)
      throw new IllegalStateException(s"Sigar native lib ${nativeLibName} is not found inside jar: /xerial/silk/sigar/native")

    // Append UUID to the jar file so that multiple class loaders can load the same library
    val sigarFile = new File(sigarDir, f"${UUID.randomUUID().getMostSignificantBits}%x.lib")
    // Delete the lib file on exit
    sigarFile.deleteOnExit()
    val s = res.openStream()
    try {
      Files.copy(s, sigarFile.toPath)
      System.load(sigarFile.getAbsolutePath)
    }
    finally {
      s.close()
    }
    new Sigar
  }

  def loadAverage: Array[Double] = {
    sigar.getLoadAverage
  }

  def freeMemory: Long = {
    sigar.getMem.getFree
  }


}


