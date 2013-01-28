//--------------------------------------
//
// DistributedFileSystem.scala
// Since: 2012/12/19 5:40 PM
//
//--------------------------------------

package xerial.silk.example

import xerial.silk._
import java.io.File
import xerial.silk.core.Silk
import xerial.silk.cluster.Host


case class FileLoc(file:File, host:Host)

/**
 * Collect files in each hosts, then read files in the hosts where the file is located
 *
 * @author Taro L. Saito
 */
class DistributedFileSystem {

  def main(args:Array[String])  {

    val path = new File("/export/data")

    def listFiles(h:Host, p:File) : Seq[FileLoc] = {
      if(p.isDirectory)
        p.listFiles.flatMap { listFiles(h, _) }
      else if(p.exists())
        Seq(FileLoc(p, h))
      else
        Seq.empty
    }

    val hosts = Silk.hosts
    // Create list of files in each host
    val fileList = hosts.flatMap { h => listFiles(h.host, path) }

    val fs = fileList.iterator.toSeq


    val file = fs.apply(0)

    at(file.host) { 
      // access to the file
    }

  }

}