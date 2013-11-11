//--------------------------------------
//
// HadoopOps.scala
// Since: 2013/10/09 2:22 PM
//
//--------------------------------------

package xerial.silk.framework.ops

import xerial.silk.{SilkUtil, SilkSeq, SilkSingle}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{BlockLocation, Path, FileSystem}
import xerial.core.io.IOUtil
import java.util.UUID


object HDFSUtil {

  def loadBlock(conf:Configuration, bl:BlockLocation) {
    val fs = FileSystem.get(conf)

  }

}


case class LoadHDFS(id:UUID, fc:FContext, conf:Configuration, file:String) extends SilkSingle[String] {


  def blockLocations : Seq[BlockLocation] = {
    val fs = FileSystem.get(conf)
    val b = Seq.newBuilder[BlockLocation]
    IOUtil.withResource(fs) { fs =>
      val p = new Path(file)
      val s = fs.getFileStatus(p)
      val bl = fs.getFileBlockLocations(p, 0, s.getLen)
      b ++= bl
    }
    b.result
  }
}