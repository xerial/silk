//--------------------------------------
//
// SilkFramework.scala
// Since: 2013/06/09 11:44
//
//--------------------------------------

package xerial.silk.framework

import scala.language.higherKinds
import scala.language.experimental.macros
import scala.reflect.ClassTag
import xerial.silk.{Silk, CommentLine, SilkException, SilkError}
import xerial.core.log.Logger
import java.util.UUID
import java.net.InetAddress
import xerial.silk.framework.ops.CallGraph
import xerial.silk.core.SilkSerializer
import xerial.core.util.DataUnit

import java.io.File;
import java.io.FileOutputStream;

import scala.sys.process._;
import scala.language.postfixOps;

/**
 * SilkFramework contains the abstraction of input and result data types of Silk operations.
 *
 * @author Taro L. Saito
 */
trait SilkFramework {

  /**
   * Silk is an abstraction of data processing operation. By calling run method, its result can be obtained
   * @tparam A
   */
  type Result[A] = Seq[A]
  type Session = SilkSession

  /**
   * Future reference to a result
   * @tparam A
   */
  type ResultRef[A] = SilkFuture[Result[A]]

  /**
   * Helper functions
   */
  protected object helper {
    def fwrap[A,B](f:A=>B) = f.asInstanceOf[Any=>Any]
    def filterWrap[A](f:A=>Boolean) = f.asInstanceOf[Any=>Boolean]
    def rwrap[P, Q, R](f: (P, Q) => R) = f.asInstanceOf[(Any, Any) => Any]
  }

}

trait LocalClient {

  def currentNodeName : String
  def address : String
  def executor : ExecutorAPI
  def sliceStorage : SliceStorageAPI
}


/**
 * Used to refer to SilkClient within components
 */
trait LocalClientComponent {

  def localClient : LocalClient

}




trait SerializationService {

  implicit class Serializer(a:Any) {
    def serialize : Array[Byte] = SilkSerializer.serializeObj(a)
  }

  implicit class Deserializer(b:Array[Byte]) {
    def deserialize[A] : A = SilkSerializer.deserializeObj[A](b)
  }
}

trait SilkRunner extends SilkFramework with ProgramTreeComponent {
  self: ExecutorComponent =>

  def eval[A](silk:Silk[A]) = executor.eval(silk)

  /**
   * Evaluate the silk using the default session
   * @param silk
   * @tparam A
   * @return
   */
  def run[A](silk:Silk[A]) : Result[A] = run(SilkSession.defaultSession, silk)
  def run[A](silk:Silk[A], target:String) : Result[_] = {
    ProgramTree.findTarget(silk, target).map { t =>
      run(t)
    } getOrElse { SilkException.error(s"target $target is not found") }
  }

  def run[A](session:Session, silk:Silk[A]) : Result[A] = {
    executor.run(session, silk)
  }

}


/**
 * Components that need some initialization and termination steps should override this trait.
 * startup and teardown methods will be invoked from SilkClientService or SilkMasterService.
 */
trait LifeCycle {

  def startup {}
  def teardown {}
}


/**
 * A component for manipulating program trees
 */
trait ProgramTreeComponent {
  self:SilkFramework =>

  /**
   * Enclose the tree traversal functions within the object since they should be accessible within SilkFramework only
   */
  protected object ProgramTree extends Logger {

    def graphOf[A](op:Silk[A]) = CallGraph.createCallGraph(op)

    /**
     * Find a part of the silk tree
     * @param silk
     * @param targetName
     * @tparam A
     * @return
     */
    def collectTarget[A](silk:Silk[A], targetName:String) : Seq[Silk[_]] = {
      info(s"Find target {$targetName} from $silk")
      val g = graphOf(silk)
      debug(s"call graph: $g")

      g.nodes.collect{
        case op if op.fc.localValName.map(_ == targetName) getOrElse false =>
          op
      }
    }

    def findTarget[A](silk:Silk[A], targetName:String) : Option[Silk[_]] = {
      val matchingOps = collectTarget(silk, targetName)
      matchingOps.size match {
        case v if v > 1 => throw new IllegalArgumentException(s"more than one target is found for $targetName")
        case other => matchingOps.headOption
      }
    }

    def descendantsOf[A](silk:Silk[A]) : Set[Silk[_]] = {
      val g = graphOf(silk)
      g.descendantsOf(silk)
    }

    def descendantsOf[A](silk:Silk[A], targetName:String) : Set[Silk[_]] = {
      val g = graphOf(silk)
      findTarget(silk, targetName) match {
        case Some(x) => g.descendantsOf(x)
        case None => Set.empty
      }
    }
  }

}


//
//
//
///**
// * Managing running state of
// */
//trait StageManagerComponent extends SilkFramework {
//
//  type StageManager <: StageManagerAPI
//  val stageManager: StageManagerAPI
//
//  trait StageManagerAPI {
//    /**
//     * Call this method when an evaluation of the given Silk expression has started
//     * @param op
//     * @return Future of the all slices
//     */
//    def startStage[A](op:Silk[A])
//
//    def finishStage[A](op:Silk[A])
//
//    def abortStage[A](op:Silk[A])
//
//    /**
//     * Returns true if the evaluation of the Silk expression has finished
//     * @param op
//     * @return
//     */
//    def isFinished[A](op: Silk[A]): Boolean
//  }
//
//}




trait DistributedFramework
  extends SilkFramework {

}

/**
 * Representing worker node
 * @param name
 * @param address
 * @param clientPort
 * @param dataServerPort
 */
case class Node(name:String,
                address:String,
                pid:Int,
                clientPort:Int,
                dataServerPort:Int,
                webuiPort:Int,
                resource:NodeResource) {
  def host = Host(name, address)
  def toRef = NodeRef(name, address, clientPort)

  def loadAverage(): String = {
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
    );

    val uuid = UUID.randomUUID().toString();
    val s_tmpdir = System.getProperty("java.io.tmpdir") + "/" + uuid;
    val tmpdir = new File(s_tmpdir);
    tmpdir.deleteOnExit();
    if(!tmpdir.exists()){
      tmpdir.mkdirs();
    }
    for(t <- natives){
      val foo = t.split("/");
      val lib = foo(foo.length-1);
      val extracted_file = new File(tmpdir+"/"+lib);
      extracted_file.deleteOnExit();

      try{
        val reader = getClass.getResourceAsStream(t);
        val writer = new FileOutputStream(extracted_file);
        var buffer = new Array[Byte](8192);

        Iterator
          .continually(reader.read(buffer))
          .takeWhile(_ != -1)
          .map(len => writer.write(buffer, 0, len))

        try{
          var len = -1;
          // DO NOT USE 'break'
          while({len = reader.read(buffer); len != -1}){
            writer.write(buffer,0,len);
          }
        }
        finally{
          if(writer != null){
            writer.close();
          }
          if(reader != null){
            reader.close();
          }
        }
      }
      catch{
        case t: Throwable => {t.printStackTrace(); return "error 1";}
      }
    }

    try{
      val result = ("java -Djava.library.path="+s_tmpdir+" -jar "+s_tmpdir+"/sigar.jar uptime") !! ;
      val t2 = result.dropRight(1).split(" ");// chop the '\n'
      val la = t2(t2.length-3)+" "+t2(t2.length-2)+" "+t2(t2.length-1);
      return la;
//      System.out.println("#"+la+"#");
    }
    catch{
      case t:Throwable => {t.printStackTrace(); return "error 2";}
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

}

case class NodeRef(name:String, address:String, clientPort:Int) {
  def host = Host(name, address)
}



object Host extends Logger {
  def apply(s:String) : Host = {
    val lh = InetAddress.getByName(s)
    Host(s, lh.getHostAddress)
  }

  def parseHostsLine(line:String) : Option[Host] = {
    try {
      // Strip by white spaces (hostname, ip address)
      val trimmed = line.trim
      val c = trimmed.split("""\s+""")
      if(trimmed.startsWith("#") || trimmed.isEmpty)
        None
      else if(c.length >= 1 && !c(0).isEmpty) {
        if(c.length > 1 && !c(1).isEmpty)
          Some(new Host(c(0), c(1)))
        else
          Some(apply(c(0)))
      }
      else
        None
    }
    catch {
      case e:Exception =>
        warn(s"invalid line: $line")
        None
    }
  }
}

case class Host(name: String, address: String) {
  def prefix = name.split("\\.")(0)
}



case class NodeResource(nodeName:String, numCPUs:Int, memorySize:Long) {

  def readableMemorySize = DataUnit.toHumanReadableFormat(memorySize)

  private def ensureSameNode(n:String) {
    require(nodeName == n, "must be the same node")
  }

  private def ensureNonNegative(v:Long) = if(v < 0) 0L else v

  def adjustFor(r:ResourceRequest) = {
    NodeResource(nodeName, r.cpu, r.memorySize.getOrElse(-1))
  }

  def -(r:NodeResource) = {
    ensureSameNode(r.nodeName)
    NodeResource(nodeName, numCPUs - r.numCPUs, memorySize - ensureNonNegative(r.memorySize))
  }

  def +(r:NodeResource) = {
    ensureSameNode(r.nodeName)
    NodeResource(nodeName, numCPUs + r.numCPUs, memorySize + ensureNonNegative(r.memorySize))
  }

  def isEnoughFor(r:ResourceRequest) : Boolean = {
    r.cpu <= numCPUs && r.memorySize.map(_ <= memorySize).getOrElse(true)
  }
}

case class ResourceRequest(nodeName:Option[String], cpu:Int, memorySize:Option[Long])


/**
 * Node manager component
 */
trait NodeManagerComponent {

  type NodeManager <: NodeManagerAPI
  val nodeManager : NodeManager

  def clientIsActive(nodeName:String) : Boolean

  trait NodeManagerAPI {
    def nodes : Seq[Node]
    def getNode(nodeName:String) : Option[Node]
    def addNode(n:Node)
    def removeNode(nodeName:String)
  }
}

/**
 * ResourceManager runs on a master node and manages
 * CPU/memory resources available in nodes
 */
trait ResourceManagerComponent {

  type ResourceManager <: ResourceManagerAPI

  val resourceManager : ResourceManager


}

trait ResourceManagerAPI {
  /**
   * Acquire the resource. This operation blocks until the resource becomes available
   */
  def acquireResource(r:ResourceRequest) : NodeResource
  def addResource(n:Node, r:NodeResource)
  def getNodeRef(nodeName:String) : Option[NodeRef]
  def releaseResource(r:NodeResource)
  def lostResourceOf(nodeName:String)
}
