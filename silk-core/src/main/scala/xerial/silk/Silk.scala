package xerial.silk

import java.util.UUID
import xerial.silk.framework.ops._
import scala.reflect.ClassTag
import scala.language.experimental.macros
import scala.language.existentials
import xerial.silk.framework._
import java.io.File
import xerial.lens.{FieldParameter, ObjectSchema}
import xerial.silk.SilkException._
import scala.reflect.runtime.{universe=>ru}
import xerial.silk.util.Guard
import xerial.core.log.Logger
import java.net.{UnknownHostException, InetAddress}
import scala.io.Source
import xerial.silk
import xerial.silk.framework.NodeRef
import scala.Some
import xerial.silk.framework.Node
import xerial.silk.framework.ops.LoadFile
import xerial.lens.FieldParameter
import xerial.silk.framework.ops.CommandOp
import xerial.silk.framework.ops.FContext
import xerial.silk.core.SilkInitializer


object Silk extends Guard with Logger {

  implicit class SilkSeqWrap[A](val a:Seq[A]) {
    def toSilk : SilkSeq[A] = macro SilkMacros.mRawSeq[A]
  }

  implicit class SilkArrayWrap[A](val a:Array[A]) {
    def toSilk : SilkSeq[A] = macro SilkMacros.mArrayToSilk[A]
  }

  implicit class SilkWrap[A](val a:A) {
    def toSilkSingle : SilkSingle[A] = macro SilkMacros.mNewSilkSingle[A]
  }

  implicit class CommandBuilder(val sc:StringContext) extends AnyVal {
    def c(args:Any*) : CommandOp = macro SilkMacros.mCommand
  }

  /**
   * Import another workflow trait as a mixin to this class. The imported workflow shares the same session
   * @param ev
   * @tparam A
   * @return
   */
  def mixin[A](implicit ev:ClassTag[A]) : A = macro WorkflowMacros.mixinImpl[A]


  def empty[A] = Empty
  private[silk] def emptyFContext = FContext(classOf[Silk[_]], "empty", None, None, "", 0, 0)

  object Empty extends SilkSeq[Nothing] {
    override val id = UUID.nameUUIDFromBytes("empty".getBytes)
    def fc = emptyFContext
  }

  private[silk] def setEnv(newEnv:SilkEnv) {
    _env = Some(newEnv)
  }

  @transient private[silk] var _env : Option[SilkEnv] = None

//
//  def env: SilkEnv = _env.getOrElse {
//    SilkException.error("SilkEnv is not yet initialized")
//  }

  def loadFile(file:String) : LoadFile = macro SilkMacros.loadImpl

  def newSilk[A](in:Seq[A]) : SilkSeq[A] = macro SilkMacros.mNewSilk[A]

  def scatter[A](in:Seq[A], numNodes:Int) : SilkSeq[A] = macro SilkMacros.mScatter[A]

  def registerWorkflow[W](name:String, workflow:W) : W ={
    workflow
  }

  /**
   * Shuffle the two input sequences then merge them
   * @param a
   * @param b
   * @param probeA
   * @param probeB
   * @tparam A
   * @tparam B
   * @return
   */
  def shuffleMerge[A, B](a:SilkSeq[A], b:SilkSeq[B], probeA:A=>Int, probeB:B=>Int) : SilkSeq[(Int, SilkSeq[A], SilkSeq[B])] = macro SilkMacros.mShuffleMerge[A, B]


  private var silkEnvList : List[SilkInitializer] = List.empty

  /**
   * Initialize a Silk environment
   * @param zkConnectString
   * @return
   */
  def init(zkConnectString: => String = silk.config.zk.zkServersConnectString) = {
    val launcher = new SilkInitializer(zkConnectString)
    // Register a new launcher
    guard {
      silkEnvList ::= launcher
    }
    launcher.start
  }

  def testInit : SilkEnv = new SilkEnv {
    def run[A](op: Silk[A]) = Seq.empty[A]
    def run[A](op: Silk[A], target: String) = Seq.empty[A]
    def eval[A](op: Silk[A]) {}
    private[silk] def runF0[R](locality: Seq[String], f: => R) = f
  }

  /**
   * Clean up all SilkEnv
   */
  def cleanUp = guard {
    for(env <- silkEnvList.par)
      env.stop
    silkEnvList = List.empty
  }

  def defaultHosts(clusterFile:File = config.silkHosts): Seq[Host] = {
    if (clusterFile.exists()) {
      def getHost(line: String): Option[Host] = {
        try
          Host.parseHostsLine(line)
        catch {
          case e: UnknownHostException => {
            warn(s"unknown host: $line")
            None
          }
        }
      }
      val hosts = for {
        (line, i) <- Source.fromFile(clusterFile).getLines.zipWithIndex
        host <- getHost(line)
      } yield host
      hosts.toSeq
    }
    else {
      warn("$HOME/.silk/hosts is not found. Use localhost only")
      Seq(localhost)
    }
  }



  def hosts : Seq[Node] = {

    def collectClientInfo(zkc: ZooKeeperClient): Seq[Node] = {
      val cm = new ClusterNodeManager with ZooKeeperService {
        val zk = zkc
      }
      cm.nodeManager.nodes
    }

    val ci = ZooKeeper.defaultZkClient.flatMap(zk => collectClientInfo(zk))
    ci.toSeq
  }

  def master : Option[MasterRecord] = {
    def getMasterInfo(zkc: ZooKeeperClient) : Option[MasterRecord] = {
      val cm = new MasterRecordComponent  with ZooKeeperService with DistributedCache {
        val zk = zkc
      }
      cm.getMaster
    }
    ZooKeeper.defaultZkClient.flatMap(zk => getMasterInfo(zk)).headOption
  }

  /**
   * Execute a command at the specified host
   * @param h
   * @param f
   * @tparam R
   * @return
   */
  def at[R](h:Host)(f: => R)(implicit env:SilkEnv) : R = {
    Remote.at[R](NodeRef(h.name, h.address, config.silkClientPort))(f)
  }

  def at[R](n:Node)(f: => R)(implicit env:SilkEnv) : R =
    Remote.at[R](n.toRef)(f)

  def at[R](n:NodeRef)(f: => R)(implicit env:SilkEnv) : R =
    Remote.at[R](n)(f)


  private var _localhost : Host = {
    try {
      val lh = InetAddress.getLocalHost
      val addr = System.getProperty("silk.localaddr", lh.getHostAddress)
      Host(lh.getHostName, addr)
    }
    catch {
      case e:UnknownHostException =>
        val addr = InetAddress.getLoopbackAddress
        Host(addr.getHostName, addr.getHostAddress)
    }
  }

  def setLocalHost(h:Host) { _localhost = h }

  def localhost : Host = _localhost

}



/**
 * Silk[A] is an abstraction of a set of data elements of type A, which might be distributed
 * over cluster nodes.
 *
 * Silk[A] is a base trait for all Silk data types.
 *
 * We do not define operations on Silk data in this trait,
 * since map, filter etc. needs macro-based implementation, which cannot be used to override
 * interface methods.
 *
 * @tparam A element type
 */
trait Silk[+A] extends Serializable with IDUtil {

  /**
   * ID of this operation. To provide stable IDs for each run, the id should be generated by using
   * the input IDs of this operation and FContext.
   *
   * For
   *
   * @return
   */
  val id: UUID // = SilkUtil.newUUIDOf(fc, inputs:_*)

  override def hashCode = id.hashCode()

  override def equals(obj:Any) : Boolean = {
    obj match {
      case s:Silk[_] => id == s.id
      case other => false
    }
  }

  /**
   * Dependent input Silk data
   * @return
   */
  def inputs : Seq[Silk[_]] = {
    val b = Seq.newBuilder[Silk[_]]
    this match {
      case p:Product => p.productIterator.foreach{
        case s:Silk[_] => b += s
        case _ =>
      }
    }
    b.result
  }

  def idPrefix = id.prefix

  def save : SilkSingle[File] = NA


  /**
   * Byte size of the given input
   */
  def byteSize : SilkSingle[Long] = NA

  /**
   * Returns Where this Silk operation is defined. A possible value of fc is a variable or a function name.
p   */
  def fc: FContext

  def isSingle : Boolean

  override def toString = {
    val cl = this.getClass
    val schema = ObjectSchema(cl)
    val params = for {p <- schema.constructor.params
                      if p.name != "id" &&  p.name != "ss" && p.valueType.rawType != classOf[ClassTag[_]]
                      v = p.get(this) if v != null} yield {
      if (classOf[ru.Expr[_]].isAssignableFrom(p.valueType.rawType)) {
        s"${v}[${v.toString.hashCode}]"
      }
      else if (classOf[Silk[_]].isAssignableFrom(p.valueType.rawType)) {
        s"[${v.asInstanceOf[Silk[_]].idPrefix}]"
      }
      else
        v
    }

    val prefix = s"[$idPrefix]"
    val s = s"${cl.getSimpleName}(${params.mkString(", ")})"
    //    val fv = freeVariables
    //    val fvStr = if (fv == null || fv.isEmpty) "" else s"{${fv.mkString(", ")}}|= "
    s"$prefix $s"
  }


  def toSilkString : String = {
    val s = new StringBuilder
    mkSilkText(0, s)
    s.result.trim
  }


  private def mkSilkText(level:Int, s:StringBuilder)  {
    def indent(n:Int) : String = {
      val b = new StringBuilder(n)
      for(i <- 0 until n) b.append(' ')
      b.result
    }

    val sc = ObjectSchema.apply(getClass)
    val idt = indent(level)
    s.append(s"${idt}-${sc.name}\n")
    for(p <- sc.constructor.params) {
      val v = p.get(this)
      val idt = indent(level+1)
      v match {
        case f:Silk[_] =>
          s.append(s"$idt-${p.name}\n")
          f.mkSilkText(level+2, s)
        case e:ru.Expr[_] =>
          s.append(s"$idt-${p.name}: ${ru.show(e)}\n")
        case _ =>
          s.append(s"$idt-${p.name}: $v\n")
      }
    }
  }


  /**
   * Assign a new ID to this operation to enforce recomputing the same function
   * @return
   */
  def touch : this.type = {
    val sc = ObjectSchema(this.getClass)
    val params = for(p <- sc.constructor.params) yield {
      val v = p.get(this)
      val newV = if(p.name == "id") {
        // Create a stable UUID based on the previous ID
        SilkUtil.newUUIDFromString(v.toString+"-touch")
      }
      else
        v
      newV.asInstanceOf[AnyRef]
    }
    val c = sc.constructor.newInstance(params.toSeq.toArray[AnyRef])
//
//    try {
//      sc.findParameter("id").map { p =>
//        p match {
//          case fp:FieldParameter =>
//            fp.field.setAccessible(true)
//            fp.field.set(c, newID)
//          case _ => SilkException.error("cannot set a new id to this operation")
//        }
//      }
//    }
//    catch {
//      case e:NoSuchFieldException => SilkException.error("no id field is found")
//    }
    c.asInstanceOf[this.type]
  }


}







