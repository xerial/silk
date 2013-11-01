package xerial.silk

import java.util.UUID
import xerial.silk.framework.ops.{CommandOp, LoadFile, SilkMacros, FContext}
import scala.reflect.ClassTag
import scala.language.experimental.macros
import scala.language.existentials
import xerial.silk.framework._
import java.io.{ByteArrayOutputStream, ObjectOutputStream, File, Serializable}
import xerial.lens.ObjectSchema
import xerial.silk.SilkException._
import scala.reflect.runtime.{universe=>ru}
import scala.collection.GenTraversable
import xerial.core.io.IOUtil
import xerial.silk.util.Guard
import xerial.core.log.Logger
import scala.Some
import xerial.silk.framework.ops.LoadFile
import xerial.silk.framework.ops.CommandOp
import xerial.silk.framework.ops.FContext
import java.net.{UnknownHostException, InetAddress}
import scala.io.Source
import xerial.silk


object Silk extends Guard with Logger {

  implicit class SilkSeqWrap[A](val a:Seq[A]) {
    def toSilk(implicit env:SilkEnv) : SilkSeq[A] = macro SilkMacros.mRawSeq[A]
  }

  implicit class SilkArrayWrap[A](val a:Array[A]) {
    def toSilk(implicit env:SilkEnv) : SilkSeq[A] = macro SilkMacros.mArrayToSilk[A]
  }

  implicit class SilkWrap[A:ClassTag](a:A) {
    def toSilkSingle : SilkSingle[A] = SilkException.NA
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
  def mixin[A](implicit ev:ClassTag[A], env:SilkEnv) : A = macro WorkflowMacros.mixinImpl[A]


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

  def loadFile(file:String)(implicit env:SilkEnv) : LoadFile = macro SilkMacros.loadImpl

  def newSilk[A](in:Seq[A])(implicit env:SilkEnv) : SilkSeq[A] = macro SilkMacros.mNewSilk[A]

  def scatter[A](in:Seq[A], numNodes:Int)(implicit env:SilkEnv) : SilkSeq[A] = macro SilkMacros.mScatter[A]

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

  class SilkInitializer(zkConnectString:String) extends Guard with Logger with IDUtil { self =>
    private val isReady = newCondition
    private var started = false
    private var inShutdownPhase = false
    private val toTerminate = newCondition

    private var env : SilkEnv = null

    private val t = new Thread(new Runnable {
      def run() {
        self.info("Initializing Silk")
        withConfig(Config.testConfig(zkConnectString)) {
          // Use a temporary node name to distinguish settings from SilkClient running in this node.
          val hostname = s"localhost-${UUID.randomUUID.prefix}"
          setLocalHost(Host(hostname, localhost.address))

          for{
            zk <- ZooKeeper.defaultZkClient
            actorSystem <- ActorService(localhost.address, IOUtil.randomPort)
            dataServer <- DataServer(IOUtil.randomPort, config.dataServerKeepAlive)
          } yield {
            env = new SilkEnvImpl(zk, actorSystem, dataServer)
            //Silk.setEnv(env)
            started = true

            guard {
              isReady.signalAll()
            }

            guard {
              while(!inShutdownPhase) {
                toTerminate.await()
              }
              started = false
            }
          }

        }
      }
    })
    t.setDaemon(true)

    Runtime.getRuntime.addShutdownHook(new Thread(new Runnable {
      def run() = { self.stop }
    }))


    private[Silk] def start : SilkEnv = {
      t.start
      guard {
        isReady.await()
      }
      if(!started)
        throw SilkException.error("Failed to initialize Silk")
      env
    }

    def stop {
      guard {
        if(started) {
          self.info("Terminating Silk")
          inShutdownPhase = true
          toTerminate.signalAll()
        }
      }
    }



  }


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
   * ID of this operation. To provide stable IDs for each run, the id is generated using
   * the input IDs and FContext.
   *
   * For
   *
   * @return
   */
  val id: UUID = SilkUtil.newUUIDOf(fc, inputs:_*)

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


}




/**
 * SilkSeq represents a sequence of elements. Silk data type contains FContext, class and variable names where
 * this SilkSeq is defined. In order to retrieve FContext information,
 * the operators in Silk use Scala macros to inspect the AST of the program code.
 *
 * Since methods defined using macros cannot be called within the same
 * class, each method in Silk must have a separate macro statement.
 *
 */
abstract class SilkSeq[+A] extends Silk[A] {

  import SilkMacros._

  def isSingle = false
  def isEmpty(implicit env:SilkEnv) : Boolean = macro mIsEmpty[A]
  def size : SilkSingle[Long] = macro mSize[A]

  // Map with resources
  def mapWith[A, B, R1](r1:Silk[R1])(f: (A, R1) => B) : SilkSeq[B] = macro mMapWith[A, B, R1]
  def mapWith[A, B, R1, R2](r1:Silk[R1], r2:Silk[R2])(f:(A, R1, R2) => B) : SilkSeq[B] = macro mMap2With[A, B, R1, R2]

  // FlatMap with resources
  def flatMapWith[A, B, R1](r1:Silk[R1])(f:(A, R1) => Silk[B]) : SilkSeq[B] = macro mFlatMapWith[A, B, R1]
  def flatMapWith[A, B, R1, R2](r1:Silk[R1], r2:Silk[R2])(f:(A, R1, R2) => Silk[B]) : SilkSeq[B] = macro mFlatMap2With[A, B, R1, R2]

  // For-comprehension
  def foreach[B](f:A=>B) : SilkSeq[B] = macro mForeach[A, B]
  def map[B](f: A => B): SilkSeq[B] = macro mMap[A, B]
  def flatMap[B](f: A => SilkSeq[B]): SilkSeq[B] = macro mFlatMap[A, B]
  def fMap[B](f: A=>GenTraversable[B]) : SilkSeq[B] = macro mFlatMapSeq[A, B]

  // Filtering in for-comprehension
  def filter(cond: A => Boolean): SilkSeq[A] = macro mFilter[A]
  def filterNot(cond: A => Boolean): SilkSeq[A] = macro mFilterNot[A]
  def withFilter(cond: A => Boolean) : SilkSeq[A] = macro mFilter[A] // Use filter

  // Extractor
  def head : SilkSingle[A] = macro mHead[A]
  def collect[B](pf: PartialFunction[A, B]): SilkSeq[B] = NA
  def collectFirst[B](pf: PartialFunction[A, B]): SilkSingle[Option[B]] = NA

  // List operations
  def distinct : SilkSeq[A] = NA

  // Block operations
  def split : SilkSeq[SilkSeq[A]] = macro mSplit[A]
  def concat[B](implicit asSilkSeq: A => Seq[B]) : SilkSeq[B] = macro mConcat[A, B]

  // Grouping
  def groupBy[K](f: A => K): SilkSeq[(K, SilkSeq[A])] = macro mGroupBy[A, K]


  // Aggregators
  def aggregate[B](z: B)(seqop: (B, A) => B, combop: (B, B) => B): SilkSingle[B] = NA
  def reduce[A1 >: A](f:(A1, A1) => A1) : SilkSingle[A1] = macro mReduce[A1]
  def reduceLeft[B >: A](op: (B, A) => B): SilkSingle[B] = NA // macro mReduceLeft[A, B]
  def fold[A1 >: A](z: A1)(op: (A1, A1) => A1): SilkSingle[A1] = NA // macro mFold[A, A1]
  def foldLeft[B](z: B)(op: (B, A) => B): SilkSingle[B] = NA // macro mFoldLeft[A, B]

  // Scan operations
  /**
   * Scan the elements with an additional variable z (e.g., a counter) , then produce another Silk data set
   * @param z initial value
   * @param op function that produces a pair (new z, another element)
   * @tparam B additional variable (e.g., counter)
   * @tparam C produced element
   */
  def scanLeftWith[B, C](z: B)(op : (B, A) => (B, C)): SilkSeq[C] = NA

  // Shuffle operators are used to describe concrete distributed operations (e.g., GroupBy, HashJoin, etc.)
  def shuffle[K](probe:A=>K, numPartition:Int) : SilkSeq[(K, SilkSeq[A])] = NA
  def shuffleReduce[A <: (K, SilkSeq[B]), K, B] : SilkSeq[(K, SilkSeq[B])] = NA


  // Joins
  def naturalJoin[B](other: SilkSeq[B])(implicit ev1: ClassTag[A], ev2: ClassTag[B]): SilkSeq[(A, B)] = macro mNaturalJoin[A, B]
  def join[K, B](other: SilkSeq[B], k1: A => K, k2: B => K) : SilkSeq[(A, B)]= macro mJoin[A, K, B]
  //def joinBy[B](other: SilkSeq[B], cond: (A, B) => Boolean) = macro mJoinBy[A, B]


  // Numeric operation
  def sum[A1>:A](implicit num: Numeric[A1]) : SilkSingle[A1] = macro mSum[A1]
  def product[A1 >: A](implicit num: Numeric[A1]) = macro mProduct[A1]
  def min[A1 >: A](implicit cmp: Ordering[A1]) = macro mMin[A1]
  def max[A1 >: A](implicit cmp: Ordering[A1]) = macro mMax[A1]
  def minBy[A1 >: A, B](f: (A1) => B)(implicit cmp: Ordering[B]) = macro mMinBy[A1, B]
  def maxBy[A1 >: A, B](f: (A1) => B)(implicit cmp: Ordering[B]) = macro mMaxBy[A1, B]


  // String
  def mkString(start: String, sep: String, end: String): SilkSingle[String] = macro mMkString[A]
  def mkString(sep: String): SilkSingle[String] = macro mMkStringSep[A]
  def mkString: SilkSingle[String] = macro mMkStringDefault[A]


  // Sampling
  def takeSample(proportion:Double) : SilkSeq[A] = macro mSampling[A]

  // Zipper
  def zip[B](other:SilkSeq[B]) : SilkSeq[(A, B)] = macro mZip[A, B]
  def zipWithIndex : SilkSeq[(A, Int)] = macro mZipWithIndex[A]

  // Sorting
  def sortBy[K](keyExtractor: A => K)(implicit ord: Ordering[K]): SilkSeq[A] = macro mSortBy[A, K]
  def sorted[A1 >: A](partitioner:Partitioner[A])(implicit ord: Ordering[A1]): SilkSeq[A1] = macro mSorted[A1]


  // Operations for gathering distributed data to a node
  /**
   * Collect all distributed data to the node calling this method. This method should be used only for small data.
   */
  def toSeq[A1>:A](implicit env:SilkEnv) : Seq[A1] = get[A1]

  /**
   * Collect all distributed data to the node calling this method. This method should be used only for small data.
   * @tparam A1
   * @return
   */
  def toArray[A1>:A](implicit ev:ClassTag[A1], env:SilkEnv) : Array[A1] = get[A1].toArray

  def toMap[K, V](implicit env:SilkEnv) : Map[K, V] = {
    val entries : Seq[(K, V)] = this.get[A].collect{ case (k, v) => (k -> v).asInstanceOf[(K, V)] }
    entries.toMap[K, V]
  }

  def get[A1>:A](implicit env:SilkEnv) : Seq[A1] = {
    // TODO switch the running cluster according to the env
    env.run(this)
  }

  def get(target:String)(implicit env:SilkEnv) : Seq[_] = {
    env.run(this, target)
  }

  def eval(implicit env:SilkEnv) : this.type = {
    env.eval(this)
    this
  }

}




object SilkSingle {
  import scala.language.implicitConversions
  //implicit def toSilkSeq[A:ClassTag](v:SilkSingle[A]) : SilkSeq[A] = NA
}


/**
 * Silk data class for a single element
 * @tparam A element type
 */
abstract class SilkSingle[+A] extends Silk[A] {

  import SilkMacros._

  def isSingle = true
  def size : Int = 1

  /**
   * Get the materialized result
   */
  def get(implicit env:SilkEnv) : A = {
    env.run(this).head
  }

  def get(target:String)(implicit env:SilkEnv) : Seq[_] = {
    env.run(this, target)
  }

  def eval(implicit env:SilkEnv): this.type = {
    env.eval(this)
    this
  }


  def map[B](f: A => B): SilkSingle[B] = macro mapSingleImpl[A, B]
  def flatMap[B](f: A => SilkSeq[B]): SilkSeq[B] = macro flatMapSingleImpl[A, B]
  def filter(cond: A => Boolean): SilkSingle[A] = macro mFilterSingle[A]
  def withFilter(cond: A => Boolean): SilkSingle[A] = macro mFilterSingle[A]


}

