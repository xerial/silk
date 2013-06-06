//--------------------------------------
//
// SilkMini.scala
// Since: 2013/05/17 12:28 PM
//
//--------------------------------------

package xerial.silk.mini

import scala.reflect.runtime.{universe => ru}
import scala.language.experimental.macros
import scala.reflect.macros.Context
import xerial.lens.{Parameter, ObjectType, TypeUtil, ObjectSchema}
import xerial.core.log.Logger
import xerial.silk.{MacroUtil, Pending, NotAvailable, SilkException}
import java.io._
import xerial.core.util.DataUnit
import scala.language.existentials
import scala.reflect.ClassTag
import java.util.concurrent.{Executors, ConcurrentHashMap}
import java.util.concurrent.locks.{Condition, ReentrantLock}
import java.util.UUID
import scala.Some
import scala.Serializable
import java.lang.reflect.Constructor
import xerial.silk.core.ClosureSerializer


package object mini {


}



trait Workflow extends Serializable {
  @transient val session : SilkSession

  implicit class Runner[A](op:SilkMini[A]) {
    def run = op.eval(session)
  }

}

class MyWorkflow extends Workflow {
  @transient val session = new SilkSession
}



trait Guard {
  private val lock = new ReentrantLock
  protected def newCondition = lock.newCondition

  protected def guard[U](f: => U): U = {
    try {
      lock.lock
      f
    }
    finally
      lock.unlock
  }
}


/**
 * SilkFuture interface to postpone the result acquisition.
 * @tparam A
 */
trait SilkFuture[A] extends Responder[A] {
  /**
   * Supply the data value for this future. Any process awaiting result will be signalled after this method.
   * @param v
   */
  def set(v: A) : Unit

  /**
   * Get the result. This operation blocks until the result will be available.
   * @return
   */
  def get : A
}

/**
 * SilkFuture implementation for multi-threaded code.
 * @tparam A
 */
class SilkFutureMultiThread[A] extends SilkFuture[A] with Guard {
  @volatile private var holder: Option[A] = None
  private val notNull = newCondition

  def set(v: A) = {
    guard {
      holder = Some(v)
      notNull.signalAll
    }
  }

  def get : A = {
    var v : A = null.asInstanceOf[A]
    respond(v = _)
    v
  }

  def respond(k: (A) => Unit) {
    guard {
      while (holder.isEmpty) {
        notNull.await
      }
      k(holder.get)
    }
  }


}



trait DistributedCache {

  def contains(uuid: UUID): Boolean
  def get(uuid: UUID): SilkFuture[Seq[Slice[_]]]
  def putIfAbsent(uuid: UUID, v: => Seq[Slice[_]])
}


/**
 * DistributedCache holds references to locations (slices) of the already evaluated results
 */
class SimpleDistributedCache extends DistributedCache with Guard with Logger {
  private val table = collection.mutable.Map[UUID, Seq[Slice[_]]]()
  private val futureToResolve = collection.mutable.Map[UUID, SilkFuture[Seq[Slice[_]]]]()


  def get(uuid: UUID): SilkFuture[Seq[Slice[_]]] = guard {
    if (futureToResolve.contains(uuid)) {
      futureToResolve(uuid)
    }
    else {
      val f = new SilkFutureMultiThread[Seq[Slice[_]]]
      if (table.contains(uuid)) {
        f.set(table(uuid))
      }
      else
        futureToResolve += uuid -> f
      f
    }
  }

  def putIfAbsent(uuid: UUID, v: => Seq[Slice[_]]) {
    guard {
      if (!table.contains(uuid)) {
        val vv = v
        table += uuid -> vv
      }

      if (futureToResolve.contains(uuid)) {
        futureToResolve(uuid).set(v)
        futureToResolve -= uuid
      }
    }
  }

  def contains(uuid: UUID) = guard {
    table.contains(uuid)
  }
}


class SilkSession(val sessionID: UUID = UUID.randomUUID) extends Logger {

  info(s"A new SilkSession: $sessionID")
  import SilkMini._

  def newSilk[A](in: Seq[A])(implicit ev: ClassTag[A]): SilkMini[A] = macro SilkMini.newSilkImpl[A]

  def get(uuid: UUID) = cache.get(uuid)
  def putIfAbsent(uuid: UUID, v: => Seq[Slice[_]]) {
    debug(s"put uuid:${uuid}")
    cache.putIfAbsent(uuid, v)
  }



  /**
   * Run and wait the result of this operation
   * @param op
   * @tparam A
   * @return
   */
  def eval[A](op: SilkMini[A]): Seq[Slice[A]] = {
    info(s"eval: ${op}")

    // run(op) is non-blocking
    run(op)

    var result : Seq[Slice[A]] = null
    // cache.get blocks until the result is obtained
    for(r <- cache.get(op.uuid))
      result = r.asInstanceOf[Seq[Slice[A]]]
    result
  }


  def run[A](op: SilkMini[A]) {
    if (cache.contains(op.uuid)) {
      return
    }

    val ba = serializeOp(op)
    scheduler.submit(this, Task(ba))
  }


}


/**
 * A unit of distributed operation.
 * @param opBinary
 */
case class Task(opBinary: Array[Byte]) {

  def estimateRequiredResource: (Int, Long) = (1, -1)

}


case class WorkerResource(host: Host, cpu: Int, memory: Long) {

  private def ensureSameHost(other: WorkerResource) {
    require(this.host == other.host)
  }

  private def nz(memoryAmount: Long): Long = if (memoryAmount <= 0L) 0L else memoryAmount

  def -(other: WorkerResource): WorkerResource = {
    ensureSameHost(other)
    WorkerResource(host, cpu - other.cpu, memory - nz(other.memory))
  }

  def +(other: WorkerResource): WorkerResource = {
    ensureSameHost(other)
    WorkerResource(host, cpu + other.cpu, memory + nz(other.memory))
  }

}

object Worker {
  def hosts = Seq(Host("host1"), Host("host2"))

  def fwrap[P, Q](f: P => Q) = f.asInstanceOf[Any => Any]
  def rwrap[P, Q, R](f: (P, Q) => R) = f.asInstanceOf[(Any, Any) => Any]

  /**
   * Execute and wait until the result becomes available at some host
   * @param v
   * @tparam A
   * @return
   */
  private def evalRecursively[A](ss: SilkSession, h: Host, v: Any): Seq[Slice[A]] = {
    v match {
      case s: SilkMini[_] =>
        s.slice(ss).asInstanceOf[Seq[Slice[A]]]
      case s: Seq[_] =>
        Seq(RawSlice(h, 0, s.asInstanceOf[Seq[A]]))
      case e =>
        Seq(RawSlice(h, 0, Seq(e.asInstanceOf[A])))
    }
  }


  private def execFlatMap[A,B](ss:SilkSession, slc:Slice[_], f:A => B) = {
    slc.data.flatMap {
      e => evalRecursively(ss, slc.host, fwrap(f)(e))
    }
  }

  private def execFilter(ss:SilkSession, slc:Slice[_], f:Any=>Boolean) = {
    evalRecursively(ss, slc.host, slc.data.filter(e => f(e)))
  }

}


class Worker(val host: Host) extends Logger {

  import Worker._

  def resource = WorkerResource(host, 2, 1 * 1024 * 1024) // 2CPUs, 1G memory

  private def flattenSlices(in: Seq[Seq[Slice[_]]]): Seq[Slice[_]] = {
    var counter = 0
    val result = for (ss <- in; s <- ss) yield {
      val r = RawSlice(s.host, counter, s.data)
      counter += 1
      r
    }
    result
  }

  private def evalAtRemote[U](ss: SilkSession, op: SilkMini[_], slice: Slice[_])(f: (SilkSession, Slice[_]) => U): U = {
    debug(s"Eval slice (opID:${op.idPrefix}) at ${slice.host}")
    val b = SilkMini.serializeFunc(f)
    trace(s"serialized ${f.getClass} size:${b.length}")
    val fd = SilkMini.deserializeFunc(b).asInstanceOf[(SilkSession, Slice[_]) => U]
    fd(ss, slice)
  }


  private def evalSlice(ss: SilkSession, in: SilkMini[_]): Seq[Slice[_]] = {
    // Evaluate slice
    // TODO: parallel evaluation
    in.slice(ss)
  }


  def execute(ss: SilkSession, task: Task) = {

    // Deserialize the operation
    val op = SilkMini.deserializeOp(task.opBinary)
    trace(s"execute: ${op}, byte size: ${DataUnit.toHumanReadableFormat(task.opBinary.length)}")


    // TODO send the job to a remote machine
    val result: Seq[Slice[_]] = op match {
      case m@MapOp(fref, in, f, expr) =>
        // in: S1, S2, ...., Sk
        // output: S1.map(f), S2.map(f), ..., Sk.map(f)
        // TODO: Open a stage
        val r = for (slice <- evalSlice(ss, in)) yield {
          // TODO: Choose an appropriate host
          evalAtRemote(ss, op, slice) { execFlatMap(_, _, f) }
        }
        // TODO: Await until all of the sub stages terminates
        flattenSlices(r)
      case fm@FlatMapOp(fref, in, f, expr) =>
        val r = for (slice <- evalSlice(ss, in)) yield
          evalAtRemote(ss, op, slice) { execFlatMap(_, _, f) }
        flattenSlices(r)
      case fl@FilterOp(fref, in, f, expr) =>
        val r = for (slice <- evalSlice(ss, in)) yield
          evalAtRemote(ss, op, slice) { execFilter(_, _, f) }
        flattenSlices(r)
      case rs@RawSeq(fref, in) =>
        // TODO distribute the data set
        val r = rs.slice(ss)
        debug(s"rawseq $r")
        r
      case jo@JoinOp(fref, left, right) =>
        //val ls = evalSlice(left)
        //val rs = evalSlice(right)
        val keyParams = jo.keyParameterPairs
        debug(s"key params: ${jo.keyParameterPairs.mkString(", ")}")
        // TODO: Shuffle operation
        if (keyParams.length == 1) {
          val ka = keyParams.head._1
          val kb = keyParams.head._2
          val partitioner = {
            k: Int => k % 2
          } // Simple partitioner
          val ls = ShuffleOp(left.fref, left, ka, partitioner)
          val rs = ShuffleOp(right.fref, right, kb, partitioner)
          val merger = MergeShuffleOp(fref, ls, rs)
          ss.eval(merger)
        }
        else {
          warn("multiple primary keys are not supported yet")
          Seq.empty
        }
      case so@ShuffleOp(fref, in, keyParam, partitioner) =>
        val shuffleSet = for (slice <- evalSlice(ss, in)) yield {
          evalAtRemote(ss, so, slice) {
            (sc, sl) =>
              val shuffled = for (e <- sl.data) yield {
                val key = keyParam.get(e)
                val partition = fwrap(partitioner)(key).asInstanceOf[Int]
                (key, partition) -> e
              }
              val slices = for (((key, partition), lst) <- shuffled.groupBy(_._1)) yield
                PartitionedSlice(sl.host, partition, lst.map {
                  x => (x._1._1, x._2)
                })
              slices.toSeq
          }
        }
        // Merge partitions generated from a slice
        val hostList = Worker.hosts
        val partitions = for ((pid, slices) <- shuffleSet.flatten.groupBy(_.index)) yield {
          val hi = slices.head.index % hostList.length
          val h = hostList(hi)
          PartitionedSlice(h, hi, slices.flatMap(_.data))
        }
        debug(s"partitions:${partitions}")
        partitions.toSeq
      case mo@MergeShuffleOp(fref, left, right) =>
        val l = evalSlice(ss, left).sortBy(_.index)
        val r = evalSlice(ss, right).sortBy(_.index)
        val joined = for (le <- l; re <- r.filter(_.index == le.index)) yield {
          // TODO hash-join
          for ((lkey, ll) <- le.data; (rkey, rr) <- re.data if lkey == rkey) yield {
            (ll, rr)
          }
        }
        debug(s"joined $joined")
        Seq(RawSlice(Host("localhost"), 0, joined.flatten))
      case ReduceOp(fref, in, f, expr) =>
        val r = for (slice <- evalSlice(ss, in)) yield {
          // Reduce at each host
          val red = evalAtRemote(ss, op, slice) {
            (sc, slc) => slc.data.reduce(rwrap(f))
          }
          // TODO: Create new reducers
          //evalRecursively(in).reduce{evalSingleRecursively(f.asInstanceOf[(Any,Any)=>Any](_, _))}
        }
        Seq(RawSlice(Host("localhost"), 0, Seq(r.reduce(rwrap(f)))))
      case _ =>
        warn(s"unknown op: ${op}")
        Seq.empty
    }

    ss.putIfAbsent(op.uuid, result)
  }

  //  def scatter[A: ClassTag](rs: RawSeq[A]): DistributedSeq[A] = {
  //    val numSlices = 2 // TODO retrieve default number of slices
  //    val sliceSize = (rs.in.size + (numSlices - 1)) / numSlices
  //    val slices = for ((slice, i) <- rs.in.sliding(sliceSize, sliceSize).zipWithIndex) yield {
  //      val h = hostList(i % hostList.size) // round-robin split
  //      // TODO: Send data to remote hosts
  //      RawSlice(h, i, slice)
  //    }
  //    DistributedSeq[A](rs.fref, ss.newID, slices.toIndexedSeq)
  //  }

}


/**
 * Manages list of available machine resources
 */
class ResourceManager extends Guard with Logger {

  private val update = newCondition
  private val availableResources = collection.mutable.Map[Host, WorkerResource]()


  /**
   * Acquire the specified amount of resources from some host. This operation is blocking until
   * the resource will be available
   * @param cpu
   * @param memory
   * @return
   */
  def acquireResource(cpu: Int, memory: Long): WorkerResource = {
    guard {
      @volatile var acquired: WorkerResource = null
      while (acquired == null) {
        // TODO fair scheduling
        val r = availableResources.values.find(r => r.cpu >= cpu && r.memory >= memory)
        r match {
          case Some(x) =>
            acquired = WorkerResource(x.host, cpu, memory)
            val remaining = x - acquired
            availableResources += x.host -> remaining
          case None =>
            update.await() // Await until new resource will be available
        }
      }
      acquired
    }
  }

  def addResource(r: WorkerResource) = releaseResource(r)

  def releaseResource(r: WorkerResource) {
    guard {
      availableResources.get(r.host) match {
        case Some(x) =>
          availableResources += r.host -> (x + r)
        case None =>
          availableResources += r.host -> r
      }
      update.signalAll()
    }
  }

}


/**
 * Scheduler dispatches tasks to SilkClients at remote hosts
 */
class Scheduler extends Logger {

  private val hostList = Worker.hosts
  // TODO make these workers run at remote hosts
  private val workers = hostList.map(new Worker(_))
  private val resourceManager = new ResourceManager
  for (w <- workers) resourceManager.addResource(w.resource)

  private val threadManager = Executors.newCachedThreadPool()

  def submit(sc: SilkSession, task: Task) {
    // TODO push the task to the queue

    // The following code should run at the SilkMaster

    threadManager.submit(new Runnable {
      def run() {
        // Estimate the required resource
        val (cpu, mem) = task.estimateRequiredResource
        // Acquire resource (blocking operation)
        val res = resourceManager.acquireResource(cpu, mem)
        debug(s"acquired resource: ${res}")

        for (w <- workers.find(_.host == res.host) orElse {
          throw sys.error(s"invalid host ${res.host}")
        }) {
          w.execute(sc, task)
        }
      }
    })
  }


}


case class CallGraph(nodes: Seq[SilkMini[_]], edges: Map[UUID, Seq[UUID]]) {

  private def idPrefix(uuid:UUID) = uuid.toString.substring(0, 8)

  override def toString = {
    val s = new StringBuilder
    s append "[nodes]\n"
    for (n <- nodes)
      s append s" $n\n"

    s append "[edges]\n"
    for ((src, lst) <- edges.toSeq.sortBy(_._1); dest <- lst.sorted) {
      s append s" ${idPrefix(src)} -> ${idPrefix(dest)}\n"
    }
    s.toString
  }
}

object SilkMini {

  def newUUID: UUID = UUID.randomUUID
  val cache: DistributedCache = new SimpleDistributedCache
  val scheduler = new Scheduler


  def serializeObj(v:Any) = {
    val buf = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(buf)
    oos.writeObject(v)
    oos.close()
    val ba = buf.toByteArray
    ba
  }

  private def deserializeObj[A](b:Array[Byte]): A = {
    val ois = new ObjectInputStream(new ByteArrayInputStream(b))
    val op = ois.readObject().asInstanceOf[A]
    op

  }


  private [silk] def serializeFunc[A, B, C](f: (A, B) => C) = serializeObj(f)
  private [silk] def deserializeFunc[A, B, C](b:Array[Byte]) : (A, B) => C = deserializeObj(b)
  private[silk] def serializeOp[A](op: SilkMini[A]) = serializeObj(op)
  private[silk] def deserializeOp(b: Array[Byte]): SilkMini[_] = deserializeObj(b)


  def newUUIDOf[A](in: Seq[A]): UUID = {
    val b = new ByteArrayOutputStream
    val os = new ObjectOutputStream(b)
    for (x <- in)
      os.writeObject(x)
    os.close
    UUID.nameUUIDFromBytes(b.toByteArray)
  }


  private class CallGraphBuilder {
    var nodeTable = collection.mutable.Map[UUID, SilkMini[_]]()
    var edgeTable = collection.mutable.Map[UUID, Set[UUID]]()

    def containNode[A](n: SilkMini[A]): Boolean = {
      nodeTable.contains(n.uuid)
    }

    def addNode[A](n: SilkMini[A]) {
      nodeTable += n.uuid -> n
    }

    def addEdge[A, B](from: SilkMini[A], to: SilkMini[B]) {
      addNode(from)
      addNode(to)
      val outNodes = edgeTable.getOrElseUpdate(from.uuid, Set.empty)
      edgeTable += from.uuid -> (outNodes + to.uuid)
    }

    def result: CallGraph = {
      new CallGraph(nodeTable.values.toSeq.sortBy(_.uuid), edgeTable.map {
        case (i, lst) => i -> lst.toSeq
      }.toMap)
    }
  }


  def createCallGraph[A](op: SilkMini[A]) = {
    val g = new CallGraphBuilder

    def loop(node: SilkMini[_]) {
      if (!g.containNode(node)) {
        for (in <- node.inputs) {
          loop(in)
          g.addEdge(in, node)
        }
      }
    }
    loop(op)
    g.result
  }


  class MacroHelper[C <: Context](val c: C) {

    import c.universe._

    /**
     * Removes nested reifyTree application to Silk operations.
     */
    object RemoveDoubleReify extends Transformer {
      override def transform(tree: c.Tree) = {
        tree match {
          case Apply(TypeApply(s@Select(idt@Ident(q), termname), _), reified :: tail)
            if termname.decoded == "apply" && q.decoded.endsWith("Op")
          => c.unreifyTree(reified)
          case _ => super.transform(tree)
        }
      }
    }

    def removeDoubleReify(tree: c.Tree) = {
      RemoveDoubleReify.transform(tree)
    }

    def createFRef: c.Expr[FContext[_]] = {
      val m = c.enclosingMethod
      val methodName = m match {
        case DefDef(mod, name, _, _, _, _) =>
          name.decoded
        case _ => "unknown"
      }

      val mne = c.Expr[String](Literal(Constant(methodName)))
      val self = c.Expr[Class[_]](This(tpnme.EMPTY))
      val vd = findValDef
      val vdTree = vd.map {
        v =>
          val nme = c.Expr[String](Literal(Constant(v.name.decoded)));
          reify {
            Some(nme.splice)
          }
      } getOrElse {
        reify {
          None
        }
      }
      //println(s"vd: ${showRaw(vdTree)}")
      reify {
        FContext(self.splice.getClass, mne.splice, vdTree.splice)
      }
    }

    // Find a target variable of the operation result by scanning closest ValDefs
    def findValDef: Option[ValOrDefDef] = {

      def print(p: c.Position) = s"${p.line}(${p.column})"

      val prefixPos = c.prefix.tree.pos

      class Finder extends Traverser {

        var enclosingDef: List[ValOrDefDef] = List.empty
        var cursor: c.Position = null

        private def contains(p: c.Position, start: c.Position, end: c.Position) =
          start.precedes(p) && p.precedes(end)

        override def traverse(tree: Tree) {
          if (tree.pos.isDefined)
            cursor = tree.pos
          tree match {
            // Check whether the rhs of variable definition contains the prefix expression
            case vd@ValDef(mod, varName, tpt, rhs) =>
              // Record the start and end positions of the variable definition block
              val startPos = vd.pos
              super.traverse(rhs)
              val endPos = cursor
              //println(s"val $varName range:${print(startPos)} - ${print(endPos)}: ${print(contextPos)}, prefix: ${print(prefixPos)}")
              if (contains(prefixPos, startPos, endPos)) {
                enclosingDef = vd :: enclosingDef
              }
            case other =>
              super.traverse(other)
          }
        }

        def enclosingValDef = enclosingDef.reverse.headOption
      }

      val m = c.enclosingMethod
      val f = new Finder()
      f.traverse(m)
      f.enclosingValDef
    }

  }


  /**
   * Generating a new RawSeq instance of SilkMini[A] and register the input data to
   * the value holder of SilkSession. To avoid double registration, this method retrieves
   * enclosing method name and use it as a key for the cache table.
   * @param c
   * @param in
   * @tparam A
   * @return
   */
  def newSilkImpl[A](c: Context)(in: c.Expr[Seq[A]])(ev: c.Expr[scala.reflect.ClassTag[A]]): c.Expr[SilkMini[A]] = {
    import c.universe._

    val helper = new MacroHelper[c.type](c)
    //println(s"newSilk(in): ${in.tree.toString}")
    val frefExpr = helper.createFRef
    reify {
      val input = in.splice
      val fref = frefExpr.splice
      //val id = ss.seen.getOrElseUpdate(fref, ss.newID)
      val r = RawSeq(fref, input)(ev.splice)
      SilkMini.cache.putIfAbsent(r.uuid, Seq(RawSlice(Host("localhost"), 0, input)))
      r
    }
  }


  def newOp[F, Out](c: Context)(op: c.Tree, f: c.Expr[F]) = {
    import c.universe._

    val helper = new MacroHelper(c)
    val rmdup = helper.removeDoubleReify(f.tree.asInstanceOf[helper.c.Tree]).asInstanceOf[Tree]
    val checked = c.typeCheck(rmdup)
    val t = c.reifyTree(c.universe.treeBuild.mkRuntimeUniverseRef, EmptyTree, checked)
    val exprGen = c.Expr[ru.Expr[F]](t).tree
    val frefTree = helper.createFRef.tree.asInstanceOf[c.Tree]
    val e = c.Expr[SilkMini[Out]](Apply(Select(op, newTermName("apply")), List(frefTree, c.prefix.tree, f.tree, exprGen)))
    reify {
      val silk = e.splice
      silk
    }
  }

  def mapImpl[A, B](c: Context)(f: c.Expr[A => B]) = {
    newOp[A => B, B](c)(c.universe.reify {
      MapOp
    }.tree, f)
  }

  def flatMapImpl[A, B](c: Context)(f: c.Expr[A => SilkMini[B]]) = {
    newOp[A => SilkMini[B], B](c)(c.universe.reify {
      FlatMapOp
    }.tree, f)
  }

  def filterImpl[A](c: Context)(f: c.Expr[A => Boolean]) = {
    newOp[A => Boolean, A](c)(c.universe.reify {
      FilterOp
    }.tree, f)
  }


  def naturalJoinImpl[A: c.WeakTypeTag, B](c: Context)(other: c.Expr[SilkMini[B]])(ev1: c.Expr[scala.reflect.ClassTag[A]], ev2: c.Expr[scala.reflect.ClassTag[B]]): c.Expr[SilkMini[(A, B)]] = {
    import c.universe._

    val helper = new MacroHelper(c)
    val fref = helper.createFRef
    reify {
      JoinOp(fref.splice, c.prefix.splice.asInstanceOf[SilkMini[A]], other.splice)(ev1.splice, ev2.splice)
    }
  }

}

import SilkMini._


/**
 * Mini-implementation of the Silk framework.
 *
 * SilkMini is an abstraction of operations on data.
 *
 */
abstract class SilkMini[+A: ClassTag](val fref: FContext[_], val uuid: UUID = SilkMini.newUUID) extends Serializable with Logger {

  def inputs: Seq[SilkMini[_]] = Seq.empty
  def getFirstInput: Option[SilkMini[_]] = None

  def idPrefix : String = uuid.toString.substring(0, 8)

  /**
   * Compute slices, the results of evaluating this operation.
   * @return
   */
  def slice[A1 >: A](ss:SilkSession): Seq[Slice[A1]] = ss.eval(this)

  override def toString = {
    val cl = this.getClass
    val schema = ObjectSchema(cl)
    val params = for {p <- schema.constructor.params
                      if p.name != "ss" && p.valueType.rawType != classOf[ClassTag[_]]
                      v = p.get(this) if v != null} yield {
      if (classOf[ru.Expr[_]].isAssignableFrom(p.valueType.rawType)) {
        s"${v}[${v.toString.hashCode}]"
      }
      else if (classOf[SilkMini[_]].isAssignableFrom(p.valueType.rawType)) {
        s"[${v.asInstanceOf[SilkMini[_]].idPrefix}]"
      }
      else
        v
    }

    val prefix = s"[$idPrefix]"
    val s = s"${cl.getSimpleName}(${params.mkString(", ")})"
    val fv = freeVariables
    val fvStr = if (fv == null || fv.isEmpty) "" else s"{${fv.mkString(", ")}}|= "
    s"${
      prefix
    } ${
      fvStr
    }$s"
  }


  def map[B](f: A => B): SilkMini[B] = macro mapImpl[A, B]
  def flatMap[B](f: A => SilkMini[B]): SilkMini[B] = macro flatMapImpl[A, B]
  def filter(f: A => Boolean): SilkMini[A] = macro filterImpl[A]
  def naturalJoin[B](other: SilkMini[B])(implicit ev1: ClassTag[A], ev2: ClassTag[B]): SilkMini[(A, B)] = macro naturalJoinImpl[A, B]

  def eval[A1 >: A](ss:SilkSession): Seq[A1] = slice[A1](ss).flatMap(sl => sl.data)

  @transient val argVariable: Option[ValType] = None
  @transient val freeVariables: Seq[ValType] = Seq.empty

  def enclosingFunctionID: String = fref.refID

}


case class ValType(name: String, tpe: ru.Type) {
  override def toString = s"$name:${
    if (isSilkType) "*" else ""
  }$tpe"
  def isSilkType = {
    import ru._
    tpe <:< typeOf[SilkMini[_]]
  }
}

/**
 * Function context
 */
case class FContext[A](owner: Class[A], name: String, localValName: Option[String]) {

  def baseTrait : Class[_] = {

    val isAnonFun = owner.getSimpleName.contains("$anonfun")
    if(!isAnonFun)
      owner
    else {
      // The owner is a mix-in class
      owner.getInterfaces.headOption getOrElse owner
    }
  }

  override def toString = {
    s"${baseTrait.getSimpleName}.$name${localValName.map(x => s"#$x") getOrElse ""}"
  }

  def refID: String = s"${owner.getName}#$name"
}


case class Host(name: String) {
  override def toString = name
}


abstract class Slice[+A](val host: Host, val index: Int) {
  def data: Seq[A]
}
case class RawSlice[A](override val host: Host, override val index: Int, data: Seq[A]) extends Slice[A](host, index)

//case class RemoteSlice[A](override val host: Host, override val index:Int) extends Slice[A](host, index) {
//  def data : Seq[A] = {
//    // Connect to a remote data server
//
//
//  }
//}

/**
 * Partitioned slice has the same structure with RawSlice.
 * @param host
 * @param index
 * @param data
 * @tparam A
 */
case class PartitionedSlice[A](override val host: Host, override val index: Int, data: Seq[A]) extends Slice[A](host, index) {
  def partitionID = index
}

// Abstraction of distributed data set

// SilkMini[A].map(f:A=>B) =>  f(Slice[A]_1, ...)* =>  Slice[B]_1, ... => SilkMini[B]
// SilkMini -> Slice* ->


case class RawSeq[+A: ClassTag](override val fref: FContext[_], @transient in:Seq[A])
  extends SilkMini[A](fref, newUUIDOf(in)) {

}


case class DistributedSeq[+A: ClassTag](override val fref: FContext[_], slices: Seq[Slice[A]])
  extends SilkMini[A](fref) {

  override def slice[A1 >: A](ss:SilkSession) = slices
}


case class MapOp[A, B: ClassTag](override val fref: FContext[_], in: SilkMini[A], f: A => B, @transient fe: ru.Expr[A => B])
  extends SilkMini[B](fref)
  with SplitOp[A => B, A, B] {
}

case class FilterOp[A: ClassTag](override val fref: FContext[_], in: SilkMini[A], f: A => Boolean, @transient fe: ru.Expr[A => Boolean])
  extends SilkMini[A](fref)
  with SplitOp[A => Boolean, A, A]


case class FlatMapOp[A, B: ClassTag](override val fref: FContext[_], in: SilkMini[A], f: A => SilkMini[B], @transient fe: ru.Expr[A => SilkMini[B]])
  extends SilkMini[B](fref)
  with SplitOp[A => SilkMini[B], A, B] {
}

case class JoinOp[A: ClassTag, B: ClassTag](override val fref: FContext[_], left: SilkMini[A], right: SilkMini[B])
  extends SilkMini[(A, B)](fref) {
  override def inputs = Seq(left, right)
  override def getFirstInput = Some(left)

  import ru._

  def keyParameterPairs = {
    val lt = ObjectSchema.of[A]
    val rt = ObjectSchema.of[B]
    val lp = lt.constructor.params
    val rp = rt.constructor.params
    for (pl <- lp; pr <- rp if (pl.name == pr.name) && pl.valueType == pr.valueType) yield (pl, pr)
  }
}


case class ShuffleOp[A: ClassTag, K](override val fref: FContext[_], in: SilkMini[A], keyParam: Parameter, partitioner: K => Int)
  extends SilkMini[A](fref)

case class MergeShuffleOp[A: ClassTag, B: ClassTag](override val fref: FContext[_], left: SilkMini[A], right: SilkMini[B])
  extends SilkMini[(A, B)](fref) {
  override def inputs = Seq(left, right)
  override def getFirstInput = Some(left)
}


case class ReduceOp[A: ClassTag](override val fref: FContext[_], in: SilkMini[A], f: (A, A) => A, @transient fe: ru.Expr[(A, A) => A])
  extends SilkMini[A](fref)
  with MergeOp


trait SplitOp[F, P, A] extends Logger {
  self: SilkMini[A] =>

  import ru._

  val in: SilkMini[P]
  @transient val fe: ru.Expr[F]

  override def getFirstInput = Some(in)
  override def inputs = Seq(in)

  def functionClass: Class[Function1[_, _]] = {
    MacroUtil.mirror.runtimeClass(fe.staticType).asInstanceOf[Class[Function1[_, _]]]
  }

  @transient override val argVariable = {
    fe.tree match {
      case f@Function(List(ValDef(mod, name, e1, e2)), body) =>
        fe.staticType match {
          case TypeRef(prefix, symbol, List(from, to)) =>
            Some(ValType(name.decoded, from))
          case _ => None
        }
      case _ => None
    }
  }

  @transient override val freeVariables = {

    val fvNameSet = (for (v <- fe.tree.freeTerms) yield v.name.decoded).toSet
    val b = Seq.newBuilder[ValType]

    val tv = new Traverser {
      override def traverse(tree: ru.Tree) {
        def matchIdent(idt: Ident): ru.Tree = {
          val name = idt.name.decoded
          if (fvNameSet.contains(name)) {
            val tt: ru.Tree = MacroUtil.toolbox.typeCheck(idt, silent = true)
            b += ValType(idt.name.decoded, tt.tpe)
            tt
          }
          else
            idt
        }

        tree match {
          case idt@Ident(term) =>
            matchIdent(idt)
          case other => super.traverse(other)
        }
      }
    }
    tv.traverse(fe.tree)

    // Remove duplicate occurrences.
    b.result.distinct
  }

}


trait MergeOp
