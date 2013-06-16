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
import xerial.lens.{Parameter, ObjectSchema}
import xerial.core.log.Logger
import xerial.silk.{MacroUtil}
import java.io._
import xerial.core.util.DataUnit
import scala.language.existentials
import scala.reflect.ClassTag
import java.util.concurrent.Executors
import java.util.UUID
import scala.Some
import xerial.silk.framework._
import scala.Some
import xerial.silk.mini.EvalOpTask
import xerial.silk.mini.FContext
import xerial.silk.mini.WorkerResource
import xerial.silk.mini.CallGraph
import xerial.silk.mini.EvalSliceTask
import xerial.silk.mini.PartitionedSlice
import xerial.silk.mini.RawSlice
import xerial.silk.mini.ValType
import xerial.silk.framework.ops._
import scala.Some
import xerial.silk.mini.EvalOpTask
import xerial.silk.mini.FContext
import xerial.silk.mini.WorkerResource
import xerial.silk.mini.CallGraph
import xerial.silk.mini.EvalSliceTask
import xerial.silk.mini.LoadFile
import xerial.silk.mini.PartitionedSlice
import xerial.silk.mini.RawSlice
import xerial.silk.mini.ValType
import scala.Some
import xerial.silk.mini.EvalOpTask
import xerial.silk.mini.FContext
import xerial.silk.mini.WorkerResource
import xerial.silk.mini.CallGraph
import xerial.silk.mini.EvalSliceTask
import xerial.silk.mini.LoadFile
import xerial.silk.mini.PartitionedSlice
import xerial.silk.mini.RawSlice
import xerial.silk.mini.ValType
import scala.Some
import xerial.silk.mini.EvalOpTask
import xerial.silk.mini.FContext
import xerial.silk.mini.WorkerResource
import xerial.silk.mini.CallGraph
import xerial.silk.mini.EvalSliceTask
import xerial.silk.mini.LoadFile
import xerial.silk.mini.PartitionedSlice
import xerial.silk.mini.RawSlice
import xerial.silk.mini.ValType
import scala.Some
import xerial.silk.mini.EvalOpTask
import xerial.silk.framework.ops.RawSeq
import xerial.silk.mini.FContext
import xerial.silk.framework.ops.MergeShuffleOp
import xerial.silk.mini.WorkerResource
import xerial.silk.framework.ops.JoinOp
import xerial.silk.framework.ops.FlatMapOp
import xerial.silk.mini.CallGraph
import xerial.silk.mini.EvalSliceTask
import xerial.silk.framework.ops.ShuffleOp
import xerial.silk.mini.LoadFile
import xerial.silk.mini.PartitionedSlice
import xerial.silk.mini.RawSlice
import xerial.silk.framework.ops.ReduceOp
import xerial.silk.mini.ValType
import xerial.silk.framework.ops.FilterOp
import xerial.silk.framework.ops.MapOp


package object mini {


}

object Workflow {

  def mixinImpl[A:c.WeakTypeTag](c:Context)(ev:c.Expr[ClassTag[A]]) = {
    import c.universe._
    val self = c.Expr[Class[_]](This(tpnme.EMPTY))
    val at = c.weakTypeOf[A]
    val t : c.Tree = reify {
      new WithSession(self.splice.asInstanceOf[Workflow].session) with DummyWorkflow
    }.tree

    object replace extends Transformer {
      override def transform(tree: Tree) = {
        tree match {
          case id @ Ident(nme) if nme.decoded == "DummyWorkflow" =>
            Ident(newTypeName(at.typeSymbol.name.decoded))
          case other => super.transform(tree)
        }
      }
    }
    val replaced = replace.transform(t)
    c.Expr[A](replaced)
  }

}

/**
 * Used in mixinImpl macro to find the code replacement target
 */
private[silk] trait DummyWorkflow { this:Workflow =>

}

trait Workflow extends Serializable {
  @transient implicit val session : SilkSession

  implicit class Runner[A](op:SilkMini[A]) {
    def run = op.eval(session)
  }

  /**
   * Import another workflow trait as a mixin to this class. The imported workflow shares the same session
   * @param ev
   * @tparam A
   * @return
   */
  def mixin[A](implicit ev:ClassTag[A]) : A = macro Workflow.mixinImpl[A]

}

class MyWorkflow extends Workflow {
  @transient implicit val session = new SilkSession
}

class WithSession(val session:SilkSession) extends Workflow









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
      val f = new SilkFutureMultiThread[Seq[Slice[_]]]()
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








/**
 * A unit of distributed operation.
 */
sealed trait EvalTask extends Serializable {
  val opBinary : Array[Byte]
  def estimateRequiredResource: (Int, Long)
}

case class EvalOpTask(opBinary:Array[Byte]) extends EvalTask {
  def estimateRequiredResource: (Int, Long) = (1, -1)
}

case class EvalSliceTask(slice:Slice[_], opBinary:Array[Byte]) extends EvalTask {
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
  def hosts = Seq(Host("host1", "127.0.0.1"), Host("host2", "127.0.0.1"))

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
    val b = SilkMini.serializeFunc(f)

    debug(s"Eval slice (opID:${op.idPrefix}, host:${slice.host}) at ${host} function ${f.getClass.getSimpleName} size:${b.length}")
    val fd = SilkMini.deserializeFunc(b).asInstanceOf[(SilkSession, Slice[_]) => U]
    fd(ss, slice)
  }


  private def evalSlice(ss: SilkSession, in: SilkMini[_]): Seq[Slice[_]] = {
    // Evaluate slice
    // TODO: parallel evaluation
    in.slice(ss)
  }


  def execute(ss: SilkSession, task: EvalTask) = {
    task match {
      case e @ EvalOpTask(b) => executeSilkOp(ss, e)
      case e @ EvalSliceTask(slice, b) => executeSliceOp(ss, e)
    }
  }

  def executeSliceOp(ss:SilkSession, task:EvalSliceTask) = {
    val fd = SilkMini.deserializeFunc(task.opBinary).asInstanceOf[(SilkSession, Slice[_]) => Any]
    fd(ss, task.slice)
  }

  def executeSilkOp(ss:SilkSession, task:EvalOpTask) = {

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
            debug("no resource is available. waiting...")
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

  def submit(sc: SilkSession, task: EvalTask) {
    // TODO push the task to the queue

    // The following code should run at the SilkMaster
    debug(s"Received a new task: $task")
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

  private val idToSilkTable = nodes.map(x => x.uuid -> x).toMap
  private val silkToIdTable = nodes.map(x => x -> x.uuid).toMap

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

  def childrenOf(op:SilkMini[_]) : Seq[SilkMini[_]] = {
    val childIDs = edges.getOrElse(op.uuid, Seq.empty)
    childIDs.map(cid => idToSilkTable(cid))
  }

  def descendantsOf(op:SilkMini[_]) : Set[SilkMini[_]] = {
    var seen = Set.empty[SilkMini[_]]
    def loop(current:SilkMini[_]) {
      if(seen.contains(current))
        seen += current
      for(child <- childrenOf(current))
        loop(child)
    }
    loop(op)
    seen
  }

}

object SilkMini extends Logger {


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

  def deserializeObj[A](b:Array[Byte]): A = {
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

    def createFContext: c.Expr[FContext[_]] = {
      val m = c.enclosingMethod
      val methodName = m match {
        case DefDef(mod, name, _, _, _, _) =>
          name.decoded
        case _ => "<init>"
      }

      val mne = c.literal(methodName)
      val self = c.Expr[Class[_]](This(tpnme.EMPTY))
      val vd = findValDef
      val vdTree = vd.map {
        v =>
          val nme = c.literal(v.name.decoded)
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
              //println(s"val $varName range:${print(startPos)} - ${print(endPos)}: prefix: ${print(prefixPos)}")
              if (contains(prefixPos, startPos, endPos)) {
                enclosingDef = vd :: enclosingDef
              }
            case other =>
              super.traverse(other)
          }
        }

        def enclosingValDef = enclosingDef.reverse.headOption
      }

      val f = new Finder()
      val m = c.enclosingMethod
      if(m == null)
        f.traverse(c.enclosingClass)
      else
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
  def newSilkImpl[A](c: Context)(in: c.Expr[Seq[A]])(ev: c.Expr[ClassTag[A]]): c.Expr[SilkMini[A]] = {
    import c.universe._

    val helper = new MacroHelper[c.type](c)
    //println(s"newSilk(in): ${in.tree.toString}")
    val frefExpr = helper.createFContext
    reify {
      val input = in.splice
      val fref = frefExpr.splice
      //val id = ss.seen.getOrElseUpdate(fref, ss.newID)
      val r = RawSeq(fref, input)(ev.splice)
      SilkMini.cache.putIfAbsent(r.uuid, Seq(RawSlice(Host("localhost", "127.0.0.1"), 0, input)))
      r
    }
  }

  def loadImpl[A](c:Context)(in:c.Expr[File])(ev:c.Expr[ClassTag[A]]) = {
    import c.universe._
    val helper = new MacroHelper[c.type](c)
    //println(s"newSilk(in): ${in.tree.toString}")
    val frefExpr = helper.createFContext
    reify {
      val fref = frefExpr.splice
      val r = LoadFile(fref, in.splice)(ev.splice)
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
    val frefTree = helper.createFContext.tree.asInstanceOf[c.Tree]
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
    val fref = helper.createFContext
    reify {
      JoinOp(fref.splice, c.prefix.splice.asInstanceOf[SilkMini[A]], other.splice)(ev1.splice, ev2.splice)
    }
  }

  def reduceImpl[A](c: Context)(f: c.Expr[(A, A) => A]) = {
    newOp[(A, A) => A, A](c)(c.universe.reify {
      ReduceOp
    }.tree, f)
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
  def reduce[A1 >: A](f:(A1, A1) => A1) : SilkMini[A1] = macro reduceImpl[A1]

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






















case class LoadFile[A: ClassTag](override val fref:FContext[_], file:File) extends SilkMini[A](fref)




