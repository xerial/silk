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
import java.io.{ObjectInputStream, ByteArrayInputStream, ByteArrayOutputStream, ObjectOutputStream}
import xerial.core.util.DataUnit
import scala.language.existentials
import scala.reflect.ClassTag
import java.util.concurrent.{Executors, ConcurrentHashMap}
import scala.concurrent.Lock
import java.util.concurrent.locks.{Condition, ReentrantLock}
import java.util.UUID


object SilkContext {
  val defaultContext = new SilkContext
  def newUUID: UUID = UUID.randomUUID


}

/**
 * DistributedCache holds references to locations (slices) of the already evaluated results
 */
class DistributedCache {
  private val table = collection.mutable.Map[UUID, Seq[Slice[_]]]()

  def get(uuid:UUID) : Seq[Slice[_]] = table(uuid)
  def putIfAbsent(uuid:UUID, v: => Seq[Slice[_]]) {
    if(!table.contains(uuid))
      table += uuid -> v
  }
  def contains(uuid:UUID) = table.contains(uuid)
}



class SilkContext(val uuid : UUID = UUID.randomUUID) extends Logger {

  info(s"A new SilkContext: $uuid")

  @transient private val cache = new DistributedCache()

  def newSilk[A](in: Seq[A])(implicit ev: ClassTag[A]): SilkMini[A] = macro SilkMini.newSilkImpl[A]

  def get(uuid: UUID) = cache.get(uuid)
  def putIfAbsent(uuid: UUID, v: => Seq[Slice[_]]) { cache.putIfAbsent(uuid, v) }



  private[silk] def serializeOp[A](op: SilkMini[A]) = {
    val buf = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(buf)
    oos.writeObject(op)
    oos.close()
    val ba = buf.toByteArray
    ba
  }

  private[silk] def deserializeOp(b:Array[Byte]) : SilkMini[_] = {
    // Deserialize the operation
    val ois = new ObjectInputStream(new ByteArrayInputStream(b))
    val op = ois.readObject().asInstanceOf[SilkMini[_]]
    op.setContext(this)
    op
  }


  /**
   * Run and wait the result of this operation
   * @param op
   * @tparam A
   * @return
   */
  def eval[A](op: SilkMini[A]): Seq[Slice[A]] = {
    info(s"eval: ${op}")

    // TODO: Make this part asynchronous
    run(op)
    get(op.uuid).asInstanceOf[Seq[Slice[A]]]
  }


  def run[A](op: SilkMini[A]) {
    if (cache.contains(op.uuid))
      return

    val ba = serializeOp(op)
    scheduler.submit(this, Task(ba))
  }

  private val scheduler = new Scheduler
}




/**
 * A unit of distributed operation.
 * @param opBinary
 */
case class Task(opBinary: Array[Byte]) {

  def estimateRequiredResource : (Int, Long) = (1, 64 * 1024 * 1024)

}


case class WorkerResource(host:Host, cpu:Int, memory:Long) {

  private def ensureSameHost(other:WorkerResource) {
    require(this.host == other.host)
  }

  def -(other:WorkerResource) : WorkerResource = {
    ensureSameHost(other)
    WorkerResource(host, cpu - other.cpu, memory - other.memory)
  }

  def +(other:WorkerResource) : WorkerResource = {
    ensureSameHost(other)
    WorkerResource(host, cpu + other.cpu, memory + other.memory)
  }

}

class Worker(val host:Host) extends Logger {


  def resource = WorkerResource(host, 2, 1 * 1024 * 1024) // 2CPUs, 1G memory

  /**
   * Execute and wait until the result becomes available at some host
   * @param v
   * @tparam A
   * @return
   */
  private def evalRecursively[A](sc:SilkContext, h: Host, v: Any): Seq[Slice[A]] = {
    v match {
      case s: SilkMini[_] =>
        s.setContext(sc)
        s.slice.asInstanceOf[Seq[Slice[A]]]
      case s: Seq[_] =>
        Seq(RawSlice(h, 0, s.asInstanceOf[Seq[A]]))
      case e =>
        Seq(RawSlice(h, 0, Seq(e.asInstanceOf[A])))
    }
  }

  private def flattenSlices(in:Seq[Seq[Slice[_]]]) : Seq[Slice[_]] = {
    var counter = 0
    val result = for(ss <- in; s <- ss) yield {
      val r = RawSlice(s.host, counter, s.data)
      counter += 1
      r
    }
    result
  }

  private def evalAtRemote[U](sc:SilkContext, op:SilkMini[_], slice:Slice[_])(f: (SilkContext, Slice[_]) => U) : U = {
    debug(s"Get slice (opID:${op.uuid}) at ${slice.host}")
    f(sc, slice)
  }


  private def evalSlice(sc:SilkContext, in:SilkMini[_]): Seq[Slice[_]] = {
    in.setContext(sc)
    // Evaluate slice
    in.slice
  }


  def execute(sc:SilkContext, task:Task) = {

    // Deserialize the operation
    val op = sc.deserializeOp(task.opBinary)
    trace(s"submitted: ${op}, byte size: ${DataUnit.toHumanReadableFormat(task.opBinary.length)}")

    def fwrap[P,Q](f:P=>Q) = f.asInstanceOf[Any=>Any]
    def rwrap[P,Q,R](f:(P,Q)=>R) = f.asInstanceOf[(Any,Any)=>Any]

    // TODO send the job to a remote machine
    val result : Seq[Slice[_]] = op match {
      case m@MapOp(fref, in, f, expr) =>
        // in: S1, S2, ...., Sk
        // output: S1.map(f), S2.map(f), ..., Sk.map(f)
        // TODO: Open a stage
        val r = for (slice <- evalSlice(sc, in)) yield {
          // Each slice must be evaluated at some host
          // TODO: Choose an appropriate host
          evalAtRemote(sc, op, slice) { (sc, slc) => slc.data.flatMap { e => evalRecursively(sc, slc.host, fwrap(f)(e)) } }
        }
        // TODO: Await until all of the sub stages terminates
        flattenSlices(r)
      case fm@FlatMapOp(fref, in, f, expr) =>
        val r = for (slice <- evalSlice(sc, in)) yield
          evalAtRemote(sc, op, slice) { (sc, slc) => slc.data.flatMap(e =>evalRecursively(sc, slc.host, fwrap(f)(e))) }
        flattenSlices(r)
      case fl@FilterOp(fref, in, f, expr) =>
        val r = for(slice <- evalSlice(sc, in)) yield
          evalAtRemote(sc, op, slice) { (sc, slc) => evalRecursively(sc, slc.host, slc.data.filter(e => f(e))) }
        flattenSlices(r)
//      case rs@RawSeq(fref, in) =>
//        // Create a distributed data set
//        val d = scatter(rs)
//        d.slice
      case jo @ JoinOp(fref, left, right) =>
        //val ls = evalSlice(left)
        //val rs = evalSlice(right)
        val keyParams = jo.keyParameterPairs
        debug(s"key params: ${jo.keyParameterPairs.mkString(", ")}")
        // TODO: Shuffle operation
        if(keyParams.length == 1) {
          val ka = keyParams.head._1
          val kb = keyParams.head._2
          left.setContext(sc)
          right.setContext(sc)
          val partitioner = { k : Int => k % 2 } // Simple partitioner
          val ls = ShuffleOp(left.fref, left, ka, partitioner)
          val rs = ShuffleOp(right.fref, right, kb, partitioner)
          val merger = MergeShuffleOp(fref, ls, rs)
          sc.run(merger)
          sc.get(merger.uuid)
        }
        else {
          warn("multiple primary keys are not supported yet")
          Seq.empty
        }
      case so @ ShuffleOp(fref, in, keyParam, partitioner) =>
        val shuffleSet = for(slice <- evalSlice(sc, in)) yield {
          evalAtRemote(sc, so, slice) { (sc, sl) =>
            val shuffled = for(e <- sl.data) yield {
              val key = keyParam.get(e)
              val partition = fwrap(partitioner)(key).asInstanceOf[Int]
              (key, partition) -> e
            }
            val slices = for(((key, partition), lst) <- shuffled.groupBy(_._1)) yield
              PartitionedSlice(sl.host, partition, lst.map{x => (x._1._1, x._2)})
            slices.toSeq
          }
        }
        // Merge partitions generated from a slice
        val partitions = for((pid, slices) <- shuffleSet.flatten.groupBy(_.index)) yield {
          val hi = slices.head.index % hostList.length
          val h = hostList(hi)
          PartitionedSlice(h, hi, slices.flatMap(_.data))
        }
        debug(s"partitions:${partitions}")
        partitions.toSeq
      case mo @ MergeShuffleOp(fref, left, right) =>
        val l = evalSlice(sc, left).sortBy(_.index)
        val r = evalSlice(sc, right).sortBy(_.index)
        val joined =for(le <- l; re <- r.filter(_.index == le.index)) yield {
          // TODO hash-join
          for((lkey, ll) <- le.data; (rkey, rr) <- re.data if lkey == rkey) yield {
            (ll, rr)
          }
        }
        debug(s"joined $joined")
        Seq(RawSlice(Host("localhost"), 0, joined.flatten))
      case ReduceOp(fref, in, f, expr) =>
        val r = for(slice <- evalSlice(sc, in)) yield {
          // Reduce at each host
          val red = evalAtRemote(sc, op, slice) { (sc, slc) => slc.data.reduce(rwrap(f)) }
          // TODO: Create new reducers
          //evalRecursively(in).reduce{evalSingleRecursively(f.asInstanceOf[(Any,Any)=>Any](_, _))}
        }
        Seq(RawSlice(Host("localhost"), 0, Seq(r.reduce(rwrap(f)))))
      case _ =>
        warn(s"unknown op: ${op}")
        Seq.empty
    }

    sc.putIfAbsent(op.uuid, result)
  }

//  def scatter[A: ClassTag](rs: RawSeq[A]): DistributedSeq[A] = {
//    val numSlices = 2 // TODO retrieve default number of slices
//    val sliceSize = (rs.in.size + (numSlices - 1)) / numSlices
//    val slices = for ((slice, i) <- rs.in.sliding(sliceSize, sliceSize).zipWithIndex) yield {
//      val h = hostList(i % hostList.size) // round-robin split
//      // TODO: Send data to remote hosts
//      RawSlice(h, i, slice)
//    }
//    DistributedSeq[A](rs.fref, sc.newID, slices.toIndexedSeq)
//  }

}


/**
 * Manages list of available machine resources
 */
class ResourceManager {

  private val lock = new ReentrantLock()
  private val update = lock.newCondition
  private val availableResources = collection.mutable.Map[Host, WorkerResource]()

  /**
   * Scoped lock
   * @param f
   * @tparam U
   * @return
   */
  def guard[U](f: => U): U = {
    try {
      lock.lock
      f
    }
    finally
      lock.unlock
  }

  /**
   * Acquire the specified amount of resources from some host. This operation is blocking until
   * the resource will be available
   * @param cpu
   * @param memory
   * @return
   */
  def acquireResource(cpu:Int, memory:Long) : WorkerResource = {
    guard {
      @volatile var done = false
      var acquired : WorkerResource = null
      while(!done) {
        // TODO fair scheduling
        val r = availableResources.values.find(r => r.cpu >= cpu && r.memory >= memory)
        r match {
          case Some(x) =>
            acquired = WorkerResource(x.host, cpu, memory)
            val remaining = x - acquired
            availableResources += x.host -> remaining
            done = true
          case None =>
            update.await() // Await until new resource will be available
        }
      }
      acquired
    }
  }

  def addResource(r:WorkerResource) = releaseResource(r)

  def releaseResource(r:WorkerResource) {
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

  private val hostList = Seq(Host("host1"), Host("host2"))
  // TODO make these workers run at remote hosts
  private val workers = hostList.map(new Worker(_))
  private val resourceManager = new ResourceManager
  for(w <- workers) resourceManager.addResource(w.resource)

  private val threadManager = Executors.newCachedThreadPool()

  def submit(sc:SilkContext, task: Task) {
    // TODO push the task to the queue

    // The following code should run at the SilkMaster

    threadManager.submit(new Runnable {
      def run() {
        // Estimate the required resource
        val (cpu, mem) = task.estimateRequiredResource
        // Acquire resource (blocking operation)
        val res = resourceManager.acquireResource(cpu, mem)

        for(w <- workers.find(_.host == res.host) orElse { throw sys.error(s"invalid host ${res.host}")} ) {
          w.execute(sc, task)
        }
      }
    })

  }



}


case class CallGraph(nodes:Seq[SilkMini[_]], edges:Map[UUID, Seq[UUID]]) {

  override def toString = {
    val s = new StringBuilder
    s append "[nodes]\n"
    for(n <- nodes)
      s append s" $n\n"

    s append "[edges]\n"
    for((src, lst) <- edges.toSeq.sortBy(_._1); dest <- lst.sorted) {
      s append s" ${src} -> ${dest}\n"
    }
    s.toString
  }
}

object SilkMini {


  def newUUIDOf[A](in:Seq[A]) : UUID = {
    val b = new ByteArrayOutputStream
    val os = new ObjectOutputStream(b)
    for(x <- in)
      os.writeObject(x)
    os.close
    UUID.nameUUIDFromBytes(b.toByteArray)
  }



  private class CallGraphBuilder {
    var nodeTable = collection.mutable.Map[UUID, SilkMini[_]]()
    var edgeTable = collection.mutable.Map[UUID, Set[UUID]]()

    def containNode[A](n:SilkMini[A]) : Boolean = {
      nodeTable.contains(n.uuid)
    }

    def addNode[A](n:SilkMini[A]) {
      nodeTable += n.uuid -> n
    }

    def addEdge[A, B](from:SilkMini[A], to:SilkMini[B]) {
      addNode(from)
      addNode(to)
      val outNodes = edgeTable.getOrElseUpdate(from.uuid, Set.empty)
      edgeTable += from.uuid -> (outNodes + to.uuid)
    }

    def result : CallGraph = {
      new CallGraph(nodeTable.values.toSeq.sortBy(_.uuid), edgeTable.map{case (i, lst) => i -> lst.toSeq}.toMap)
    }
  }


  def createCallGraph[A](op:SilkMini[A]) = {
    val g = new CallGraphBuilder

    def loop(node:SilkMini[_]) {
      if(!g.containNode(node)) {
        for(in <- node.inputs) {
          loop(in)
          g.addEdge(in, node)
        }
      }
    }
    loop(op)
    g.result
  }



  class MacroHelper[C<:Context](val c: C) {
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

    def removeDoubleReify(tree:c.Tree) = {
      RemoveDoubleReify.transform(tree)
    }

    def createFRef: c.Expr[FRef[_]] = {
      val m = c.enclosingMethod
      val methodName = m match {
        case DefDef(mod, name, _, _, _, _) =>
          name.decoded
        case _ => "unknown"
      }

      val mne = c.Expr[String](Literal(Constant(methodName)))
      val self = c.Expr[Class[_]](This(tpnme.EMPTY))
      val vd = findValDef
      val vdTree = vd.map{ v =>
        val nme = c.Expr[String](Literal(Constant(v.name.decoded)));
        reify { Some(nme.splice) }
      } getOrElse { reify { None }}
      //println(s"vd: ${showRaw(vdTree)}")
      reify {
        FRef(self.splice.getClass, mne.splice, vdTree.splice)
      }
    }

    // Find a target variable of the operation result by scanning closest ValDefs
    def findValDef : Option[ValOrDefDef] = {

      def print(p:c.Position) = s"${p.line}(${p.column})"

      val prefixPos = c.prefix.tree.pos

      class Finder extends Traverser {

        var enclosingDef : List[ValOrDefDef] = List.empty
        var cursor : c.Position = null

        private def contains(p:c.Position, start:c.Position, end:c.Position) =
          start.precedes(p) && p.precedes(end)

        override def traverse(tree: Tree) {
          if(tree.pos.isDefined)
            cursor = tree.pos
          tree match {
            // Check whether the rhs of variable definition contains the prefix expression
            case vd @ ValDef(mod, varName, tpt, rhs) =>
              // Record the start and end positions of the variable definition block
              val startPos = vd.pos
              super.traverse(rhs)
              val endPos = cursor
              //println(s"val $varName range:${print(startPos)} - ${print(endPos)}: ${print(contextPos)}, prefix: ${print(prefixPos)}")
              if(contains(prefixPos, startPos, endPos)) {
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
   * the value holder of SilkContext. To avoid double registration, this method retrieves
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
      val sc = c.prefix.splice.asInstanceOf[SilkContext]
      val input = in.splice
      val fref = frefExpr.splice
      //val id = sc.seen.getOrElseUpdate(fref, sc.newID)
      val r = RawSeq(fref, input)(ev.splice)
      r.setContext(sc)
      sc.putIfAbsent(r.uuid, Seq(RawSlice(Host("localhost"), 0, input)))
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

  def filterImpl[A](c:Context)(f:c.Expr[A=>Boolean]) = {
    newOp[A=>Boolean, A](c)(c.universe.reify { FilterOp }.tree, f)
  }


  def naturalJoinImpl[A:c.WeakTypeTag, B](c: Context)(other: c.Expr[SilkMini[B]])(ev1:c.Expr[scala.reflect.ClassTag[A]], ev2:c.Expr[scala.reflect.ClassTag[B]]) : c.Expr[SilkMini[(A, B)]] = {
    import c.universe._

    val helper = new MacroHelper(c)
    val fref = helper.createFRef
    reify {
      JoinOp(fref.splice, c.prefix.splice.asInstanceOf[SilkMini[A]], other.splice)(ev1.splice , ev2.splice)
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
abstract class SilkMini[+A: ClassTag](val fref: FRef[_], val uuid:UUID = UUID.randomUUID) extends Serializable with Logger {

  @transient var sc: SilkContext = null

  def inputs : Seq[SilkMini[_]] = Seq.empty
  def getFirstInput: Option[SilkMini[_]] = None

  def setContext(newSC: SilkContext) = {
    this.sc = newSC
  }
  def getContext = sc

  /**
   * Compute slices, the results of evaluating this operation.
   * @return
   */
  def slice[A1 >:A]: Seq[Slice[A1]] = sc.eval(this)

  override def toString = {
    val cl = this.getClass
    val schema = ObjectSchema(cl)
    val params = for {p <- schema.constructor.params
                      if p.name != "sc" && p.valueType.rawType != classOf[ClassTag[_]]
                      v = p.get(this) if v != null} yield {
      if (classOf[ru.Expr[_]].isAssignableFrom(p.valueType.rawType)) {
        s"${v}[${v.toString.hashCode}]"
      }
      else if (classOf[SilkMini[_]].isAssignableFrom(p.valueType.rawType)) {
        s"[${v.asInstanceOf[SilkMini[_]].uuid}]"
      }
      else
        v
    }

    val prefix = s"[$uuid]"
    val s = s"${cl.getSimpleName}(${params.mkString(", ")})"
    val fv = freeVariables
    val fvStr = if (fv == null || fv.isEmpty) "" else s"{${fv.mkString(", ")}}|= "
    s"${prefix}${fvStr}$s"
  }


  def map[B](f: A => B): SilkMini[B] = macro mapImpl[A, B]
  def flatMap[B](f: A => SilkMini[B]): SilkMini[B] = macro flatMapImpl[A, B]
  def filter(f:A=>Boolean):SilkMini[A] = macro filterImpl[A]
  def naturalJoin[B](other:SilkMini[B])(implicit ev1:ClassTag[A], ev2:ClassTag[B]) : SilkMini[(A,B)] = macro naturalJoinImpl[A, B]

  def eval[A1>:A]: Seq[A1] = slice[A1].flatMap(sl => sl.data)

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
 * Function reference
 */

case class FRef[A](owner: Class[A], name: String, localValName:Option[String]) {

  override def toString = s"${owner.getSimpleName}.$name${localValName.map(x => s"#$x") getOrElse ""}"

  def refID: String = s"${owner.getName}#$name"
}



case class Host(name: String) {
  override def toString = name
}


abstract class Slice[+A](val host: Host, val index:Int) {
  def data: Seq[A]
}
case class RawSlice[A](override val host: Host, override val index:Int, data: Seq[A]) extends Slice[A](host, index)

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
case class PartitionedSlice[A](override val host:Host, override val index:Int, data:Seq[A]) extends Slice[A](host, index) {
  def partitionID = index
}

// Abstraction of distributed data set

// SilkMini[A].map(f:A=>B) =>  f(Slice[A]_1, ...)* =>  Slice[B]_1, ... => SilkMini[B]
// SilkMini -> Slice* ->


case class RawSeq[+A: ClassTag](override val fref: FRef[_], @transient in: Seq[A])
  extends SilkMini[A](fref, newUUIDOf(in)) {

}


case class DistributedSeq[+A: ClassTag](override val fref: FRef[_], slices: Seq[Slice[A]])
  extends SilkMini[A](fref) {

  override def slice[A1>:A] = slices
}


case class MapOp[A, B: ClassTag](override val fref: FRef[_], in: SilkMini[A], f: A => B, @transient fe: ru.Expr[A => B])
  extends SilkMini[B](fref)
  with SplitOp[A => B, A, B] {
}

case class FilterOp[A:ClassTag](override val fref:FRef[_], in:SilkMini[A], f:A=>Boolean, @transient fe:ru.Expr[A=>Boolean])
  extends SilkMini[A](fref)
  with SplitOp[A=>Boolean, A, A]


case class FlatMapOp[A, B: ClassTag](override val fref: FRef[_], in: SilkMini[A], f: A => SilkMini[B], @transient fe: ru.Expr[A => SilkMini[B]])
  extends SilkMini[B](fref)
  with SplitOp[A => SilkMini[B], A, B] {
}

case class JoinOp[A:ClassTag, B:ClassTag](override val fref:FRef[_], left:SilkMini[A], right:SilkMini[B])
  extends SilkMini[(A, B)](fref) {
  override def inputs = Seq(left, right)
  override def getFirstInput = Some(left)

  import ru._

  def keyParameterPairs = {
    val lt = ObjectSchema.of[A]
    val rt = ObjectSchema.of[B]
    val lp = lt.constructor.params
    val rp = rt.constructor.params
    for(pl <- lp; pr <- rp if (pl.name == pr.name) && pl.valueType == pr.valueType) yield (pl, pr)
  }
}


case class ShuffleOp[A:ClassTag, K](override val fref:FRef[_], in:SilkMini[A], keyParam:Parameter, partitioner:K=>Int)
  extends SilkMini[A](fref)

case class MergeShuffleOp[A:ClassTag, B:ClassTag](override val fref:FRef[_], left:SilkMini[A], right:SilkMini[B])
  extends SilkMini[(A, B)](fref) {
  override def inputs = Seq(left, right)
  override def getFirstInput = Some(left)
}


case class ReduceOp[A: ClassTag](override val fref: FRef[_], in: SilkMini[A], f: (A, A) => A, @transient fe: ru.Expr[(A, A) => A])
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
