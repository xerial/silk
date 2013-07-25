package xerial.silk.framework

import xerial.silk.{SilkUtil, SilkSeq, Silk, SilkException}
import xerial.silk.framework.ops._
import xerial.silk.framework.ops.ReduceOp
import xerial.silk.framework.ops.FilterOp
import xerial.silk.framework.ops.FlatMapOp
import xerial.silk.framework.ops.MapOp
import xerial.core.log.{LoggerFactory, Logger}
import xerial.silk.core.ClosureSerializer
import java.util.UUID
import scala.util.Random


trait ClassBoxAPI {
  def classLoader : ClassLoader
}


/**
 * ClassBoxComponent has a role to provide the current ClassBoxID and distribute
 * the ClassBox to cluster nodes.
 */
trait ClassBoxComponent {
  self: SilkFramework =>

  type ClassBoxType <: ClassBoxAPI

  /**
   * Get the current class box id
   */
  def classBoxID : UUID

  /**
   * Retrieve the class box having the specified id
   * @param classBoxID
   * @return
   */
  def getClassBox(classBoxID:UUID) : ClassBoxAPI

}


trait DefaultExecutor extends ExecutorComponent {
  self : SilkFramework
    with LocalTaskManagerComponent
    with LocalClientComponent
    with ClassBoxComponent
    with SliceStorageComponent =>

  type Executor = ExecutorImpl
  def executor : Executor = new ExecutorImpl

  class ExecutorImpl extends ExecutorAPI {}
}

/**
 * Executor receives a silk, optimize a plan, then submit evaluation tasks to the local task manager
 */
trait ExecutorComponent {
  self : SilkFramework
    with LocalTaskManagerComponent
    with LocalClientComponent
    with ClassBoxComponent
    with SliceStorageComponent =>

  type Executor <: ExecutorAPI
  def executor : Executor

  trait ExecutorAPI extends Logger {

    implicit class toGenFun[A,B](f:A=>B) {
      def toF1 : Any=>Any = f.asInstanceOf[Any=>Any]
      def toFlatMap : Any=>SilkSeq[Any] = f.asInstanceOf[Any=>SilkSeq[Any]]
      def toFilter : Any=>Boolean = f.asInstanceOf[Any=>Boolean]
    }

    def run[A](session:Session, silk: Silk[A]): Result[A] = {

      val dataSeq : Seq[Seq[A]] = for{
        future <- getSlices(silk)
      }
      yield {
        val slice = future.get
        trace(s"get slice: $slice in $silk")
        sliceStorage.retrieve(silk.id, slice).asInstanceOf[Seq[A]]
      }

      val result = dataSeq.flatten
      result
    }


    def getStage[A](op:Silk[A]) : StageInfo = {
      sliceStorage.getStageInfo(op).map { si =>
        si.status match {
          case StageStarted(ts) => si
          case StageFinished(ts) => si
          case StageAborted(cause, ts) => // TODO restart
            SilkException.error(s"stage of [${op.idPrefix}] has been aborted: $cause")
        }
      } getOrElse {
        // Start a stage for evaluating the Silk
        executor.startStage(op)
      }
    }

    private def startStage[A,Out](op:Silk[A], in:Silk[A], f:Seq[_]=>Out) = {
      val inputStage = getStage(in)
      val N = inputStage.numSlices
      val stageInfo = StageInfo(0, N, StageStarted(System.currentTimeMillis()))
      sliceStorage.setStageInfo(op, stageInfo)
      // TODO append par
      for(i <- (0 until N)) {
        // Get an input slice
        val inputSlice = sliceStorage.get(in.id, i).get
        // Send the slice processing task to a node close to the inputSlice location
        localTaskManager.submitEvalTask(classBoxID, locality=Seq(inputSlice.nodeName), op.id, in.id, inputSlice, f.toF1)
      }
      stageInfo
    }


    private def startReduceStage[A](op:Silk[A], in:Silk[A], reducer:Seq[_] => Any, aggregator:Seq[_]=>Any) = {
      val inputStage = getStage(in)
      val N = inputStage.numSlices
      // Determine the number of reducers to use. The default is 1/3 of the number of the input slices
      val R = ((N + (3-1))/ 3.0).toInt
      val W = (N + (R-1)) / R
      info(s"num reducers:$R, W:$W")

      // The outer reduce task produces only 1 slice
      val stageInfo = StageInfo(0, 1, StageStarted(System.currentTimeMillis()))

      // Evaluate the input slices in a new sub stage
      val subStageID = SilkUtil.newUUID
      for((sliceRange, i) <- (0 until N).sliding(W, W).zipWithIndex) {
        val sliceIndexSet = sliceRange.toIndexedSeq
        localTaskManager.submitReduceTask(classBoxID, subStageID, in.id, sliceIndexSet, i, reducer, aggregator)
      }
      // The final aggregate task
      localTaskManager.submitReduceTask(classBoxID, op.id, subStageID, (0 until R).toIndexedSeq, 0, reducer, aggregator)

      stageInfo
    }


    private def startShuffleStage[A, K](shuffleOp:ShuffleOp[A, K]) = {
      val inputStage = getStage(shuffleOp.in)
      val N = inputStage.numSlices
      val P = shuffleOp.partitioner.numPartitions
      val stageInfo = StageInfo(P, N, StageStarted(System.currentTimeMillis()))
      // Shuffle each input slice
      for(i <- (0 until N)) {
        val inputSlice = sliceStorage.get(shuffleOp.in.id, i).get
        localTaskManager.submitShuffleTask(classBoxID, Seq(inputSlice.nodeName), shuffleOp.id, shuffleOp.in.id, inputSlice, shuffleOp.partitioner)
      }
      stageInfo
    }

    def startStage[A](op:Silk[A]) : StageInfo = {
      info(s"Start stage: $op")
      try {
        op match {
          case RawSeq(id, fc, in) =>
            SilkException.error(s"RawSeq must be found in SliceStorage: $op")
          case m @ MapOp(id, fc, in, f, fe) =>
            val fc = f.toF1
            startStage(op, in, { _.map(fc) })
          case fo @ FilterOp(id, fc, in, f, fe) =>
            val fc = f.toFilter
            startStage(op, in, { _.filter(fc)})
          case ReduceOp(id, fc, in, f, fe) =>
            val fc = f.asInstanceOf[(Any,Any)=>Any]
            startReduceStage(op, in, { _.reduce(fc) }, { _.reduce(fc) })
          case fo @ FlatMapOp(id, fc, in, f, fe) =>
            val fc = f.toF1
            startStage(fo, in, { _.map(fc) })
          case SizeOp(id, fc, in) =>
            startReduceStage(op, in, { _.size }, { sizes:Seq[Int] => sizes.map(_.toLong).sum }.asInstanceOf[Seq[_]=>Any])
          case so @ SortOp(id, fc, in, ord, partitioner) =>
            val shuffler = ShuffleOp(SilkUtil.newUUID, fc, in, partitioner)
            val shuffleReducer = ShuffleReduceOp(id, fc, shuffler, ord)
            startStage(shuffleReducer)
          case sp @ SamplingOp(id, fc, in, proportion) =>
            startStage(op, in, { data:Seq[_] =>
              // Sampling
              val indexedData = data.toIndexedSeq
              val N = data.size
              val m = (N * proportion).toInt
              val r = new Random
              val sample = (for(i <- 0 until m) yield indexedData(r.nextInt(N))).toIndexedSeq
              sample
            })
          case ShuffleReduceOp(id, fc, shuffleIn, ord) =>
            val inputStage = getStage(shuffleIn)
            val N = inputStage.numSlices
            val P = inputStage.numKeys
            info(s"shuffle reduce: N:$N, P:$P")
            val stageInfo = StageInfo(0, P, StageStarted(System.currentTimeMillis))
            for(p <- 0 until P) {
              localTaskManager.submitShuffleReduceTask(classBoxID, id, shuffleIn.id, p, N, ord.asInstanceOf[Ordering[_]])
            }
            stageInfo
          case so @ ShuffleOp(id, fc, in, partitioner) =>
            startShuffleStage(so)
          case other =>
            SilkException.error(s"unknown op:$other")
        }
      }
      catch {
        case e:Exception =>
          warn(s"aborted evaluation of [${op.idPrefix}]")
          error(e)
          val aborted = StageInfo(-1, -1, StageAborted(e.getMessage, System.currentTimeMillis()))
          sliceStorage.setStageInfo(op, aborted)
          aborted
      }
    }


    def getSlices[A](op: Silk[A]) : Seq[Future[Slice]] = {
      debug(s"getSlices: $op")

      val si = getStage(op)
      if(si.isFailed)
        SilkException.error(s"failed: ${si}")
      else
        for(i <- 0 until si.numSlices) yield sliceStorage.get(op.id, i)
    }

  }
}







//
//class Worker(val host: Host) extends Logger {
//
//  import Worker._
//
//  def resource = WorkerResource(host, 2, 1 * 1024 * 1024) // 2CPUs, 1G memory
//
//  private def flattenSlices(in: Seq[Seq[Slice[_]]]): Seq[Slice[_]] = {
//    var counter = 0
//    val result = for (ss <- in; s <- ss) yield {
//      val r = RawSlice(s.host, counter, s.data)
//      counter += 1
//      r
//    }
//    result
//  }
//
//  private def evalAtRemote[U](ss: SilkSession, op: Silk[_], slice: Slice[_])(f: (SilkSession, Slice[_]) => U): U = {
//    val b = Silk.serializeFunc(f)
//
//    debug(s"Eval slice (opID:${op.idPrefix}, host:${slice.host}) at ${host} function ${f.getClass.getSimpleName} size:${b.length}")
//    val fd = Silk.deserializeFunc(b).asInstanceOf[(SilkSession, Slice[_]) => U]
//    fd(ss, slice)
//  }
//
//
//  private def evalSlice(ss: SilkSession, in: Silk[_]): Seq[Slice[_]] = {
//    // Evaluate slice
//    // TODO: parallel evaluation
//    in.slice(ss)
//  }
//
//
//  def execute(ss: SilkSession, task: EvalTask) = {
//    task match {
//      case e @ EvalOpTask(b) => executeSilkOp(ss, e)
//      case e @ EvalSliceTask(slice, b) => executeSliceOp(ss, e)
//    }
//  }
//
//  def executeSliceOp(ss:SilkSession, task:EvalSliceTask) = {
//    val fd = Silk.deserializeFunc(task.opBinary).asInstanceOf[(SilkSession, Slice[_]) => Any]
//    fd(ss, task.slice)
//  }
//
//  def executeSilkOp(ss:SilkSession, task:EvalOpTask) = {
//
//    // Deserialize the operation
//    val op = Silk.deserializeOp(task.opBinary)
//    trace(s"execute: ${op}, byte size: ${DataUnit.toHumanReadableFormat(task.opBinary.length)}")
//
//
//    // TODO send the job to a remote machine
//    val result: Seq[Slice[_]] = op match {
//      case m@MapOp(fref, in, f, expr) =>
//        // in: S1, S2, ...., Sk
//        // output: S1.map(f), S2.map(f), ..., Sk.map(f)
//        // TODO: Open a stage
//        val r = for (slice <- evalSlice(ss, in)) yield {
//          // TODO: Choose an appropriate host
//          evalAtRemote(ss, op, slice) { execFlatMap(_, _, f) }
//        }
//        // TODO: Await until all of the sub stages terminates
//        flattenSlices(r)
//      case fm@FlatMapOp(fref, in, f, expr) =>
//        val r = for (slice <- evalSlice(ss, in)) yield
//          evalAtRemote(ss, op, slice) { execFlatMap(_, _, f) }
//        flattenSlices(r)
//      case fl@FilterOp(fref, in, f, expr) =>
//        val r = for (slice <- evalSlice(ss, in)) yield
//          evalAtRemote(ss, op, slice) { execFilter(_, _, f) }
//        flattenSlices(r)
//      case rs@RawSeq(fref, in) =>
//        // TODO distribute the data set
//        val r = rs.slice(ss)
//        debug(s"rawseq $r")
//        r
//      case jo@NaturalJoinOp(fref, left, right) =>
//        //val ls = evalSlice(left)
//        //val rs = evalSlice(right)
//        val keyParams = jo.keyParameterPairs
//        debug(s"key params: ${jo.keyParameterPairs.mkString(", ")}")
//        // TODO: Shuffle operation
//        if (keyParams.length == 1) {
//          val ka = keyParams.head._1
//          val kb = keyParams.head._2
//          val partitioner = {
//            k: Int => k % 2
//          } // Simple partitioner
//          val ls = ShuffleOp(left.fref, left, ka, partitioner)
//          val rs = ShuffleOp(right.fref, right, kb, partitioner)
//          val merger = MergeShuffleOp(fref, ls, rs)
//          ss.eval(merger)
//        }
//        else {
//          warn("multiple primary keys are not supported yet")
//          Seq.empty
//        }
//      case so@ShuffleOp(fref, in, keyParam, partitioner) =>
//        val shuffleSet = for (slice <- evalSlice(ss, in)) yield {
//          evalAtRemote(ss, so, slice) {
//            (sc, sl) =>
//              val shuffled = for (e <- sl.data) yield {
//                val key = keyParam.get(e)
//                val partition = fwrap(partitioner)(key).asInstanceOf[Int]
//                (key, partition) -> e
//              }
//              val slices = for (((key, partition), lst) <- shuffled.groupBy(_._1)) yield
//                PartitionedSlice(sl.host, partition, lst.map {
//                  x => (x._1._1, x._2)
//                })
//              slices.toSeq
//          }
//        }
//        // Merge partitions generated from a slice
//        val hostList = Worker.hosts
//        val partitions = for ((pid, slices) <- shuffleSet.flatten.groupBy(_.index)) yield {
//          val hi = slices.head.index % hostList.length
//          val h = hostList(hi)
//          PartitionedSlice(h, hi, slices.flatMap(_.data))
//        }
//        debug(s"partitions:${partitions}")
//        partitions.toSeq
//      case mo@MergeShuffleOp(fref, left, right) =>
//        val l = evalSlice(ss, left).sortBy(_.index)
//        val r = evalSlice(ss, right).sortBy(_.index)
//        val joined = for (le <- l; re <- r.filter(_.index == le.index)) yield {
//          // TODO hash-join
//          for ((lkey, ll) <- le.data; (rkey, rr) <- re.data if lkey == rkey) yield {
//            (ll, rr)
//          }
//        }
//        debug(s"joined $joined")
//        Seq(RawSlice(Host("localhost"), 0, joined.flatten))
//      case ReduceOp(fref, in, f, expr) =>
//        val r = for (slice <- evalSlice(ss, in)) yield {
//          // Reduce at each host
//          val red = evalAtRemote(ss, op, slice) {
//            (sc, slc) => slc.data.reduce(rwrap(f))
//          }
//          // TODO: Create new reducers
//          //evalRecursively(in).reduce{evalSingleRecursively(f.asInstanceOf[(Any,Any)=>Any](_, _))}
//        }
//        Seq(RawSlice(Host("localhost"), 0, Seq(r.reduce(rwrap(f)))))
//      case _ =>
//        warn(s"unknown op: ${op}")
//        Seq.empty
//    }
//
//    ss.putIfAbsent(op.uuid, result)
//  }
//
//  //  def scatter[A: ClassTag](rs: RawSeq[A]): DistributedSeq[A] = {
//  //    val numSlices = 2 // TODO retrieve default number of slices
//  //    val sliceSize = (rs.in.size + (numSlices - 1)) / numSlices
//  //    val slices = for ((slice, i) <- rs.in.sliding(sliceSize, sliceSize).zipWithIndex) yield {
//  //      val h = hostList(i % hostList.size) // round-robin split
//  //      // TODO: Send data to remote hosts
//  //      RawSlice(h, i, slice)
//  //    }
//  //    DistributedSeq[A](rs.fref, ss.newID, slices.toIndexedSeq)
//  //  }
//
//


