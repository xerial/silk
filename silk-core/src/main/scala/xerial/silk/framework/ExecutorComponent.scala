package xerial.silk.framework

import xerial.silk.SilkException
import xerial.silk.framework.ops._
import xerial.silk.framework.ops.ReduceOp
import xerial.silk.framework.ops.FilterOp
import xerial.silk.framework.ops.FlatMapOp
import xerial.silk.framework.ops.MapOp
import xerial.core.log.{LoggerFactory, Logger}
import xerial.silk.core.ClosureSerializer
import java.util.UUID


trait DefaultExecutor extends ExecutorComponent {
  self : SilkFramework
    with SliceComponent
    with LocalTaskManagerComponent
    with LocalClientComponent
    //with StageManagerComponent
    with SliceStorageComponent =>

  type Executor = ExecutorImpl
  def executor : Executor = new ExecutorImpl

  class ExecutorImpl extends ExecutorAPI {}
}

/**
 * Executor of Silk programs
 */
trait ExecutorComponent {
  self : SilkFramework
    with SliceComponent
    with LocalTaskManagerComponent
    with LocalClientComponent
    //with StageManagerComponent
    with SliceStorageComponent =>

  type Executor <: ExecutorAPI
  def executor : Executor



  trait ExecutorAPI extends Logger {

    implicit class toGenFun[A,B](f:A=>B) {
      def toF1 : Any=>Any = f.asInstanceOf[Any=>Any]
      def toFlatMap : Any=>SilkSeq[Any] = f.asInstanceOf[Any=>SilkSeq[Any]]
      def toFilter : Any=>Boolean = f.asInstanceOf[Any=>Boolean]
    }


    def defaultParallelism : Int = 2

//    def newSlice[A](op:Silk[_], index:Int, data:Seq[A]) : Slice[A] = {
//      val slice = Slice(localClient.currentNodeName, index)
//      sliceStorage.put(op.id, index, slice, data)
//      slice
//    }

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

//    def evalRecursively[A](op:Silk[A], v:Any) : Seq[Slice[A]] = {
//      v match {
//        case silk:Silk[_] => getSlices(silk).asInstanceOf[Seq[Slice[A]]]
//        case seq:Seq[_] => Seq(newSlice(op, 0, seq.asInstanceOf[Seq[A]]))
//        case e => Seq(newSlice(op, 0, Seq(e).asInstanceOf[Seq[A]]))
//      }
//    }
//
//    private def flattenSlices[A](op:Silk[_], in: Seq[Seq[Slice[A]]]): Seq[Slice[A]] = {
//      var counter = 0
//      val result = for (ss <- in; s <- ss) yield {
//        val r = newSlice(op, counter, sliceStorage.retrieve(op, s).asInstanceOf[Seq[A]])
//        counter += 1
//        r
//      }
//      result
//    }


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
      val stageInfo = StageInfo(N, StageStarted(System.currentTimeMillis()))
      sliceStorage.setStageInfo(op, stageInfo)
      // TODO append par
      for(i <- (0 until N)) {
        // Get an input slice
        val inputSlice = sliceStorage.get(in.id, i).get
        // Send the slice processing task to a node close to the inputSlice location
        localTaskManager.submitEvalTask(Seq(inputSlice.nodeName))(op.id, in.id, inputSlice, f.toF1)
      }
      stageInfo
    }


    private def startReduceStage[A, Out](op:Silk[A], in:Silk[A], f:(Any,Any)=>Any) = {
      val inputStage = getStage(in)
      val N = inputStage.numSlices
      // Determine the number of reducers to use. The default is 1/3 of the number of the input slices
      val R = ((N + (3-1))/ 3.0).toInt
      val W = (N + (R-1)) / R
      info(s"num reducers:$R, W:$W")

      // Reduce task produces only 1 slice
      val stageInfo = StageInfo(1, StageStarted(System.currentTimeMillis()))

      // Evaluate each slice in a new sub stage
      val subStageID = Silk.newUUID
      for((sliceRange, i) <- (0 until N).sliding(W, W).zipWithIndex) {
        val sliceIndexSet = sliceRange.toIndexedSeq
        localTaskManager.submitReduceTask(subStageID, in.id, sliceIndexSet, i, f)
      }
      // The final aggregate task
      localTaskManager.submitReduceTask(op.id, subStageID, (0 until R).toIndexedSeq, 0, f)

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
            startReduceStage(op, in, fc)
          case fo @ FlatMapOp(id, fc, in, f, fe) =>
            val fc = f.toF1
            startStage(fo, in, { _.map(fc) })
          case other =>
            warn(s"unknown op: $other")
            StageInfo(-1, StageAborted(s"unknown op:$other", System.currentTimeMillis))
        }
      }
      catch {
        case e:Exception =>
          val aborted = StageInfo(-1, StageAborted(e.getMessage, System.currentTimeMillis()))
          sliceStorage.setStageInfo(op, aborted)
          aborted
      }
    }


    def getSlices[A](op: Silk[A]) : Seq[Future[Slice[A]]] = {
      debug(s"getSlices: $op")

      val si = getStage(op)
      if(si.isFailed)
        SilkException.error(s"failed: ${si}")
      else
        for(i <- 0 until si.numSlices) yield sliceStorage.get(op.id, i).asInstanceOf[Future[Slice[A]]]
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


