//--------------------------------------
//
// SilkSession.scala
// Since: 2013/06/16 9:27
//
//--------------------------------------

package xerial.silk.framework

/**
 * @author Taro L. Saito
 */
class SilkSession(val sessionID: UUID = UUID.randomUUID) extends Logger with Serializable {

  def sessionIDPrefix = sessionID.toString.substring(0, 8)

  //info(s"A new SilkSession: $sessionID")

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
    scheduler.submit(this, EvalOpTask(ba))
  }
}