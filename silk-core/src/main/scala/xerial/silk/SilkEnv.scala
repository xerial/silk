package xerial.silk


/**
 * Defines a cluster environment to execute Silk operations
 * @author Taro L. Saito
 */
trait SilkEnv extends Serializable {

  def run[A](op:SilkSeq[A]) : Seq[A] = eval(op).get
  def run[A](op:SilkSingle[A]) : A = eval(op).get
  def eval[A](op:SilkSeq[A]) : SilkFuture[Seq[A]]
  def eval[A](op:SilkSingle[A]) : SilkFuture[A]

  //def run[A](op:SilkSeq[A], target:String) : Seq[_] = eval(op, target).get
  //def evalSeq[A](op:Silk[_], target:String) : SilkFuture[Seq[A]]
  //def evalSingle[A](op:Silk[_], target:String) : SilkFuture[A]



  private[silk] def runF0[R](locality:Seq[String], f: => R) : R


}

