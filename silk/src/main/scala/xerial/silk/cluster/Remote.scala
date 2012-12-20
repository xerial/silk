//--------------------------------------
//
// Remote.scala
// Since: 2012/12/20 2:22 PM
//
//--------------------------------------

package xerial.silk.cluster

/**
 * Remote command launcher
 * @author Taro L. Saito
 */
object Remote {

  /**
   * Run the given function at the specified host
   * @param host
   * @param f
   * @tparam U
   * @return
   */
  def at[U](host:Host)(f: => U) : U = {

    // TODO copy closure environment
    val closureCls = f.getClass
    val classBox = ClassBox.current


    val client = SilkClient.getClientAt(host.address)





    f
  }

  private[cluster] def run(cb:ClassBox, className:String) {


  }

}