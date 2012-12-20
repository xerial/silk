//--------------------------------------
//
// Remote.scala
// Since: 2012/12/20 2:22 PM
//
//--------------------------------------

package xerial.silk.cluster

import xerial.silk.cluster.SilkClient.Run
import xerial.core.log.Logger

/**
 * Remote command launcher
 * @author Taro L. Saito
 */
object Remote extends Logger {

  /**
   * Run the given function at the specified host
   * @param host
   * @param f
   * @tparam U
   * @return
   */
  def at[U](host:Host)(f: () => U) : U = {
    // TODO copy closure environment
    val closureCls = f.getClass
    val classBox = ClassBox.current
    SilkClient.dataServer.map { classBox.register(_) }

    // Get remote client
    info("getting remote client at %s", host.address)
    val client = SilkClient.getClientAt(host.address)
    // TODO Support functions with arguments
    // Send a remote command request
    client ! Run(classBox, closureCls.getName)



    // TODO retrieve result
    null.asInstanceOf[U]
  }

  private[cluster] def run(cb:ClassBox, className:String) {
    info("Running command at %s", localhost)

    val myClassBox = cb.resolve

    import ClassBox._
    val cl = myClassBox.classLoader
    withClassLoader(cl) {
      val mainClass = cl.loadClass(className)
      val m = mainClass.getMethod("apply")
      m.invoke(null)
    }
  }

}