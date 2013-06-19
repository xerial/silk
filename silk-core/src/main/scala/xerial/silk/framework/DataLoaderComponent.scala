//--------------------------------------
//
// DataLoaderComponent.scala
// Since: 2013/06/16 15:51
//
//--------------------------------------

package xerial.silk.framework

import java.io.File
import scala.language.experimental.macros
import scala.reflect.ClassTag
import scala.reflect.macros.Context
import scala.reflect.runtime.{universe=>ru}
import xerial.silk.framework.ops.{LoadFile, SilkOps}


/**
 * A component for loading input data
 * @author Taro L. Saito
 */
trait DataLoaderComponent {
  self: SilkFramework =>
  type Loader <: LoaderAPI

  val loader : Loader

  trait LoaderAPI {
    //def loadFile[A:ClassTag](file:String) : Silk[A] = load(new File(file))

    /**
     * Load a file data as a sequence of object A
     * @param file
     * @tparam A
     * @return
     */
//    def load[A](file:File)(implicit ev:ClassTag[A]) : LoadFile[A] = macro SilkOps.loadImpl[A]

  }
}