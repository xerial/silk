//--------------------------------------
//
// SilkLocalFramework.scala
// Since: 2013/11/19 13:07
// 
//--------------------------------------

package xerial.silk.framework.local

import java.io.File
import xerial.silk.util.Path
import Path._
import xerial.silk.framework.BaseConfig

/**
 * 
 * 
 * @author Taro L. Saito
 */
trait SilkLocalFramework {

  type Config = BaseConfig
  object config extends BaseConfig
}


