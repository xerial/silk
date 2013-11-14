package xerial.silk.core

import java.util.UUID

/**
 * Helper functions to manipulate UUIDs
 *
 * @author Taro L. Saito
 */
trait IDUtil {

  implicit class IDPrefix(id: UUID) {
    def prefix2 = id.toString.substring(0, 2)
    def prefix = id.toString.substring(0, 8)
    def path = s"$prefix2/$prefix"
  }

}
