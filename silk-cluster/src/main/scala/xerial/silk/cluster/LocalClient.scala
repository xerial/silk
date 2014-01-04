package xerial.silk.cluster

import xerial.silk.cluster.store.DataServer
import xerial.silk.framework.{ExecutorAPI, SliceStorageAPI}

/**
 * @author Taro L. Saito
 */
trait LocalClient {

  def currentNodeName : String
  def address : String
  def executor : ExecutorAPI
  def localTaskManager: LocalTaskManagerAPI
  def sliceStorage : SliceStorageAPI
  def dataServer : DataServer
}


/**
 * Used to refer to SilkClient within components
 */
trait LocalClientComponent extends LocalInfoComponent {

  def localClient : LocalClient

}


trait LocalInfoComponent {

  def currentNodeName : String

}


trait DefaultLocalInfoComponent extends LocalInfoComponent {
  def currentNodeName = "localhost"

}
