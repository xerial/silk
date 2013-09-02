/*
 * Copyright 2013 Taro L. Saito
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//--------------------------------------
//
// SilkMaster.scala
// Since: 2013/01/08 10:15 AM
//
//--------------------------------------

package xerial.silk.cluster

import akka.actor.Actor
import xerial.silk.cluster.framework.SilkMasterService
import SilkClient.{OK, ReportStatus}
import xerial.silk.framework._
import xerial.silk.framework.TaskStatusUpdate

object SilkMaster {
  /**
   * Master messages
   */
  case class RegisterDataInfo(id: String, holder: DataAddr)
  case class AskDataHolder(id: String)
  case class DataHolder(id: String, holder: DataAddr)
  case class DataNotFound(id: String)
}


/**
 * @author Taro L. Saito
 */
class SilkMaster(val name:String, val address:String, val zk:ZooKeeperClient) extends Actor
  with SilkMasterService with IDUtil {

  import SilkMaster._
  private val argsLocation = collection.mutable.Map[String, Set[DataAddr]]()


  override def preStart() {
    info(s"Start SilkMaster at ${address}:${config.silkMasterPort}")
    startup
  }

  def receive = {
    case ReportStatus => {
      info(s"Received a status ping from ${sender}")
      sender ! OK
    }
    case s : TaskRequest =>
      taskManager.receive(s)
    case u @ TaskStatusUpdate(taskID, newStatus) =>
      taskManager.receive(u)
    case RegisterDataInfo(id, holder) =>
    {
      info(s"Registering an arguments info: ${id}")
      val prevHolders: Set[DataAddr] = argsLocation.getOrElseUpdate(id, Set())
      argsLocation += id -> (prevHolders + holder)
      sender ! OK
    }
    case AskDataHolder(id) =>
    {
      info(s"Query Arguments info ${id}")
      if (argsLocation.contains(id))
      {
        val holder = argsLocation(id)
        sender ! DataHolder(id, holder.head)
      }
      else
      {
        sender ! DataNotFound(id)
      }
    }
    case other =>
    {
      warn(s"Unknown message: $other")
    }
  }
  override def postStop() {
    info("Stopped SilkMaster")
    teardown
  }
}