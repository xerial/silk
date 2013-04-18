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
import java.util.UUID
import xerial.core.log.Logger
import xerial.silk.cluster.SilkClient.{DataReference, OK, ReportStatus}

object SilkMaster {
  /**
   * Master messages
   */

  case class RegisterClassBox(cb:ClassBox, holder:ClientAddr)
  case class AskClassBoxHolder(id:String)
  case class ClassBoxHolder(cb:ClassBox, holder:ClientAddr)
  case class ClassBoxNotFound(id:String)

  case class RegisterArgumentsInfo(id: String, holder: DataAddr)
  case class AskArgumentsHolder(id: String)
  case class ArgumentsHolder(id: String, holder: DataAddr)
  case class ArgumentsNotFound(id: String)
}


/**
 * @author Taro L. Saito
 */
class SilkMaster extends Actor with Logger {

  import SilkMaster._

  private val classBoxLocation = scala.collection.mutable.Map[String, Set[ClientAddr]]()
  private val classBoxTable = scala.collection.mutable.Map[String, ClassBox]()
  private val argsLocation = collection.mutable.Map[String, Set[DataAddr]]()


  override def preStart() {
    info(s"Start SilkMaster at ${localhost.address}:${config.silkMasterPort}")
  }

  def receive = {
    case ReportStatus => {
      debug("Recieved a status ping")
      sender ! OK
    }
    case RegisterClassBox(cb, holder) =>
      info(s"Registering a ClassBox: ${cb.id}")
      classBoxTable.getOrElseUpdate(cb.id, cb)
      val prevHolders : Set[ClientAddr] = classBoxLocation.getOrElseUpdate(cb.id, Set())
      classBoxLocation += cb.id -> (prevHolders + holder)
      sender ! OK
    case AskClassBoxHolder(id) =>
      info(s"Query ClassBox ${id}")
      if(classBoxLocation.contains(id)) {
        val holder = classBoxLocation(id)
        // TODO return a closest or free holder address
        sender ! ClassBoxHolder(classBoxTable(id), holder.head)
      }
      else {
        sender ! ClassBoxNotFound(id)
      }
    case RegisterArgumentsInfo(id, holder) =>
    {
      info(s"Registering an arguments info: ${id}")
      val prevHolders: Set[DataAddr] = argsLocation.getOrElseUpdate(id, Set())
      argsLocation += id -> (prevHolders + holder)
      sender ! OK
    }
    case AskArgumentsHolder(id) =>
    {
      info(s"Query Arguments info ${id}")
      if (argsLocation.contains(id))
      {
        val holder = argsLocation(id)
        sender ! ArgumentsHolder(id, holder.head)
      }
      else
      {
        sender ! ArgumentsNotFound(id)
      }
    }
    case _ =>
    {
      warn("Unknown message")
    }
  }
  override def postStop() {
    info("Stopped SilkMaster")
  }
}