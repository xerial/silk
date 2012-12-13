/*
 * Copyright 2012 Taro L. Saito
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//--------------------------------------
//
// MachineResource.scala
// Since: 2012/10/25 10:00 AM
//
//--------------------------------------

package xerial.silk.cluster

import management.ManagementFactory
import com.sun.management.OperatingSystemMXBean
import sys.process.Process
import xerial.core.util.Shell
import xerial.core.log.Logger
import collection.JavaConversions._
import java.net.{UnknownHostException, Inet4Address, InetAddress, NetworkInterface}

/**
 * Machine resource information
 *
 * @author leo
 */
case class MachineResource(host: Host, numCPUs: Int, memory: Long, networkInterfaces: Seq[NetworkIF]) {
  override def toString = "host:%s, CPU:%d, memory:%s, networkInterface:%s".format(host, numCPUs, MachineResource.toHumanReadableFormat(memory), networkInterfaces.mkString(", "))
}

case class Host(name: String, address: String)

case class NetworkIF(name: String, address: InetAddress)


object MachineResource extends Logger {



  def localhost: Host = {
    val lh = InetAddress.getLocalHost
    debug("host:%s, %s", lh.getHostName, lh.getHostAddress)
    Host(lh.getHostName, lh.getHostAddress)
  }

  def toHumanReadableFormat(byteSize: Long): String = {
    // kilo, mega, giga, tera, peta, exa, zetta, yotta
    val unitName = Seq("", "K", "M", "G", "T", "P", "E", "Z", "Y")

    def loop(index: Int, unit: Long): (Long, String) = {
      if (index >= unitName.length)
        (byteSize, "")
      val nextUnit = unit * 1024
      if (byteSize < nextUnit)
        (byteSize / unit, unitName(index))
      else
        loop(index + 1, nextUnit)
    }

    val f = loop(0, 1)
    "%d%s".format(f._1, f._2)
  }

  /**
   * Retrieve [[xerial.silk.cluster.MachineResource]] information of this machine
   * @return
   */
  def thisMachine: MachineResource = {
    val osInfo = ManagementFactory.getOperatingSystemMXBean
    // number of CPUs in this machine
    val numCPUs = osInfo.getAvailableProcessors

    // Get the system memory size
    val memory = osInfo match {
      case o: OperatingSystemMXBean => o.getTotalPhysicalMemorySize
      case _ => {
        // Use system command to test memory size
        val pb = Shell.prepareProcessBuilder( """free -b | head -2 | tail -1 | awk '{ print $2; }'""", inheritIO = false)
        val r = Process(pb).!!
        r.trim.toLong
      }
    }

    def isValidNetworkInterface(nif: NetworkInterface): Boolean = {
      // Retrieve ethernet and infiniband network interfaces
      val prefix = Seq("eth", "ib", "net")
      val name = nif.getName
      (!nif.isLoopback) && prefix.exists(name.startsWith(_))
    }

    def getInet4Address(nif: NetworkInterface): Option[InetAddress] =
      nif.getInetAddresses.collectFirst {
        case i: Inet4Address => i
      }

    // Network interfaces
    val interfaces =
      for {
        nif <- NetworkInterface.getNetworkInterfaces if isValidNetworkInterface(nif)
        address <- getInet4Address(nif)
      } yield {
        trace("network %s:%s MTU:%d ", nif.getName, nif.getInetAddresses.map(_.getHostAddress).mkString(","), nif.getMTU)
        NetworkIF(nif.getName, address)
      }

    MachineResource(localhost, numCPUs, memory, interfaces.toSeq)
  }

}