/*
 * Copyright 2012 Taro L. Saito
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

package xerial.silk.util

//--------------------------------------
//
// CommandModule.scala
// Since: 2012/02/13 16:14
//
//--------------------------------------

/**
 * CUI program launcher
 *
 * @author leo
 */

trait Command extends Serializable {
  val commandName: String
  val oneLineDescription: String

  /**
   * Execute the command
   * @param m module and its options containing this command
   * @param args argument of this command
   */
  def execute(m: CommandModule, args: Array[String]): Unit = execute(args)

  def execute(args: Array[String]): Unit

  private[util] def optionParser = OptionParser(this)

  def printUsage: Unit = {
    optionParser.printUsage
  }
}

trait CommandLauncher {
  self =>
  val oneLineDescription: String

  protected val rootModule = new CommandModule {
    val commandName: String = "root"
    val oneLineDescription: String = self.oneLineDescription
  }

  def execute(argLine: String): Unit = rootModule.execute(argLine)

  def execute(args: Array[String]): Unit = rootModule.execute(args)

  def addCommands(command: Command*) = rootModule.addCommands(command: _*)

  def printUsage: Unit = rootModule.printUsage
}

object CommandModule {

  trait DefaultGlobalOption {
    @option(symbol = "h", longName = "help", description = "Display help message")
    var displayHelp = false

    @argument(description = "command name")
    var subCommandName: String = ""
  }

}

trait CommandModule extends Command with CommandModule.DefaultGlobalOption with Logging {
  type T = this.type

  private var commandList = Array[Command]()

  def execute(argLine: String): Unit = {
    execute(CommandLineTokenizer.tokenize(argLine))
  }

  def execute(args: Array[String]): Unit = {
    debug("execute in %s: %s", commandName, args.mkString(" "))
    val remainingArgs = optionParser.parse(args)

    def findSubCommand: Option[Command] = {
      if (subCommandName.isEmpty)
        None
      else
        commandList.find(_.commandName == subCommandName)
    }

    val subCommand = findSubCommand

    if (displayHelp) {
      subCommand match {
        case None => printUsage
        case Some(x) => x.printUsage
      }
      return
    }

    if (subCommand.isEmpty) {
      if (subCommandName.isEmpty)
        System.err.println("No sub command is given")
      else
        System.err.println("Unknown command: %s".format(subCommandName))
      return
    }

    {
      // Run the sub command. Creating a clone of the sub command is necessary to reset the command line options
      val s = subCommand.get.getClass.newInstance().asInstanceOf[Command]
      s.optionParser.parse(remainingArgs)
      s.execute(this, remainingArgs)
    }
  }


  override def printUsage: Unit = {
    optionParser.printUsage

    if (!commandList.isEmpty) {
      println("[sub commands]")
      println(subCommandSummary)
    }
  }

  def subCommandSummary: String = {
    val list = for (cmd <- commandList) yield (cmd.commandName, cmd.oneLineDescription)
    if (list.isEmpty)
      ""
    else {
      val w1 = list.map(_._1.length).max
      val f = " %%-%ds\t%%s".format(w1)
      list.map(l => f.format(l._1, l._2)).mkString("\n")
    }

  }

  def addCommands(command: Command*) : T = {
    commandList ++= command
    this
  }

}


