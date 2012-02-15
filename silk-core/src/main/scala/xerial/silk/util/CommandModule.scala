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

trait Command {
  val commandName: String
  val oneLineDescription: String

  protected val opt = new OptionParser(this)
  /**
   * Execute the command
   * @param m module and its options containing this command
   * @param args argument of this command
   */
  def execute(m: CommandModule, args: Array[String]): Unit = execute(args)

  def execute(args: Array[String]): Unit


  def printUsage: Unit = {
    opt.displayHelpMessage
  }
}

trait CommandLauncher extends CommandModule {
  val commandName = "root"
}

object CommandModule {
  trait DefaultGlobalOption {
    @option(symbol = "h", longName = "help", description = "Display help message")
    var displayHelp = false

    @argument(index = 0, description = "command name")
    var subCommandName: String = ""
  }
}

trait CommandModule extends Command with CommandModule.DefaultGlobalOption with Logging {

  private var commandList = Array[Command]()

  def execute(argLine: String): Unit = {
    execute(CommandLineTokenizer.tokenize(argLine))
  }

  def execute(args: Array[String]): Unit = {
    debug("execute in %s: %s", commandName, args.mkString(" "))
    val remainingArgs = opt.parse(args, exitAfterFirstArgument = true)

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
      // run sub command
      val sopt = OptionParser(subCommand.get)
      sopt.parse(remainingArgs)
      subCommand.get.execute(this, remainingArgs)
    }
  }


  override def printUsage: Unit = {
    opt.displayHelpMessage

    println("[sub commands]")
    println(subCommandSummary)
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

  def addCommand(command: Command*) = {
    commandList ++= command
  }

}


