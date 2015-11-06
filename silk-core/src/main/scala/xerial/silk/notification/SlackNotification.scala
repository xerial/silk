/*
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
package xerial.silk.notification

import com.flyberrycapital.slack.SlackClient

/**
 *
 */
class SlackNotification(token:String, globalOptions:Map[String, String]=Map.empty) {

  def send(channel:String, message:String, options:Map[String, String]=Map.empty): Unit = {

    val client = new SlackClient(token)
    client.chat.postMessage(channel, message, options)

  }

}
