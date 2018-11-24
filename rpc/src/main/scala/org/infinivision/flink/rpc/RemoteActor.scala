package org.infinivision.flink.rpc

import java.io.File

import akka.actor.{Actor, ActorSystem, Props}
import com.typesafe.config.ConfigFactory

class RemoteActor extends Actor {
  override def receive: Receive = {
    case msg: String => {
      println("remote received: " + msg + " from " + sender())
      sender ! "hi"
    }
    case _ => println("unknown message!")
  }

}

object RemoteActor{
  def main(args: Array[String]) {
    //get the configuration file from classpath
    val configFile = getClass.getClassLoader.getResource("remote_application.conf").getFile
    //parse the config
    val config = ConfigFactory.parseFile(new File(configFile))
    //create an actor system with that config
    val system = ActorSystem("RemoteSystem" , config)
    //create a remote actor from actorSystem
    val remote = system.actorOf(Props[RemoteActor], name="remote")
    println("remote is ready")


  }
}