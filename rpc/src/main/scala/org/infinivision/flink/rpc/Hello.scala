package org.infinivision.flink.rpc

import akka.actor.{Actor, ActorSystem, Props}
import akka.event.Logging


class HelloActor extends Actor{
  val log = Logging(context.system, this)

  override def receive: Receive = {
    case "test" => log.info("received test")
    case _ => log.info("received other message")
  }
}

object Hello extends App {
  val system = ActorSystem("mySystem")
  val myActor = system.actorOf(Props[HelloActor], "myactor")
  myActor ! "Hello1"
  myActor ! "test"
}
