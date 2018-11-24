package org.infinivision.flink.rpc;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.UntypedActor;

public class Hello1 {
    public static void main(String[] args) {
        ActorSystem system = ActorSystem.create("actor-demo");
        ActorRef ref = system.actorOf(Props.create(Hello.class));
        ref.tell("Bob", ActorRef.noSender());
    }

    private static class Hello extends UntypedActor {
        @Override
        public void onReceive(Object message) {
            if (message instanceof String) {
                System.out.println(message);
            }
        }
    }
}
