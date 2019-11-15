package de.hpi.ddm.actors;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent;
import akka.cluster.Member;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class HintHashProducer extends Worker {

    /////////////////
    // Actor State //
    /////////////////
    //TODO make this dynamic and distribute the ranges between workers
    private int i = 0;
    private int[] c = new int[10];
    private List<String> letters = Arrays.asList("A","B","C","D","E","F","G","H","I","J");

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(String.class, this::handle)
                .build();
    }

    private void handle(String command) {
        if(command.equals("produce")) {
            List<String> result = new LinkedList<>();
            int size_a = this.letters.size();

            //int[] c = new int[this.letters.size()];
            Arrays.fill(c, 0);
            result.add(hash(String.join("", this.letters)));

            //int i = 0;
            int counter = 0;
            while(i < this.letters.size() && counter <= 100) {
                if(c[i] < i) {
                    if((c[i] & 1) == 0) {
                        Collections.swap(this.letters, 0, i);
                    } else {
                        Collections.swap(this.letters, c[i], i);
                    }
                    result.add(hash(String.join("", this.letters)));
                    c[i] += 1;
                    i = 0;
                    counter += 1;
                } else {
                    c[i] = 0;
                    i += 1;
                }
            }
            // TODO: Tell the result to all queues/consumers
            //return result;
        }
    }
}
