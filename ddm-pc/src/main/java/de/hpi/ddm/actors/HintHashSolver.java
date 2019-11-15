package de.hpi.ddm.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import javafx.util.Pair;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Queue;

public class HintHashSolver extends Worker {

    ////////////////////
    // Actor Messages //
    ////////////////////

    /////////////////
    // Actor State //
    /////////////////

    private Map<String, Integer> hints;
    private ActorRef batchQueue;

    @Override
    public AbstractActor.Receive createReceive() {
        return receiveBuilder()
                .match(HintHashQueue.HintHashBatchMessage.class, this::handle)
                .build();
    }

    private void handle(HintHashQueue.HintHashBatchMessage producerMessage) {
        for(String hash : producerMessage.getBatch()) {
            if(this.hints.containsKey(hash)) {
                // TODO: Tell the solved hint to the master
            }
        }
        this.batchQueue.tell(new HintHashQueue.HintHashConsumerMessage(this.getSelf()), this.getSelf());
    }
}
