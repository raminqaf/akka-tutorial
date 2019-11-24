package de.hpi.ddm.actors.archive;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import de.hpi.ddm.actors.Worker;

import java.util.Map;

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
                //  Tell the solved hint to the master
            }
        }
        this.batchQueue.tell(new HintHashQueue.HintHashConsumerMessage(this.getSelf()), this.getSelf());
    }
}
