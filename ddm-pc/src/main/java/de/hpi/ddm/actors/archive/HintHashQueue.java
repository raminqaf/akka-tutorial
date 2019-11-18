package de.hpi.ddm.actors.archive;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.dsl.Creators;
import de.hpi.ddm.actors.Worker;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;
import java.util.Queue;

public class HintHashQueue extends Worker {

    ////////////////////
    // Actor Messages //
    ////////////////////

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class HintHashBatchMessage implements Serializable {
        private static final long serialVersionUID = 2940665245810221108L;
        private List<String> batch;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class HintHashConsumerMessage implements Serializable {
        private static final long serialVersionUID = 2940665245810221108L;
        private ActorRef consumer;
    }

    ////////////////////////
    // Actor Construction //
    ////////////////////////

    public static final String DEFAULT_NAME = "hashProducer";

    /////////////////
    // Actor State //
    /////////////////

    private Queue<List<String>> batchQueue;
    private ActorRef receiver;

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(HintHashBatchMessage.class, this::handle)
                .match(HintHashConsumerMessage.class, this::handle)
                .build();
    }

    private void handle(HintHashBatchMessage producerMessage) {
        this.batchQueue.add(producerMessage.getBatch());
        // TODO: Da kann man das queue handling auch noch sinnvoller gestalten
        this.receiver.tell(new HintHashBatchMessage(this.batchQueue.remove()), this.getSelf());
    }

    private void handle(HintHashConsumerMessage consumerMessage) {
        if(this.batchQueue.isEmpty()) {
            ActorSelection receiverProxy = this.context().actorSelection("receiver.path().child(DEFAULT_NAME)"); //TODO match all producers
        } else {
            consumerMessage.getConsumer().tell(new HintHashBatchMessage(this.batchQueue.remove()), this.getSelf());
        }
    }

}
