package de.hpi.ddm.actors;

import akka.NotUsed;
import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import akka.stream.SourceRef;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.StreamRefs;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.ArrayUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletionStage;

public class LargeMessageProxy extends AbstractLoggingActor {

    ////////////////////////
    // Actor Construction //
    ////////////////////////

    public static final String DEFAULT_NAME = "largeMessageProxy";

    public static Props props() {
        return Props.create(LargeMessageProxy.class);
    }

    ////////////////////
    // Actor Messages //
    ////////////////////

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class LargeMessage<T> implements Serializable {
        private static final long serialVersionUID = 2940665245810221108L;
        private T message;
        private ActorRef receiver;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class BytesMessage<T> implements Serializable {
        private static final long serialVersionUID = 4057807743872319842L;
        private T bytes;
        private ActorRef sender;
        private ActorRef receiver;
    }

    /////////////////
    // Actor State //
    /////////////////

    /////////////////////
    // Actor Lifecycle //
    /////////////////////

    ////////////////////
    // Actor Behavior //
    ////////////////////

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(LargeMessage.class, this::handle)
                .match(SourceRefMessage.class, this::handle)
                .matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
                .build();
    }

    private void handle(LargeMessage<?> message) {
        ActorRef receiver = message.getReceiver();
        ActorSelection receiverProxy = this.context().actorSelection(receiver.path().child(DEFAULT_NAME));

        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            Kryo kryo = new Kryo();
            Output output = new Output(bos);
            kryo.writeClassAndObject(output, message.getMessage());
            output.close();
            byte[] messageByte = bos.toByteArray();

            Source<List<Byte>, NotUsed> source = Source.from(Arrays.asList(ArrayUtils.toObject(messageByte))).grouped(10000);
            SourceRef<List<Byte>> sourceRef = source.runWith(StreamRefs.sourceRef(), this.context().system());
            receiverProxy.tell(new SourceRefMessage(sourceRef, messageByte.length, this.sender(), message.getReceiver()), getSelf());

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void handle(SourceRefMessage sourceRefMessage) {
        System.out.println("We have a new message");
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
        LocalDateTime now = LocalDateTime.now();
        System.out.println(dtf.format(now));

        SourceRef<List<Byte>> sourceRef = sourceRefMessage.getSourceRef();
        CompletionStage<List<List<Byte>>> listCompletionStage = sourceRef
                .getSource()
                .limit(sourceRefMessage.getLength())
                .runWith(Sink.seq(), this.context().system());

        listCompletionStage.whenCompleteAsync((listOfBytes, exception) -> {
            byte[] arrayOfBytes = new byte[sourceRefMessage.getLength()];
            int index = 0;
            for (List<Byte> list : listOfBytes) {
                for (Byte singleByte : list) {
                    arrayOfBytes[index] = singleByte;
                    index++;
                }
            }

            try {
                ByteArrayInputStream bai = new ByteArrayInputStream(arrayOfBytes);
                Kryo kryo = new Kryo();
                Input input = new Input(bai);
                Object message1 = kryo.readClassAndObject(input);
                input.close();
                bai.close();
                LocalDateTime now1 = LocalDateTime.now();
                System.out.println(dtf.format(now1));
                sourceRefMessage.getReceiver().tell(message1, sourceRefMessage.getSender());
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }
}
