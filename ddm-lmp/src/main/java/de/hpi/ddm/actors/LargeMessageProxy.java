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
import de.hpi.ddm.structures.SourceRefMessage;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.ArrayUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class LargeMessageProxy extends AbstractLoggingActor {

    ////////////////////////
    // Actor Construction //
    ////////////////////////

    public static final String DEFAULT_NAME = "largeMessageProxy";
    // Defines the message size. Depending on the expected message load on the system this needs to be adjusted.
    public static final int PACKAGE_SIZE = 10000;
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
            // 1. Serialize message content with Kryo into a byte array
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            Kryo kryo = new Kryo();
            Output output = new Output(bos);
            kryo.writeClassAndObject(output, message.getMessage());
            output.close();
            byte[] messageByte = bos.toByteArray();

            // 2. Create a stream on the byte array by converting it to a Byte array
            // 2.a Group the stream content for better transfer performance
            Source<List<Byte>, NotUsed> source = Source.from(Arrays.asList(ArrayUtils.toObject(messageByte))).grouped(PACKAGE_SIZE);

            // 3. Create a reference to the source of the stream and send it to the receiving LMProxy
            SourceRef<List<Byte>> sourceRef = source.runWith(StreamRefs.sourceRef(), this.context().system());
            // and attach the original message meta information
            receiverProxy.tell(new SourceRefMessage(sourceRef, messageByte.length, this.sender(), message.getReceiver()), getSelf());

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void handle(SourceRefMessage sourceRefMessage) {
        sourceRefMessage.getSourceRef()
            // 4. Create a local source object from the reference
            .getSource()
            .limit(sourceRefMessage.getLength())
            // 5. Then read the stream by running the content sequentially into a sink
            .runWith(Sink.seq(), this.context().system())
            // and subscribing to the completion event
            .whenComplete((listOfBytes, exception) -> {
                // 6. Ungroup the received Bytes, convert them to bytes, and save them in an array
                byte[] arrayOfBytes = new byte[sourceRefMessage.getLength()];
                int index = 0;
                for (List<Byte> list : listOfBytes) {
                    for (Byte singleByte : list) {
                        arrayOfBytes[index] = singleByte;
                        index++;
                    }
                }

                try {
                    // 7. Deserialize the byte array with Kryo by reading from a byte stream
                    ByteArrayInputStream bai = new ByteArrayInputStream(arrayOfBytes);
                    Kryo kryo = new Kryo();
                    Input input = new Input(bai);
                    Object messageObject = kryo.readClassAndObject(input);
                    input.close();
                    bai.close();

                    // 8. If the serialization was successful, and no exception was thrown, forward the original object to the designated receiver
                    sourceRefMessage.getReceiver().tell(messageObject, sourceRefMessage.getSender());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
    }
}
