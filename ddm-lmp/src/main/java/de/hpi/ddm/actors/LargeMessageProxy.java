package de.hpi.ddm.actors;

import java.io.*;
import java.lang.reflect.Executable;

import akka.NotUsed;
import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.OverflowStrategy;
import akka.stream.SourceRef;
import akka.stream.javadsl.*;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.SerializationUtils;
import sun.jvm.hotspot.runtime.Bytes;

public class LargeMessageProxy extends AbstractLoggingActor {

    ////////////////////////
    // Actor Construction //
    ////////////////////////

    public static final String DEFAULT_NAME = "largeMessageProxy";

    public static Props props() {
        return Props.create(LargeMessageProxy.class);
    }

    private final Materializer materializer = ActorMaterializer.create(this.context());

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
                .match(BytesMessage.class, this::handle)
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
            byte [] messageByte = bos.toByteArray();

			bos.close();

            Source<byte[], ActorRef> source = Source.actorRef(messageByte.length, OverflowStrategy.dropHead());

            ActorRef actorRef = source
                    .to(Sink.foreach(o -> {
                    	ByteArrayInputStream bai = new ByteArrayInputStream(o);
                        Kryo kryo1 = new Kryo();
                        Input input = new Input(bai);
                        Object message1 = kryo1.readClassAndObject(input);
                        System.out.println("We have: " + message1);
                        input.close();
                        bai.close();
                    }))
                    .run(materializer);

            actorRef.tell(messageByte, this.self());
        } catch (Exception e) {
            e.printStackTrace();
        }


        // This will definitely fail in a distributed setting if the serialized message is large!
        // Solution options:
        // 1. Serialize the object and send its bytes batch-wise (make sure to use artery's side channel then).
        // 2. Serialize the object and send its bytes via Akka streaming.
        // 3. Send the object via Akka's http client-server component.
        // 4. Other ideas ...

    }

    private void handle(BytesMessage<?> message) {
        // Reassemble the message content, deserialize it and/or load the content from some local location before forwarding its content.
        message.getReceiver().tell(message.getBytes(), message.getSender());
    }
}
