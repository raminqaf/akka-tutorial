package de.hpi.ddm.actors;

import java.io.*;
import java.lang.reflect.Executable;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletionStage;

import akka.NotUsed;
import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import akka.stream.*;
import akka.stream.impl.streamref.StreamRefResolverImpl;
import akka.stream.javadsl.*;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.SerializationUtils;
import static java.lang.Math.max;

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
            byte [] messageByte = bos.toByteArray();

            Source<List<Byte>, NotUsed> source = Source.from(Arrays.asList(ArrayUtils.toObject(messageByte))).grouped(1024);
                    //.grouped(messageByte.length/1024);

                    //.map(x-> receiverProxy.tell(new SourceRefMessage(x, this.sender(), message.getReceiver()), getSelf()));

//            source.runForeach(receiverProxy.tell(new SourceRefMessage(, this.sender(), message.getReceiver()), getSelf()));

            SourceRef<List<Byte>> sourceRef = source.runWith(StreamRefs.sourceRef(), this.context().system());
            receiverProxy.tell(new SourceRefMessage(sourceRef, messageByte.length, this.sender(), message.getReceiver()), getSelf());

//            ActorRef actorRef = source
//                    .to(Sink.foreach(o -> {
//                    	ByteArrayInputStream bai = new ByteArrayInputStream(o);
//                        Kryo kryo1 = new Kryo();
//                        Input input = new Input(bai);
//                        Object message1 = kryo1.readClassAndObject(input);
//                        System.out.println("We have: " + message1);
//                        input.close();
//                        bai.close();
//                    }))
//                    .run(materializer);
//            actorRef.tell(messageByte, this.self());

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

    private void handle(SourceRefMessage message) {
        // Reassemble the message content, deserialize it and/or load the content from some local location before forwarding its content.
        System.out.println("We have a new message");
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
        LocalDateTime now = LocalDateTime.now();
        System.out.println(dtf.format(now));

        SourceRef<List<Byte>> sourceRef = message.getSourceRef();
        CompletionStage<List<List<Byte>>> listCompletionStage = sourceRef.getSource().limit(message.getLength()).runWith(Sink.seq(), this.context().system());
        listCompletionStage.whenCompleteAsync((listOfBytes, exception) -> {
            System.out.println("We have a new message");
            byte[] arrayOfbytes = new byte[message.getLength()];
            int index=0;
            for(List<Byte> list : listOfBytes) {
                for(Byte singleByte : list) {
                    arrayOfbytes[index] = singleByte;
                    index++;
                }
            }
            try {
            ByteArrayInputStream bai = new ByteArrayInputStream(arrayOfbytes);
                        Kryo kryo1 = new Kryo();
                        Input input = new Input(bai);
                        Object message1 = kryo1.readClassAndObject(input);
                input.close();
                bai.close();
                        System.out.println("We have: " + message1);
            LocalDateTime now1 = LocalDateTime.now();
            System.out.println(dtf.format(now1));
            message.getReceiver().tell(message1, message.getSender());
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
//

    }
}
