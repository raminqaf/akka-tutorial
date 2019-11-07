package de.hpi.ddm.actors;

import java.io.Serializable;
import java.time.Duration;
import java.util.concurrent.CompletionStage;

import akka.Done;
import akka.NotUsed;
import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.OverflowStrategies;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

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
	
	@Data @NoArgsConstructor @AllArgsConstructor
	public static class LargeMessage<T> implements Serializable {
		private static final long serialVersionUID = 2940665245810221108L;
		private T message;
		private ActorRef receiver;
	}

	@Data @NoArgsConstructor @AllArgsConstructor
	public static class BytesMessage<T> implements Serializable {
		private static final long serialVersionUID = 4057807743872319842L;
		private T bytes;
		private ActorRef sender;
		private ActorRef receiver;
	}
	
	/////////////////
	// Actor State //
	/////////////////

	private String test1 = "";

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
				.match(Source.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(LargeMessage<?> message) {
		ActorRef receiver = message.getReceiver();
		ActorSelection receiverProxy = this.context().actorSelection(receiver.path().child(DEFAULT_NAME));


		// TODO: message.getMessage serialisieren ohne auf das Serializable interface zu setzen
		// TODO: Stream Verbindung zum receiving LMProxy aufbauen und Stream mit serialisiertem Inhalt senden
		Source<Integer, NotUsed> source = Source.range(1, 10);
		receiverProxy.tell(source, this.self());
		//receiverProxy.tell(new BytesMessage<>(message.getMessage(), this.sender(), message.getReceiver()), this.self());
	}

	private void handle(BytesMessage<?> message) {
		// TODO: Wenn der stream aufgehört hat, dann Object deserialisierne und an Empfänger senden
		message.getReceiver().tell(message.getBytes(), message.getSender());
	}

	private void handle(Source message) {
		Materializer materializer = ActorMaterializer.create(this.context());
		CompletionStage<Done> done = message.runForeach(i -> this.test1 += i, materializer);
		done.thenRun(() -> System.out.println(this.test1));
		System.out.println(this.self().path());
	}
}
