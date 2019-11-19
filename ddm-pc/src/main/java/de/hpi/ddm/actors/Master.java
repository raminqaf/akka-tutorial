package de.hpi.ddm.actors;

import akka.actor.*;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.*;

public class Master extends AbstractLoggingActor {

    ////////////////////////
    // Actor Construction //
    ////////////////////////

    public static final String DEFAULT_NAME = "master";

    public static Props props(final ActorRef reader, final ActorRef collector) {
        return Props.create(Master.class, () -> new Master(reader, collector));
    }

    public Master(final ActorRef reader, final ActorRef collector) {
        this.reader = reader;
        this.collector = collector;
        this.workers = new ArrayList<>();

        this.passwordHashList = new ArrayList<>();
        this.hintList = HashMultimap.create();
        this.workersQueue = new LinkedList<>();
        this.messageQueue = new LinkedList<>();
        this.passwordsSolved = 0;
    }

    ////////////////////
    // Actor Messages //
    ////////////////////

    @Data
    public static class StartMessage implements Serializable {
        private static final long serialVersionUID = -50374816448627600L;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class BatchMessage implements Serializable {
        private static final long serialVersionUID = 8343040942748609598L;
        private List<String[]> lines;
    }

    @Data
    public static class RegistrationMessage implements Serializable {
        private static final long serialVersionUID = 3303081601659723997L;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class HintCrackedMessage implements Serializable {
        private static final long serialVersionUID = 744312234949119713L;
        private String message;
    }

    /////////////////
    // Actor State //
    /////////////////

    private final ActorRef reader;
    private final ActorRef collector;
    private final List<ActorRef> workers;

    private long startTime;

	private Queue<ActorRef> workersQueue;
    private Queue<Worker.PasswordCharMessage> messageQueue;
    private ArrayList<String> passwordHashList;
    private Multimap<String, Integer> hintList;
    private int passwordsSolved;

    // Queue with combinations of letters that need to be solved

    /////////////////////
    // Actor Lifecycle //
    /////////////////////

    @Override
    public void preStart() {
        Reaper.watchWithDefaultReaper(this);
    }

    ////////////////////
    // Actor Behavior //
    ////////////////////

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(StartMessage.class, this::handle)
                .match(BatchMessage.class, this::handle)
                .match(Terminated.class, this::handle)
                .match(RegistrationMessage.class, this::handle)
                .match(HintCrackedMessage.class, this::handle)
                .matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
                .build();
    }

    protected void handle(StartMessage message) {
        this.startTime = System.currentTimeMillis();
        this.reader.tell(new Reader.ReadMessage(), this.self());
    }

    protected void handle(BatchMessage message) {

        ///////////////////////////////////////////////////////////////////////////////////////////////////////
        // The input file is read in batches for two reasons: /////////////////////////////////////////////////
        // 1. If we distribute the batches early, we might not need to hold the entire input data in memory. //
        // 2. If we process the batches early, we can achieve latency hiding. /////////////////////////////////
        // TODO: Implement the processing of the data for the concrete assignment. ////////////////////////////
        ///////////////////////////////////////////////////////////////////////////////////////////////////////

        if (message.getLines().isEmpty()) {
            this.collector.tell(new Collector.PrintMessage(), this.self());
            this.terminate();
            return;
        }

        // Save all pw and hint hashes and create pw id
        getAllHints(message.getLines());
        // Data structure to track how many hints are solved (and what the possible solution set is)
        // Counter for solved pws (-> terminate when all are solved)
        // Which combinations need still to be solved

        // Set the hint solving worker in motion (he gets all hint hashes with pw ids)
        // Each worker requests a combination of 10 letters to solve (If there are no left, they die)

        // 1 worker generates permutations (for his 10 letters), hashes them and compares them to the known hints
        // If gets a hit, he sends the pw id and letter combination to the master
        // The master deletes the hint hash and adjusts the solution set for the password

        // If the solution set is of size x the master starts a worker to guess the pw hash out of the limited solution set
        this.collector.tell(new Collector.CollectMessage("Processed batch of size " + message.getLines().size()), this.self());
        this.reader.tell(new Reader.ReadMessage(), this.self());

        //send the workers the hints
        while (!workersQueue.isEmpty()) {
            ActorRef workerRef = workersQueue.peek();
            workerRef.tell(new Worker.InitializeHintsMessage(this.hintList), this.getSelf());
            workersQueue.remove();
        }
    }

    private void getAllHints(List<String[]> lines) {
        for (String[] line : lines) {
            int pwID = Integer.parseInt(line[0]);
            this.passwordHashList.add(line[4]);

            hintList.put(line[5], pwID);
            hintList.put(line[6], pwID);
            hintList.put(line[7], pwID);
            hintList.put(line[8], pwID);
            hintList.put(line[9], pwID);
            hintList.put(line[10], pwID);
            hintList.put(line[11], pwID);
            hintList.put(line[12], pwID);
            hintList.put(line[13], pwID);
        }
    }

    protected void terminate() {
        this.reader.tell(PoisonPill.getInstance(), ActorRef.noSender());
        this.collector.tell(PoisonPill.getInstance(), ActorRef.noSender());

        for (ActorRef worker : this.workers) {
            this.context().unwatch(worker);
            worker.tell(PoisonPill.getInstance(), ActorRef.noSender());
        }

        this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());

        long executionTime = System.currentTimeMillis() - this.startTime;
        this.log().info("Algorithm finished in {} ms", executionTime);
    }

    // TODO handle work request from worker
    // remove first queue element and send it to worker
    // if queue is empty start solving password hashes
    protected void handle(HintCrackedMessage message) {
		this.log().info("{} is ready to work.", this.sender());
		createPasswordCharCombinations();
		this.workersQueue.add(this.sender());
		while (!workersQueue.isEmpty() && !messageQueue.isEmpty()) {
			ActorRef workerRef = workersQueue.peek();
			Worker.PasswordCharMessage messageToSend = messageQueue.peek();
			workerRef.tell(messageToSend, this.getSelf());
			workersQueue.remove();
			messageQueue.remove();
		}
	}

    // TODO handle hint solution from worker
    // remove hash from all password hash lists (mit den pw ids)
    // adjust password solution set
    // if pw solution set has size X (maybe = 4) add password hash plus solution set to worker queue

    // TODO handle password solve
    // increment solved pw counter by 1
    // print solved password
    // empty hash list for password
    // if all pws are solved terminate system

    protected void handle(RegistrationMessage message) {
        this.context().watch(this.sender());
        this.workers.add(this.sender());
        this.workersQueue.add(this.sender());
//		this.log().info("Registered {}", this.sender());
    }

    protected void handle(Terminated message) {
        this.context().unwatch(message.getActor());
        this.workers.remove(message.getActor());
//		this.log().info("Unregistered {}", message.getActor());
    }

    private void createPasswordCharCombinations() {
        if (messageQueue.isEmpty()) {
            List<String> stringSet = Arrays.asList("A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K");
            for (int i = stringSet.size() - 1; i >= 0; i--) {
                List<String> newList = new ArrayList<>(stringSet);
                String removedStr = newList.remove(i);
                messageQueue.add(new Worker.PasswordCharMessage(newList, removedStr));
            }
        }
    }
}
