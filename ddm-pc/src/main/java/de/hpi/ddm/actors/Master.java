package de.hpi.ddm.actors;

import akka.actor.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

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
        this.initializedWorker = new ArrayList<>();

        this.passwordHashList = new ArrayList<>(100);
        this.hintList = new ArrayList<>(100);
        this.passwordSolutionSetList = new ArrayList<>(100);
        this.passwordCharVariationQueue = new LinkedList<>();
        this.messageQueue = new LinkedList<>();
        this.passwordsSolved = 0;
	}

    ////////////////////
    // Actor Messages //
    ////////////////////

    // TODO Message to tell the master the worker is ready for new work
    @Data
    public static class ReadyToWorkMessage implements Serializable {
        private static final long serialVersionUID = 3406196888271074557L;
    }

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

    @Data
    public static class LoopMessage implements Serializable {
        private static final long serialVersionUID = 4448431623231380480L;
    }


    /////////////////
    // Actor State //
    /////////////////

    private final ActorRef reader;
    private final ActorRef collector;
    private final List<ActorRef> workers;

    private long startTime;

	private Queue<List<String>> passwordCharVariationQueue;
	private Queue<Worker.InitializeHintsMessage> messageQueue;
    private ArrayList<String> passwordHashList;
    private ArrayList<List<String>> hintList;
    private ArrayList<Set<String>> passwordSolutionSetList;
    private int passwordsSolved;
    private boolean done = false;
    private ArrayList<Boolean> initializedWorker;
    private Set<String> possibleChars = new HashSet<>(Arrays.asList("A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K")); // TODO make this dynamic from input


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
                .match(ReadyToWorkMessage.class, this::handle)
                .match(LoopMessage.class, this::handle)
                .match(Worker.HintSolutionMessage.class, this::handle)
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
            for (ActorRef workerRef : workers) {
                workerRef.tell(new Worker.InitializeHintsMessage((List<List<String>>) hintList.clone()), this.getSelf());
                // TODO when all batches are finished workerRef.tell() to tell them that they can start working.
                // TODO For now it works but needs to be inspected
            }
            return;
        }

        // Save all pw and hint hashes and create pw id
        getAllHints(message.getLines());
        createPasswordCharCombinations();
        // Data structure to track how many hints are solved (and what the possible solution set is)
        // Counter for solved pws (-> terminate when all are solved)
        // Which combinations need still to be solved

        // Set the hint solving worker in motion (he gets all hint hashes with pw ids)
        // Each worker requests a combination of 10 letters to solve (If there are no left, they die)

        // 1 worker generates permutations (for his 10 letters), hashes them and compares them to the known hints
        // If gets a hit, he sends the pw id and letter combination to the master
        // The master deletes the hint hash and adjusts the solution set for the password

        // If the solution set is of size x the master starts a worker to guess the pw hash out of the limited solution set
        this.collector.tell(new Collector.CollectMessage("Processed batch of size " + hintList.size()), this.self());
        this.reader.tell(new Reader.ReadMessage(), this.self());


    }

    private void getAllHints(List<String[]> lines) {
        for (String[] line : lines) {
            this.passwordHashList.add(line[4]);
            int pwID = this.passwordHashList.size() - 1;
            List<String> hints = new LinkedList<>();
            hints.add(line[5]);
            hints.add(line[6]);
            hints.add(line[7]);
            hints.add(line[8]);
            hints.add(line[9]);
            hints.add(line[10]);
            hints.add(line[11]);
            hints.add(line[12]);
            hints.add(line[13]);
            this.hintList.add(pwID, hints);
            this.passwordSolutionSetList.add(pwID, new HashSet<>(Arrays.asList("A","B","C","D","E","F","G","H","I","J","K")));
//            getSelf().tell(new LoopMessage(), this.getSelf());
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

    protected void handle(LoopMessage message) {
        if(done) {
            context().system().terminate();
        } else {
            if(initializedWorker.contains(false)) {
                for(int index = 0; index < workers.size(); index++) {
                    if(!initializedWorker.get(index)) {
                        workers.get(index).tell(new Worker.InitializeHintsMessage((List<List<String>>) hintList.clone()), getSelf());
                        initializedWorker.set(index, true);
                    }
                }
            }
            for (ActorRef workerRef : workers) {
                if(!passwordCharVariationQueue.isEmpty()) {
                    workerRef.tell(new Worker.CharacterPermutationMessage(passwordCharVariationQueue.remove()), this.getSelf());
                }
            }
            getSelf().tell(new LoopMessage(), this.getSelf());
        }
    }

    // TODO handle work request from worker
    // remove first queue element and send it to worker
    // if queue is empty start solving password hashes
    protected void handle(ReadyToWorkMessage message) {
            if(!passwordCharVariationQueue.isEmpty()) {
                this.getSender().tell(new Worker.CharacterPermutationMessage(passwordCharVariationQueue.remove()), this.getSelf());
            } else {
//                self().tell(new LoopMessage(), getSelf());
            }

    }

    // TODO handle hint solution from worker
    // remove hash from all password hash lists (mit den pw ids)
    // adjust password solution set
    // if pw solution set has size X (maybe = 4) add password hash plus solution set to worker queue
    protected void handle(Worker.HintSolutionMessage message) {
        for(int pwID : message.getPasswordIDs()) {
            passwordSolutionSetList.set(pwID, passwordSolutionSetList.get(pwID).stream().filter(message.getSolutionChars()::contains).collect(Collectors.toSet())); // TODO make this more efficient: We can save the removed letter together with the combination, and therefore have him here at hand
            if(passwordSolutionSetList.get(pwID).size() <= 9) {
                System.out.println("PW: " + pwID + " with possible chars: " + passwordSolutionSetList.get(pwID));
            }
        }
    }

    // TODO handle password solve
    // increment solved pw counter by 1
    // print solved password
    // empty hash list for password
    // if all pws are solved terminate system

    protected void handle(RegistrationMessage message) {
        this.context().watch(this.sender());
        this.workers.add(this.sender());
//        this.initializedWorker.add(false);
//        if(!hintList.isEmpty()) {
//            this.sender().tell(new Worker.InitializeHintsMessage((List<List<String>>) hintList.clone()), getSelf());
//            this.initializedWorker.set(initializedWorker.size()-1, true);
//            if(!passwordCharVariationQueue.isEmpty()) {
//                this.sender().tell(new Worker.CharacterPermutationMessage(passwordCharVariationQueue.remove()), this.getSelf());
//            }
//        }
//		this.log().info("Registered {}", this.sender());
    }

    protected void handle(Terminated message) {
        this.context().unwatch(message.getActor());
        this.workers.remove(message.getActor());
		this.log().info("Unregistered {}", message.getActor());
    }

    private void createPasswordCharCombinations() {
            List<String> stringSet = Arrays.asList("A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K");
            for (int i = stringSet.size() - 1; i >= 0; i--) {
                List<String> newList = new ArrayList<>(stringSet);
				String removedStr = newList.remove(i);
				passwordCharVariationQueue.add(newList);
            }
    }

}
