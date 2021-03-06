package de.hpi.ddm.actors;

import akka.actor.*;
import de.hpi.ddm.data.Line;
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

        this.passwordChars = new ArrayList<>(11);
        this.passwordHashList = new ArrayList<>(100);
        this.hintList = new ArrayList<>(100);
        this.passwordSolutionSetList = new ArrayList<>(100);
        this.passwordCharVariationQueue = new LinkedList<>();
        this.passwordSolutionSetQueue = new LinkedList<>();
        this.passwordsSolved = 0;
        this.passwordLength = 0;
    }

    ////////////////////
    // Actor Messages //
    ////////////////////

    // Message to tell the master the worker is ready for new work
    @Data
    public static class ReadyToWorkMessage implements Serializable {
        private static final long serialVersionUID = 3406196888271074557L;
    }

    @Data
    public static class StartMessage implements Serializable {
        private static final long serialVersionUID = -50374816448627600L;
    }

    @Data
    public static class LoopMessage implements Serializable {
        private static final long serialVersionUID = 8091557030536129535L;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class BatchMessage implements Serializable {
        private static final long serialVersionUID = 8343040942748609598L;
        private List<Line> lines;
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
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CharVariation implements Serializable {
        private static final long serialVersionUID = -3036195152026916472L;
        private List<String> chars;
        private String removedChar;
    }


    /////////////////
    // Actor State //
    /////////////////

    private final ActorRef reader;
    private final ActorRef collector;
    private final List<ActorRef> workers;

    private long startTime;

    private List<String> passwordChars;
    private Queue<CharVariation> passwordCharVariationQueue;
    private ArrayList<String> passwordHashList;
    private ArrayList<Set<String>> hintList;
    private ArrayList<Set<String>> passwordSolutionSetList;
    private int passwordsSolved;
    private int passwordLength;

    private Queue<Worker.PasswordSolutionSetMessage> passwordSolutionSetQueue;


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
                .match(Worker.HintSolutionMessage.class, this::handle)
                .match(Worker.PasswordSolutionMessage.class, this::handle)
                .matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
                .build();
    }

    protected void handle(BatchMessage message) {
        if (message.getLines().isEmpty() && !this.passwordChars.isEmpty()) {
            this.collector.tell(new Collector.PrintMessage(), this.self());
            createPasswordCharCombinations();
            for (ActorRef workerRef : workers) {
                if (!passwordCharVariationQueue.isEmpty()) {
                    workerRef.tell(new Worker.CharacterPermutationMessage(passwordCharVariationQueue.remove(), (List<Set<String>>) hintList.clone()), this.getSelf());
                }
                // TODO optional Batch the hints
            }
            return;
        }

        // Save all pw and hint hashes and create pw id
        this.passwordChars = message.getLines().get(0).getPasswordChars();
        this.passwordLength = message.getLines().get(0).getPasswordLength();
        getAllHints(message.getLines());
        this.collector.tell(new Collector.CollectMessage("Processed batch of size " + hintList.size()), this.self());
        this.reader.tell(new Reader.ReadMessage(), this.self());

        // Data structure to track how many hints are solved (and what the possible solution set is)
        // Counter for solved pws (-> terminate when all are solved)
        // Which combinations need still to be solved

        // Set the hint solving worker in motion (he gets all hint hashes with pw ids)
        // Each worker requests a combination of 10 letters to solve (If there are no left, they die)

        // 1 worker generates permutations (for his 10 letters), hashes them and compares them to the known hints
        // If gets a hit, he sends the pw id and letter combination to the master
        // The master deletes the hint hash and adjusts the solution set for the password

        // If the solution set is of size x the master starts a worker to guess the pw hash out of the limited solution set


    }

    private void getAllHints(List<Line> lines) {
        for (Line line : lines) {
            String passwordHash = line.getHashedPassword();
            this.passwordHashList.add(passwordHash);
            int pwID = this.passwordHashList.size() - 1;
            Set<String> hints = new HashSet<>(line.getHints());
            this.hintList.add(pwID, hints);
            this.passwordSolutionSetList.add(pwID, new HashSet<>(line.getPasswordChars()));
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

    // handle work request from worker
    // remove first queue element and send it to worker
    // if queue is empty start solving password hashes
    protected void handle(ReadyToWorkMessage message) {
        if (!passwordCharVariationQueue.isEmpty()) {
            this.getSender().tell(new Worker.CharacterPermutationMessage(passwordCharVariationQueue.remove(), (List<Set<String>>) hintList.clone()), this.getSelf());
        } else if (!passwordSolutionSetQueue.isEmpty()) {
            this.getSender().tell(passwordSolutionSetQueue.remove(), getSelf());
        } else {
            getSender().tell(new LoopMessage(), getSelf());
        }

    }

    // handle hint solution from worker
    // remove hash from all password hash lists (mit den pw ids)
    // adjust password solution set
    // if pw solution set has size X (maybe = 4) add password hash plus solution set to worker queue
    protected void handle(Worker.HintSolutionMessage message) {
        for (int pwID : message.getPasswordIDs()) {
            hintList.get(pwID).remove(message.getHash());
            passwordSolutionSetList.get(pwID).remove(message.getSolutionChars().getRemovedChar());
            if (passwordSolutionSetList.get(pwID).size() <= 4 || hintList.get(pwID).size() == 0) {
                passwordSolutionSetQueue.add(new Worker.PasswordSolutionSetMessage(passwordSolutionSetList.get(pwID), passwordHashList.get(pwID), pwID, passwordLength));
                hintList.set(pwID, new HashSet<>());
            }
        }
    }

    protected void handle(Worker.PasswordSolutionMessage message) {
        System.out.println(message.getPassword());
        passwordsSolved += 1;
        if (passwordsSolved == passwordHashList.size()) {
            this.terminate();
        }
    }

    // handle password solve
    // increment solved pw counter by 1
    // print solved password
    // empty hash list for password
    // if all pws are solved terminate system

    private void createPasswordCharCombinations() {
        // TODO optional: split this in smaller ranges
        for (int i = passwordChars.size() - 1; i >= 0; i--) {
            List<String> newList = new ArrayList<>(passwordChars);
            String removedChar = newList.get(i);
            newList.remove(i);
            passwordCharVariationQueue.add(new CharVariation(newList, removedChar));
        }
    }

    protected void handle(RegistrationMessage message) {
        this.context().watch(this.sender());
        this.workers.add(this.sender());
        getSender().tell(new LoopMessage(), getSelf());
//		this.log().info("Registered {}", this.sender());
    }

    protected void handle(Terminated message) {
        this.context().unwatch(message.getActor());
        this.workers.remove(message.getActor());
        this.log().info("Unregistered {}", message.getActor());
    }

    protected void handle(StartMessage message) {
        this.startTime = System.currentTimeMillis();
        this.reader.tell(new Reader.ReadMessage(), this.self());
    }
}
