package de.hpi.ddm.actors;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent.CurrentClusterState;
import akka.cluster.ClusterEvent.MemberRemoved;
import akka.cluster.ClusterEvent.MemberUp;
import akka.cluster.Member;
import akka.cluster.MemberStatus;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import de.hpi.ddm.MasterSystem;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.*;

import static de.hpi.ddm.utils.StringUtils.generateSHA256Hash;

public class Worker extends AbstractLoggingActor {

    ////////////////////////
    // Actor Construction //
    ////////////////////////

    public static final String DEFAULT_NAME = "worker";

    public static Props props() {
        return Props.create(Worker.class);
    }

    public Worker() {
        this.cluster = Cluster.get(this.context().system());
    }

    ////////////////////
    // Actor Messages //
    ////////////////////

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class InitializeHintsMessage implements Serializable {
        private static final long serialVersionUID = 8555259510371359233L;
        private List<List<String>> hints;
    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class PasswordSolutionSetMessage implements Serializable {
        private static final long serialVersionUID = 3141460778041873236L;
        private Set<String> passwordChars;
        private String passwordHash;
        private int passwordID;
    }

    // TODO Message from master to worker to tell him hint combination to solve
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CharacterPermutationMessage implements Serializable {
        private static final long serialVersionUID = 2432730560556273732L;
        private List<String> charsToPermutate;
        private List<Set<String>> hints;
    }

    // TODO Message from worker to master to tell him solution for a hint and for which passwords
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class HintSolutionMessage implements Serializable {
        private static final long serialVersionUID = -1441743248985563055L;
        private Set<String> solutionChars;
        private Collection<Integer> passwordIDs;
        private String hash;
    }


    // TODO Message form master to worker to tell him password plus solution set to solve
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class PasswordSolutionMessage implements Serializable {
        private static final long serialVersionUID = -4895487864457713162L;
        private String password;
        private int passwordID;
    }

    // TODO Message from worker to master to tell him solution to a password id



    /////////////////
    // Actor State //
    /////////////////

    private Member masterSystem;
    private final Cluster cluster;
    private Multimap<String, Integer> hints;
    private Set<String> currentChars;
    private String currentPasswordHash;
    private int currentPasswordID;

    /////////////////////
    // Actor Lifecycle //
    /////////////////////

    @Override
    public void preStart() {
        Reaper.watchWithDefaultReaper(this);

        this.cluster.subscribe(this.self(), MemberUp.class, MemberRemoved.class);
    }

    @Override
    public void postStop() {
        this.cluster.unsubscribe(this.self());
    }

    ////////////////////
    // Actor Behavior //
    ////////////////////

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(CurrentClusterState.class, this::handle)
                .match(MemberUp.class, this::handle)
                .match(MemberRemoved.class, this::handle)
                .match(PasswordSolutionSetMessage.class, this::handle)
                .match(CharacterPermutationMessage.class, this::handle)
                .matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
                .build();
    }

    // TODO handle hint combination solving
    protected void handle(CharacterPermutationMessage message) {
        hints = HashMultimap.create();
        for (int index = 0; index < message.getHints().size(); index++) {
            for(String hint : message.getHints().get(index)) {
                hints.put(hint, index);
            }
        }
        currentChars = new HashSet<>(message.getCharsToPermutate());
        if(hints.keys().isEmpty()) {
            this.getSender().tell(new Master.ReadyToWorkMessage(), this.getSelf());
            return;
        }
        System.out.println(hints.keys().size());
        char[] a = new char[message.getCharsToPermutate().size()];
        int index = 0;
        for(String s : message.getCharsToPermutate()) {
            a[index] = s.toCharArray()[0];
            index++;
        }
        heapPermutation(a, a.length, a.length);
        this.getSender().tell(new Master.ReadyToWorkMessage(), this.getSelf());
    }
    void heapPermutation(char a[], int size, int n) {
        // if size becomes 1 then prints the obtained
        // permutation
        if (size == 1) {
            String hash = generateSHA256Hash(new String(a));
            if(hints.containsKey(hash)) {
                this.getSender().tell(new HintSolutionMessage(currentChars, hints.get(hash), hash), this.getSelf());
            }
        }

        for (int i=0; i<size; i++)
        {
            heapPermutation(a, size-1, n);

            // if size is odd, swap first and last
            // element
            if (size % 2 == 1)
            {
                char temp = a[0];
                a[0] = a[size-1];
                a[size-1] = temp;
            }

            // If size is even, swap ith and last
            // element
            else
            {
                char temp = a[i];
                a[i] = a[size-1];
                a[size-1] = temp;
            }
        }
    }

    // TODO handle password solving

    private void handle(PasswordSolutionSetMessage message) {
       currentPasswordID = message.getPasswordID();
       currentPasswordHash = message.getPasswordHash();
       Set<Set<String>> setsToTest = new HashSet<>();

       for(String s: message.getPasswordChars()) {
           for(String s1: message.getPasswordChars()) {
               if(s.equals(s1)) {
                   continue;
               }
               setsToTest.add(new HashSet<>(Arrays.asList(s, s1)));
           }
       }

       for(Set<String> comibation : setsToTest) {
           char[] a = new char[comibation.size()];
           int index = 0;
           for(String s : comibation) {
               a[index] = s.toCharArray()[0];
               index++;
           }
           if(printAllKLengthRec(a, "", a.length, 10)){
               this.getSender().tell(new Master.ReadyToWorkMessage(), this.getSelf());
               return;
           }
       }
       this.getSender().tell(new Master.ReadyToWorkMessage(), this.getSelf());
    }

    private boolean printAllKLengthRec(char[] set,
                                    String prefix,
                                    int n, int k) {

        // Base case: k is 0, print prefix
        if (k == 0) {
            if(currentPasswordID == 76) {
                if(generateSHA256Hash(prefix).equals(currentPasswordHash)) {
                    this.getSender().tell(new PasswordSolutionMessage(prefix, currentPasswordID), this.getSelf());
                    return true;
                }
            } else {
                if(generateSHA256Hash(prefix).equals(currentPasswordHash)) {
                    this.getSender().tell(new PasswordSolutionMessage(prefix, currentPasswordID), this.getSelf());
                    return true;
                }
            }
            return false;
        }

        // One by one add all valid characters and recursively call for k equals to k-1
        for (int i = 0; i < n; ++i) {

            // Next character of input added
            String newPrefix = prefix + set[i];

            // k is decreased, because we have added a new character
            if (printAllKLengthRec(set, newPrefix,
                    n, k - 1)) {
                return true;
            }
        }
        return false;
    }


    private void handle(CurrentClusterState message) {
        message.getMembers().forEach(member -> {
            if (member.status().equals(MemberStatus.up()))
                this.register(member);
        });
    }

    private void handle(MemberUp message) {
        this.register(message.member());
    }

    private void register(Member member) {
        if ((this.masterSystem == null) && member.hasRole(MasterSystem.MASTER_ROLE)) {
            this.masterSystem = member;

            this.getContext()
                    .actorSelection(member.address() + "/user/" + Master.DEFAULT_NAME)
                    .tell(new Master.RegistrationMessage(), this.self());
        }
    }

    private void handle(MemberRemoved message) {
        if (this.masterSystem.equals(message.member()))
            this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
    }
}