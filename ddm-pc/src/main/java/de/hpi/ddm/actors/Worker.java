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
    public static class PasswordCharMessage implements Serializable {
        private static final long serialVersionUID = 3141460778041873236L;
        private List<List<String>> passwordChars;
    }

    // TODO Message from master to worker to tell him hint combination to solve
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CharacterPermutationMessage implements Serializable {
        private static final long serialVersionUID = 2432730560556273732L;
        private List<String> charsToPermutate;
    }

    // TODO Message from worker to master to tell him solution for a hint and for which passwords
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class HintSolutionMessage implements Serializable {
        private static final long serialVersionUID = -1441743248985563055L;
        private Set<String> solutionChars;
        private Collection<Integer> passwordIDs;
    }


    // TODO Message form master to worker to tell him password plus solution set to solve

    // TODO Message from worker to master to tell him solution to a password id

    /////////////////
    // Actor State //
    /////////////////

    private Member masterSystem;
    private final Cluster cluster;
    private Multimap<String, Integer> hints;

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
                .match(InitializeHintsMessage.class, this::handle)
                .match(PasswordCharMessage.class, this::handle)
                .match(CharacterPermutationMessage.class, this::handle)
                .matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
                .build();
    }

    // TODO handle hint combination solving
    protected void handle(CharacterPermutationMessage message) {
        int i = 0;
        int[] c = new int[10];
        List<String> hintLetters = message.getCharsToPermutate();
        Arrays.fill(c, 0);
        String hash = generateSHA256Hash(String.join("", hintLetters));
        if(hints.containsKey(hash)) {
            this.getSender().tell(new HintSolutionMessage(new HashSet<>(hintLetters), hints.get(hash)), this.getSelf());
        }

        int hintLettersSize = hintLetters.size();
        int counter = 0;
//        final int COUNT_PASSWORD = 100;

        while(i < hintLettersSize) {// && counter <= COUNT_PASSWORD) {
            if(c[i] < i) {
                if((c[i] & 1) == 0) {
                    Collections.swap(hintLetters, 0, i);
                } else {
                    Collections.swap(hintLetters, c[i], i);
                }
                hash = generateSHA256Hash(String.join("", hintLetters));
                if(hints.containsKey(hash)) {
                    this.getSender().tell(new HintSolutionMessage(new HashSet<>(hintLetters), hints.get(hash)), this.getSelf());
                }
                c[i] += 1;
                i = 0;
                counter += 1;
            } else {
                c[i] = 0;
                i += 1;
            }
        }
        this.getSender().tell(new Master.ReadyToWorkMessage(), this.getSelf());
    }

    // TODO handle password solving

    private void handle(PasswordCharMessage message) {
        this.log().info("this is my massage: {}", message.passwordChars);
        List<String> result = new ArrayList<>();
        for (List<String> passwordChar : message.passwordChars) {
            String str = passwordChar.toString().replaceAll(",", "");
            char[] chars = str.substring(1, str.length() - 1).replaceAll(" ", "").toCharArray();
//			StringUtils.heapPermutation(chars, chars.length, chars.length, result);
//			this.log().info(result.toString());
        }
    }

    private void handle(InitializeHintsMessage message) {
        hints = HashMultimap.create();
        for (int index = 0; index < message.getHints().size(); index++) {
            for(String hint : message.getHints().get(index)) {
                this.hints.put(hint, index);
            }
        }
        System.out.println(this.hints.keys().size());
        this.getSender().tell(new Master.ReadyToWorkMessage(), this.getSelf());
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