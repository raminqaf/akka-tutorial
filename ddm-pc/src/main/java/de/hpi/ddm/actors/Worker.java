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
import de.hpi.ddm.utils.StringUtils;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.*;

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
        this.hints = HashMultimap.create();
    }

    ////////////////////
    // Actor Messages //
    ////////////////////

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class InitializeHintsMessage implements Serializable {
        private static final long serialVersionUID = 7327181886368767987L;
        private Multimap<String, Integer> hints;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class PasswordCharMessage implements Serializable {
        private static final long serialVersionUID = -1441743248985563055L;
		private List<String> passwordPermutation;
		private String keyString;
    }

    // TODO Message to tell the master the worker is ready for new work

    // TODO Message from master to worker to tell him hint combination to solve

    // TODO Message from worker to master to tell him solution for a hint and for which passwords

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
                .matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
                .build();
    }

    // TODO handle hint combination solving

    // TODO handle password solving

    private void handle(PasswordCharMessage message) {
    	System.out.println(message.keyString);
    }

    private void handle(InitializeHintsMessage message) {
//        List<String> results = new ArrayList<>();
//        List<String> permutation = message.getPasswordPermutation();
//        List<String> solvedHashes = new ArrayList<>();
//        String str = permutation.toString().replaceAll(",", "");
//        char[] chars = str.substring(1, str.length() - 1).replaceAll(" ", "").toCharArray();
//        StringUtils.heapPermutation(chars, chars.length, chars.length, results);
//
//		for (String result: results) {
//			for (String hint: message.hints) {
//				if (hint.equals(result))
//					solvedHashes.add(result);
//			}
//		}
		hints = message.hints;
        this.sender().tell(new Master.HintCrackedMessage(this.self().toString()), this.self());
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