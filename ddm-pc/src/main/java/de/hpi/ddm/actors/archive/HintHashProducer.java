package de.hpi.ddm.actors.archive;

import de.hpi.ddm.actors.Worker;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static de.hpi.ddm.utils.StringUtils.generateSHA256Hash;

public class HintHashProducer extends Worker {

    /////////////////
    // Actor State //
    /////////////////
    // make this dynamic and distribute the ranges between workers
    private int i = 0;
    private int[] c = new int[10];
    private List<String> hintLetters = Arrays.asList("A","B","C","D","E","F","G","H","I","J");

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(String.class, this::handle)
                .build();
    }

    private void handle(String command) {
        if(command.equals("produce")) {
            List<String> result = generatePermutationHash();
            //  Tell the result to all queues/consumers
        }
    }

    private List<String> generatePermutationHash() {
        List<String> results = new LinkedList<>();
        Arrays.fill(c, 0);
        results.add(generateSHA256Hash(String.join("", this.hintLetters)));

        int hintLettersSize = this.hintLetters.size();
        int counter = 0;
        final int COUNT_PASSWORD = 100;

        while(i < hintLettersSize && counter <= COUNT_PASSWORD) {
            if(c[i] < i) {
                if((c[i] & 1) == 0) {
                    Collections.swap(this.hintLetters, 0, i);
                } else {
                    Collections.swap(this.hintLetters, c[i], i);
                }
                results.add(generateSHA256Hash(String.join("", this.hintLetters)));
                c[i] += 1;
                i = 0;
                counter += 1;
            } else {
                c[i] = 0;
                i += 1;
            }
        }
        return results;
//        int i = 0;
//        int[] c = new int[10];
//        List<String> hintLetters = message.getCharsToPermutate();
//        Arrays.fill(c, 0);
//        String hash = generateSHA256Hash(String.join("", hintLetters));
//        int hits = 0;
//        if(hints.containsKey(hash)) {
//            this.getSender().tell(new HintSolutionMessage(new HashSet<>(hintLetters), hints.get(hash), hash), this.getSelf());
//            hits++;
//        }
//
//        int hintLettersSize = hintLetters.size();
//        int counter = 1;
////        final int COUNT_PASSWORD = 100;
//
//        while(i < hintLettersSize) {// && counter <= COUNT_PASSWORD) {
//            if(c[i] < i) {
//                if((c[i] & 1) == 0) {
//                    Collections.swap(hintLetters, 0, i);
//                } else {
//                    Collections.swap(hintLetters, c[i], i);
//                }
//                hash = generateSHA256Hash(String.join("", hintLetters));
//                if(hints.containsKey(hash)) {
//                    this.getSender().tell(new HintSolutionMessage(new HashSet<>(hintLetters), hints.get(hash), hash), this.getSelf());
//                    System.out.println(hash);
//                    System.out.println(hintLetters);
//                    System.out.println(counter);
//                    hits++;
//                }
//                c[i] += 1;
//                i = 0;
//                counter += 1;
//            } else {
//                c[i] = 0;
//                i += 1;
//            }
//        }
//        System.out.println(counter);
//        System.out.println(hits);
    }
}
