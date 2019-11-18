package de.hpi.ddm.actors;

import akka.actor.AbstractLoggingActor;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static de.hpi.ddm.utils.StringUtils.generateSHA256Hash;

public class HashGenerationWorker extends AbstractLoggingActor {
    @Override
    public Receive createReceive() {
        return null;
    }
    //int[] c = new int[stringList.size()];
    //int i = 0;
    private List<String> generatePermutations(List<String> stringList, int[] c, int i) {
        List<String> result = new LinkedList<>();

        //int[] c = new int[stringList.size()];
        Arrays.fill(c, 0);
        result.add(generateSHA256Hash(String.join("", stringList)));

        //int i = 0;
        int counter = 0;
        int listSize = stringList.size();
        while(i < listSize && counter <= 100) {
            if(c[i] < i) {
                if((c[i] & 1) == 0) {
                    Collections.swap(stringList, 0, i);
                } else {
                    Collections.swap(stringList, c[i], i);
                }
                result.add(generateSHA256Hash(String.join("", stringList)));
                c[i] += 1;
                i = 0;
                counter += 1;
            } else {
                c[i] = 0;
                i += 1;
            }
        }
        return result;
    }
}
