package de.hpi.ddm.actors;

import akka.actor.AbstractLoggingActor;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class HashGenerationWorker extends AbstractLoggingActor {
    @Override
    public Receive createReceive() {
        return null;
    }


    //int[] c = new int[a.size()];
    //int i = 0;
    private List<String> generatePermutations(List<String> a, int[] c, int i) {
        List<String> result = new LinkedList<>();
        int size_a = a.size();

        //int[] c = new int[a.size()];
        Arrays.fill(c, 0);
        result.add(hash(String.join("", a)));

        //int i = 0;
        int counter = 0;
        while(i < a.size() && counter <= 100) {
            if(c[i] < i) {
                if((c[i] & 1) == 0) {
                    Collections.swap(a, 0, i);
                } else {
                    Collections.swap(a, c[i], i);
                }
                result.add(hash(String.join("", a)));
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

    private String hash(String line) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hashedBytes = digest.digest(String.valueOf(line).getBytes("UTF-8"));

            StringBuffer stringBuffer = new StringBuffer();
            for (int i = 0; i < hashedBytes.length; i++) {
                stringBuffer.append(Integer.toString((hashedBytes[i] & 0xff) + 0x100, 16).substring(1));
            }
            return stringBuffer.toString();
        }
        catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
            throw new RuntimeException(e.getMessage());
        }
    }
}
