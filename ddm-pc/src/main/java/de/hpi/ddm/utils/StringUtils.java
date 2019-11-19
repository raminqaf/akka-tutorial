package de.hpi.ddm.utils;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;

public class StringUtils {
    // Generating all permutations of an array using Heap's Algorithm
    // https://en.wikipedia.org/wiki/Heap's_algorithm
    // https://www.geeksforgeeks.org/heaps-algorithm-for-generating-permutations/
    public static void heapPermutation(char[] a, int size, int n, List<String> results) {
        // If size is 1, store the obtained permutation
        if (size == 1)
            results.add(generateSHA256Hash(new String(a)));

        for (int i = 0; i < size; i++) {
            heapPermutation(a, size - 1, n, results);

            // If size is odd, swap first and last element
            if (size % 2 == 1) {
                char temp = a[0];
                a[0] = a[size - 1];
                a[size - 1] = temp;
            }

            // If size is even, swap i-th and last element
            else {
                char temp = a[i];
                a[i] = a[size - 1];
                a[size - 1] = temp;
            }
        }
    }

    static void printAllKLengthRec(char[] set,
                                   String prefix,
                                   int n, int k, int validCount) {

        // Base case: k is 0, print prefix
        if (k == 0) {
            System.out.println(prefix);
            return;
        }

        // One by one add all valid characters and recursively call for k equals to k-1
        for (int i = 0; i < validCount; ++i) {

            // Next character of input added
            String newPrefix = prefix + set[i];

            // increment the valid count if all characters up till then have already
            // appeared and there are characters that have not yet appeared
            // (i.e. validCount < n)
            int newValidCount = (i == (validCount - 1)) && (validCount < n) ?
                    validCount + 1 :
                    validCount;

            // k is decreased, because we have added a new character
            printAllKLengthRec(set, newPrefix,
                    n, k - 1, newValidCount);
        }
    }

    public static String generateSHA256Hash(String line) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hashedBytes = digest.digest(String.valueOf(line).getBytes(StandardCharsets.UTF_8));

            StringBuilder stringBuilder = new StringBuilder();
            for (byte hashedByte : hashedBytes) {
                stringBuilder.append(Integer.toString((hashedByte & 0xff) + 0x100, 16).substring(1));
            }
            return stringBuilder.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e.getMessage());
        }
    }
}
