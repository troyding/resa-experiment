package storm.resa.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/**
 * Created by Tom.fu on 23/4/2014.
 */
public class UsefulMethod {
    public static int factorial(int n) {
        if (n < 0) {
            System.out.println("Attention, negative input is not allowed: " + n);
            return -1;
        } else if (n == 0) {
            return 1;
        } else {
            int ret = 1;
            for (int i = 2; i <= n; i++) {
                ret = ret * i;
            }
            return ret;
        }
    }

    public static void powerCombination(int[] numbers) {

        int n = numbers.length;

        for (int i = 1; i < (1 << n); ++i) {
            ArrayList<Integer> emitNumber = new ArrayList<Integer>();
            for (int j = 0; j < n; ++j)
                if ((i & (1 << j)) > 0) {
                    emitNumber.add(numbers[j]);
                }
            System.out.println(i + "," + emitNumber);
        }
    }

    public static void powerCombinationExclude(int[] numbers) {

        int n = numbers.length;

        for (int i = 1; i < (1 << n) - 1; ++i) {
            ArrayList<Integer> emitNumber = new ArrayList<Integer>();
            for (int j = 0; j < n; ++j)
                if ((i & (1 << j)) > 0) {
                    emitNumber.add(numbers[j]);
                }
            System.out.println(i + "," + emitNumber);
        }
    }

    public static void findTopKKeywords(int topK, Path inputFile, Path stopWordFile, Path outputFile) {

        Map<String, Integer> wordCountMap = new HashMap<>();
        int readLineCount = 0;

        try (BufferedReader reader = Files.newBufferedReader(inputFile)) {
            reader.lines().forEach((line) -> {
                StringTokenizer tokenizer = new StringTokenizer(line.replaceAll("\\p{P}|\\p{S}", " "));
                while (tokenizer.hasMoreTokens()) {
                    String word = tokenizer.nextToken().toLowerCase().trim();
                    if (!word.isEmpty()) {
                        wordCountMap.putIfAbsent(word, 0);
                        wordCountMap.computeIfPresent(word, (k, v) -> v + 1);
                    }
                }
            });
        } catch (Exception e) {
            System.out.println(e.getStackTrace());
        }

        try (BufferedReader reader = Files.newBufferedReader(stopWordFile)) {
            reader.lines().forEach((line) -> {
                String word = line.toLowerCase().trim();
                if (!word.isEmpty()) {
                    wordCountMap.computeIfPresent(word, (k, v) -> 0);
                }
            });
        } catch (Exception e) {
            System.out.println(e.getStackTrace());
        }

        try (BufferedWriter writer = Files.newBufferedWriter(outputFile)) {
            wordCountMap.entrySet().stream().sorted((e1, e2) -> Integer.compare(e2.getValue(), e1.getValue()))
                    .limit(topK).forEach((e) -> {
                try {
                    writer.write(e.getKey() + "," + e.getValue());
                    writer.newLine();
                } catch (Exception e1) {
                }
            });
        } catch (Exception e) {
            System.out.println(e.getStackTrace());
        }
    }

    public static void sortHaspMapByIntegerValue(Map<String, Integer> input) {
        input.entrySet().stream().sorted((e1, e2) -> Integer.compare(e2.getValue(), e1.getValue())).limit(4)
                .forEach((e) -> System.out.println(e.getKey() + "," + e.getValue()));
    }
}
