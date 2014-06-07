package storm.resa.util;

import junit.framework.TestCase;

import java.nio.file.Paths;
import java.util.*;

public class UsefulMethodTest extends TestCase {

    public void testPowerCombination() throws Exception {

        int[] num = new int[]{1, 2, 3, 5};
        UsefulMethod.powerCombination(num);
    }

    public void testSortHaspMapByIntegerValue() {
        Map<String, Integer> input = new HashMap<>();
        input.put("Tom", 10);
        input.put("John", 13);
        input.put("Jack", 5);
        input.put("Jack1", 45);
        input.put("Jack2", 35);
        input.put("Jack3", 25);
        input.put("Jack4", 15);


        UsefulMethod.sortHaspMapByIntegerValue(input);
    }

    public void testFindTopKKeywords() {
        String workPath = "C:\\Users\\Tom.fu\\Desktop\\storm-experiment\\";
        ///String inputFile = workPath + "forTest.txt";
        String inputFile = workPath + "tweet.txt";
        String stopWordFile = workPath + "english.stop.txt";
        String output = workPath + "forTweetOutput.txt";

        UsefulMethod.findTopKKeywords(100000, Paths.get(inputFile),Paths.get(stopWordFile),Paths.get(output));


    }
}