package storm.resa.util;

import java.util.ArrayList;

/**
 * Created by Tom.fu on 23/4/2014.
 */
public class UsefulMethod {
    public static int factorial (int n) {
        if (n < 0) {
            System.out.println("Attention, negative input is not allowed: " + n);
            return -1;
        } else if (n == 0) {
            return 1;
        } else {
            int ret = 1;
            for (int i = 2; i <= n; i ++) {
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
}
