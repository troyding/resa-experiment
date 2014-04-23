package storm.resa.util;

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
}
