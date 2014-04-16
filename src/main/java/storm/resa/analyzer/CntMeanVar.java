package storm.resa.analyzer;

/**
 * Created by Tom.fu on 16/4/2014.
 */
public class CntMeanVar {
    private long count = 0;
    private double total = 0;
    private double total_2 = 0;
    private int emptyEntryCnt = 0;

    void emptyEntryInr() {
        emptyEntryCnt++;
    }

    int getEmptyEntryCnt() {
        return emptyEntryCnt;
    }

    ///Add one measured valued
    void addOneNumber(double num) {
        count++;
        total += num;
        total_2 += (num * num);
    }

    ///Add aggregated measure values in one window
    void addAggWin(int aggCount, double aggVal, double aggVal_2) {
        count += aggCount;
        total += aggVal;
        total_2 += aggVal_2;
    }

    void combine(CntMeanVar cmr) {
        if (cmr != null) {
            count += cmr.count;
            total += cmr.total;
            total_2 += cmr.total_2;
        }
    }

    void clear() {
        count = 0;
        total = 0;
        total_2 = 0;
        emptyEntryCnt = 0;
    }

    long getCount() {
        return count;
    }

    double getTotal() {
        return total;
    }

    double getAvg() {
        return count == 0 ? 0.0 : total / (double) count;
    }

    double getAvg2() {
        return count == 0 ? 0.0 : total_2 / (double) count;
    }

    double getVar() {
        return getAvg2() - getAvg() * getAvg();
    }

    ///Square coefficient of variation (Scv), scv(X) = Var(X) / [E(X)*E(X)];
    double getScv() {
        return count == 0 ? 0.0 : (getAvg2() / (getAvg() * getAvg()) - 1.0);
    }

    String toCMVString() {
        return "Count: " + getCount()
                + String.format(", sum: %.2f", getTotal())
                + String.format(", avg: %.5f", getAvg())
                + String.format(", var: %.5f", getVar())
                + String.format(", scv: %.5f", getScv())
                + ", empEn: " + getEmptyEntryCnt();
    }

    String toCMVStringShort() {
        return "cnt:" + getCount()
                + String.format(",avg:%.2f", getAvg())
                + String.format(",scv:%.2f", getScv());
    }
}
