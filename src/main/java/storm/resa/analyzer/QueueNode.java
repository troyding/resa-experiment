package storm.resa.analyzer;

/**
 * Created by Tom.fu on 23/4/2014.
 */
public class QueueNode {

    enum ServiceType {Exponential, Deterministic, Uniform, Other};
    String getServiceTypeString() {

        switch (type) {
         case Exponential: return "Exp";
         case Deterministic: return "Det";
         case Uniform: return "Uni";
         default:
             return "Other";
        }
    }

    private double lambda = 0.0;
    private double mu = 0.0;
    private double pho = 0.0;

    private int serverCount = 0;
    private ServiceType type = ServiceType.Other;

    public QueueNode() {
        lambda = 0.0;
        mu = 0.0;
        pho = 0.0;
        serverCount = 0;
        type = ServiceType.Other;
    }

    public QueueNode(double _lambda, double _mu, double _pho, double ) {

    }
}
