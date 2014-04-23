package storm.resa.analyzer;

import storm.resa.util.UsefulMethod;

import java.util.Map;

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
    private double rho = 0.0;

    private int serverCount = 0;
    private ServiceType type = ServiceType.Other;

    private double i2oRatio = 0.0;

    public QueueNode() {
        lambda = 0.0;
        mu = 0.0;
        rho = 0.0;
        serverCount = 1;
        type = ServiceType.Other;
        i2oRatio = 0.0;
    }

    public QueueNode(double l, double m, int sc, ServiceType t, double ioR ) {
        lambda = l;
        mu = m;
        serverCount = Math.max(sc, 1);
        type = t;
        i2oRatio = ioR;
    }

    public double getLambda () {
        return this.lambda;
    }

    public double getMu () {
        return this.mu;
    }

    public int getServerCount (){
        return this.serverCount;
    }

    public double getRhoSingleServer () {
        return (mu == 0.0) ? 0.0 : lambda / mu;
    }

    public int getMinServerCount () {
        if (mu == 0.0){
            return Integer.MAX_VALUE;
        } else {
            return (int)(lambda / mu) + 1;
        }
    }

    public double getRho () {
        rho = (mu == 0.0 || serverCount == 0) ? 0.0 : lambda / (mu * (double)serverCount);
        return rho;
    }

    public double estErlangT() {
      if (isStable()) {
          double r = lambda / (mu * (double)serverCount);
          double kr = lambda / mu;

          double phi0_p1 = 1.0;
          for (int i = 1; i < serverCount; i ++) {
              double a = Math.pow(kr, i);
              double b = (double) UsefulMethod.factorial(i);
              phi0_p1 += (a / b);
          }

          double phi0_p2_nor = Math.pow(kr, serverCount);
          double phi0_p2_denor =  (1.0 - r) * (double)(UsefulMethod.factorial(serverCount));
          double phi0_p2 = phi0_p2_nor / phi0_p2_denor;

          double phi0 = 1.0 / (phi0_p1 + phi0_p2);

          double pWait = phi0_p2 * phi0;

          double estT = pWait * r / ((1.0-r) *lambda) + 1.0 / mu;

          return estT;

      } else {
          System.out.println("Service is not stable!!!");
          return Double.MAX_VALUE;
      }
    }

    public double estMM1T() {
        if (isStable()) {
            return 1.0 / (mu * (double)serverCount - lambda);
        } else {
            System.out.println("Service is not stable!!!");
            return Double.MAX_VALUE;
        }
    }

    public boolean isStable () {
        return lambda > 0.0 && mu > 0.0 && serverCount > 0 && getRho() < 1.0;
    }

    public String queueNodeKeyStats (boolean showMinRequirement) {
        if (showMinRequirement) {
            return  String.format("lambda: %.3f, mu: %.3f, rho: %.3f, k: %d, MinRequire: %d", lambda, mu, getRho(), serverCount, getMinServerCount());
        } else {
            return  String.format("lambda: %.3f, mu: %.3f, rho: %.3f, k: %d", lambda, mu, getRho(), serverCount);
        }
    }

    public static boolean isStable (double _lambda, double _mu, int _serverCount) {
        return _lambda > 0.0 && _mu > 0.0 && _serverCount > 0 && (_lambda / (_mu * (double)_serverCount)) < 1.0;
    }

    public static double estErlangT(double _lambda, double _mu, int _serverCount) {
        if (isStable(_lambda, _mu, _serverCount)) {
            double r = _lambda / (_mu * (double)_serverCount);
            double kr = _lambda / _mu;

            double phi0_p1 = 1.0;
            for (int i = 1; i < _serverCount; i ++) {
                double a = Math.pow(kr, i);
                double b = (double) UsefulMethod.factorial(i);
                phi0_p1 += (a / b);
            }

            double phi0_p2_nor = Math.pow(kr, _serverCount);
            double phi0_p2_denor =  (1.0 - r) * (double)(UsefulMethod.factorial(_serverCount));
            double phi0_p2 = phi0_p2_nor / phi0_p2_denor;

            double phi0 = 1.0 / (phi0_p1 + phi0_p2);

            double pWait = phi0_p2 * phi0;

            double estT = pWait * r / ((1.0-r)*_lambda) + 1.0 / _mu;

            return estT;

        } else {
            System.out.println("Service is not stable!!!");
            return Double.MAX_VALUE;
        }
    }
}
