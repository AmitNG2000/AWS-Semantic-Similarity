import org.apache.hadoop.io.Text;


public class equations {

    //L1 Norm (Manhattan Distance)
    public static double computeL1(int[] v1, int[] v2) {
        double sum = 0.0;
        for (int i = 0; i < v1.length; i++) {
            sum += Math.abs(v1[i] - v2[i]);
        }
        return sum;
    }

    //L2 Norm (Euclidean Distance)
    public static double computeL2(int[] v1, int[] v2) {
        double sum = 0.0;
        for (int i = 0; i < v1.length; i++) {
            sum += Math.pow(v1[i] - v2[i], 2);
        }
        return Math.sqrt(sum);
    }

    //Cosine Similarity
    public static double computeCosine(int[] v1, int[] v2) {
        double dotProduct = 0.0, normV1 = 0.0, normV2 = 0.0;
        for (int i = 0; i < v1.length; i++) {
            dotProduct += v1[i] * v2[i];
            normV1 += Math.pow(v1[i], 2);
            normV2 += Math.pow(v2[i], 2);
        }
        return dotProduct / (Math.sqrt(normV1) * Math.sqrt(normV2));
    }

    //Jaccard Similarity
    public static double computeJaccard(int[] v1, int[] v2) {
        double minSum = 0.0, maxSum = 0.0;
        for (int i = 0; i < v1.length; i++) {
            minSum += Math.min(v1[i], v2[i]);
            maxSum += Math.max(v1[i], v2[i]);
        }
        return minSum / maxSum;
    }

    //Dice Similarity
    public static double computeDice(int[] v1, int[] v2) {
        double minSum = 0.0, sumV1 = 0.0, sumV2 = 0.0;
        for (int i = 0; i < v1.length; i++) {
            minSum += Math.min(v1[i], v2[i]);
            sumV1 += v1[i];
            sumV2 += v2[i];
        }
        return 2 * minSum / (sumV1 + sumV2);
    }

    //Jensen-Shannon Divergence
    public static double computeJS(int[] v1, int[] v2) {
        double[] m = new double[v1.length];
        double sumV1 = 0.0, sumV2 = 0.0;

        // Compute M = (v1 + v2) / 2
        for (int i = 0; i < v1.length; i++) {
            sumV1 += v1[i];
            sumV2 += v2[i];
        }

        for (int i = 0; i < v1.length; i++) {
            m[i] = (v1[i] / sumV1 + v2[i] / sumV2) / 2.0;
        }

        // Compute KL divergence for both distributions
        return computeKL(v1, m, sumV1) + computeKL(v2, m, sumV2);
    }

    private static double computeKL(int[] v, double[] m, double sumV) {
        double kl = 0.0;
        for (int i = 0; i < v.length; i++) {
            double p = v[i] / sumV;
            if (p > 0) {
                kl += p * Math.log(p / m[i]);
            }
        }
        return kl;
    }
}
