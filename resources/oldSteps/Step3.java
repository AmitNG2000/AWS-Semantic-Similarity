package oldSteps;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.net.URI;

public class Step3 {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        //key = line id | value = w1 w2    dep-label-vector
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Input: word1,word2   vector
            String[] parts = value.toString().split("\\t");
            if (parts.length != 2) return;

            String wordPair = parts[0];
            String vector = parts[1];
            context.write(new Text(wordPair), new Text(vector));
        }
    }


    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        private double totalDependencies = 0.0;

        @Override
        protected void setup(Context context) throws IOException {
            // Load total dependencies (P(d)) from configuration or distributed cache
            totalDependencies = context.getConfiguration().getDouble("totalDependencies", 1.0);
        }

        // key = w1 w2 | values = dep-label-vector
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String vectorStr = null;

            for (Text value : values) {
                vectorStr = value.toString();
            } //TODO: ?

            if (vectorStr == null) return;

            // Parse the vector
            String[] vectorParts = vectorStr.trim().split(" ");
            int[] vector = new int[vectorParts.length];
            int totalCount = 0;
            for (int i = 0; i < vectorParts.length; i++) {
                vector[i] = Integer.parseInt(vectorParts[i].trim()); // Trim spaces
                totalCount += vector[i];
            }

            // Compute association measures
            double[] associationMeasures = computeAssociationMeasures(vector, totalCount);

            // Compute vector similarity measures
            double[] similarityMeasures = computeVectorSimilarities(vector);

            // Combine into a 24-dimensional feature vector
            StringBuilder featureVector = new StringBuilder();
            for (double a : associationMeasures) {
                for (double s : similarityMeasures) {
                    featureVector.append(a * s).append(",");
                }
            }

            // Emit the word pair and its 24-dimensional feature vector
            context.write(key, new Text(featureVector.substring(0, featureVector.length() - 1)));
        }

        private double[] computeAssociationMeasures(int[] vector, int totalCount) {
            double[] measures = new double[4];

            // Compute P(d|h)
            double[] p_d_given_h = new double[vector.length];
            for (int i = 0; i < vector.length; i++) {
                p_d_given_h[i] = (double) vector[i] / totalCount;
            }

            // Compute P(d) using totalDependencies
            double[] p_d = new double[vector.length];
            for (int i = 0; i < vector.length; i++) {
                p_d[i] = (double) vector[i] / totalDependencies;
            }

            // Compute MI, LLR, and PMI_norm
            for (int i = 0; i < vector.length; i++) {
                double pdh = p_d_given_h[i];
                double pd = p_d[i];
                if (pdh > 0 && pd > 0) {
                    measures[0] += pdh; // P(d|h)
                    measures[1] += Math.log(pdh / pd); // MI
                    measures[2] += 2 * vector[i] * Math.log(pdh / pd); // LLR
                    measures[3] += Math.log(pdh / pd) / -Math.log(pd); // PMI_norm
                }
            }
            return measures;
        }

        private double[] computeVectorSimilarities(int[] vector) {
            double[] measures = new double[6];
            double norm = 0.0;
            double dotProduct = 0.0;

            // Compute norms and dot product
            for (int val : vector) {
                norm += val * val;
                dotProduct += val * val;
            }
            norm = Math.sqrt(norm);

            // Cosine similarity
            measures[0] = dotProduct / (norm * norm);

            // L1 Norm
            measures[1] = 0.0;
            for (int val : vector) {
                measures[1] += Math.abs(val);
            }

            // L2 Norm
            measures[2] = norm;

            // Jaccard Similarity
            double minSum = 0.0, maxSum = 0.0;
            for (int val : vector) {
                minSum += Math.min(val, val);
                maxSum += Math.max(val, val);
            }
            measures[3] = minSum / maxSum;

            // Dice Coefficient
            measures[4] = (2 * minSum) / (2 * maxSum);

            // Jensen-Shannon Divergence
            double[] M = new double[vector.length];
            double jsd = 0.0;
            for (int i = 0; i < vector.length; i++) {
                M[i] = vector[i] / 2.0;
            }
            for (int i = 0; i < vector.length; i++) {
                if (vector[i] > 0) {
                    jsd += vector[i] * Math.log(vector[i] / M[i]);
                }
            }
            measures[5] = jsd;

            return measures;
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 3 started!");
        Configuration conf = new Configuration();

        // Set S3 as the default filesystem
        conf.set("fs.defaultFS", "s3a://bucketassignment3");
        conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        conf.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain");

        // Check and delete output directory if it exists
        FileSystem fs = FileSystem.get(new URI(String.format("%s/outputs/output_step3", App.s3Path)), conf);
        Path outputPath = new Path(String.format("%s/outputs/output_step3", App.s3Path));
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true); // Recursively delete the output directory
        }

        Job job = Job.getInstance(conf, "Step 3");
        job.setJarByClass(Step3.class);
        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(String.format("%s/outputs/output_step2", App.s3Path)));
        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
