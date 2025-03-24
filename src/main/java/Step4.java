import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.*;

/**
 * For each pair, build a 24-dimensions vector.
 * Each entry denotes the similarity score for these two words according to one combination of association with context and vector similarity measures (4X6).
 * @Input step3's output
 * Output: (Text lexeme, Text space_separated_vector)
 */
public class Step4 {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

        private Set<String> lexemeSet = new HashSet<>();

        @Override
        protected void setup(Context context) throws IOException {

            lexemeSet = Utils.retrieveLexemeSet();
        } //end of mapper.setup

        @Override
        public void map(LongWritable line_id, Text line, Context context) throws IOException, InterruptedException {
            // line format: lexeme <tab> v5:v6:v7:v8
            // vi is space-separated vector

            String[] lineParts = line.toString().split("\t");
            String lexeme1 = lineParts[0];
            String vectors = lineParts[1];

            for (String lexeme2 : lexemeSet) {
                // Use compareTo for lexicographic comparison
                if (lexeme1.compareTo(lexeme2) < 0) {
                    // lexeme1 comes before lexeme2 lexicographically
                    context.write(new Text(lexeme1 + " " + lexeme2), new Text(vectors));
                } else {
                    // lexeme2 comes before lexeme1 lexicographically
                    context.write(new Text(lexeme2 + " " + lexeme1), new Text(vectors));
                }
            }
        } //end map()
    }


    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text lexemes, Iterable<Text> unitedVectors, Context context) throws IOException, InterruptedException, IllegalArgumentException {
            // Input format: lexeme1 lexeme2 <TAB> v5:v6:v7:v8
            // Expecting 2 values per key

            double[][] lexeme1_vectors = new double[4][];
            double[][] lexeme2_vectors = new double[4][];

            int i = 0;
            for (Text unitedVector : unitedVectors) {
                String[] vectors = unitedVector.toString().split(":");
                // vectors = v5:v6:v7:v8  vi is a space separated vector
                if (vectors.length != 4) {
                    throw new IOException("For " + lexemes.toString() + "Expected 4 vectors (v5:v6:v7:v8), but got " + vectors.length);
                }


                if (i == 0) {
                    for (int j=0; j<4; j++){
                        lexeme1_vectors[j] = Arrays.stream(vectors[j].split(" ")).mapToDouble(Double::parseDouble).toArray();
                    }
                }

                if (i == 1) {
                    for (int j=0; j<4; j++){
                        lexeme2_vectors[j] = Arrays.stream(vectors[j].split(" ")).mapToDouble(Double::parseDouble).toArray();
                    }
                }

                i++;
            }
            // compute vector similarity measures with 6 methods.

            if (i!=2) {
                // throw new IllegalArgumentException("Expected 2 vectors for " + lexemes.toString() + " unitedVectors: " + unitedVectors.toString());
                return; // One of the lexeme is not in the corpus but is presented because it is in the word-relatedness.txt.
            }



            // Compute results and store them in result_24_vector
            // The 24_vector format is that for each measure of association have six continuous entries in the vector representing the different measures of vector similarity.
            // The orders of the measures is by the order of the artical.
            List<Double> result_24_vector = new ArrayList<>();
            for (int j = 0; j < 4; j++) {
                double[] vec1 = lexeme1_vectors[j];
                double[] vec2 = lexeme2_vectors[j];

                if (vec1.length == 0 || vec2.length == 0) {
                    throw new IllegalArgumentException("Vector length is 0. vec1 = " + vec1 + ", vec2 = " + vec2);
                }

                if (vec1.length != vec2.length) {
                    throw new IllegalArgumentException("Vector length mismatch: vec1 = " + vec1 + ", vec2 = " + vec2);
                }

                result_24_vector.add(dist_by_method_9(vec1, vec2));
                result_24_vector.add(dist_by_method_10(vec1, vec2));
                result_24_vector.add(dist_by_method_11(vec1, vec2));
                result_24_vector.add(dist_by_method_13(vec1, vec2));
                result_24_vector.add(dist_by_method_15(vec1, vec2));
                result_24_vector.add(dist_by_method_17(vec1, vec2));
            }

            String result_24_vector_str = result_24_vector.stream().map(String::valueOf).reduce((a, b) -> a + " " + b).orElse("");
            context.write(lexemes, new Text(result_24_vector_str));

        }//end reduce


        ///////// Measures of Vector Similarity /////////
        protected double dist_by_method_9(double[] vector1, double[] vector2) {
            double sum = 0.0;
                for (int i = 0; i < vector1.length; i++) {
                    sum += Math.abs(vector1[i] - vector2[i]);
                }
            return sum;
        }

        protected double dist_by_method_10(double[] vector1, double[] vector2) {
            double sum = 0.0;
            for (int i = 0; i < vector1.length; i++) {
                sum += Math.pow((vector1[i] - vector2[i]), 2);
            }
            return Math.sqrt(sum);
        }

        protected double dist_by_method_11(double[] vector1, double[] vector2) {
                double sumOfProducts = 0.0;
                double sumOfSquaredVector1 = 0.0;
                double sumOfSquaredVector2 = 0.0;
                for (int i = 0; i < vector1.length; i++) {
                    sumOfProducts += vector1[i] * vector2[i];
                    sumOfSquaredVector1 += Math.pow(vector1[i], 2);
                    sumOfSquaredVector2 += Math.pow(vector2[i], 2);
                }
            return (sumOfProducts / (Math.sqrt(sumOfSquaredVector1) * Math.sqrt(sumOfSquaredVector2)));
        }

        protected double dist_by_method_13(double[] vector1, double[] vector2) {
            double sumOfMin = 0.0;
            double sumOfMax = 0.0;
            for (int i = 0; i < vector1.length; i++) {
                sumOfMin += Math.min(vector1[i], vector2[i]);
                sumOfMax += Math.max(vector1[i], vector2[i]);
            }
            return sumOfMin / sumOfMax;
        }

        protected double dist_by_method_15(double[] vector1, double[] vector2) {
            double sumOfMin = 0.0;
            double sumOfCoordinate = 0.0;
            for (int i = 0; i < vector1.length; i++) {
                sumOfMin += Math.min(vector1[i], vector2[i]);
                sumOfCoordinate += vector1[i] + vector2[i];
            }
            return 2* sumOfMin / sumOfCoordinate;
        }

        protected double dist_by_method_17(double[] vector1, double[] vector2) {

            // Compute the Kullback-Leibler divergence

            double[] midpoint = new double[vector1.length];
            for (int i = 0; i < vector1.length; i++) {
                midpoint[i] = (vector1[i] + vector2[i]) / 2.0;
            }

            double kl1 = klDivergence(vector1, midpoint);
            double kl2 = klDivergence(vector2, midpoint);

            // Return the sum of the two KL divergences as the Jensen-Shannon divergence
            return kl1 + kl2;
        }

        // Calculate Kullback-Leibler Divergence
        private double klDivergence(double[] p, double[] q) {
            double klDiv = 0.0;
            for (int i = 0; i < p.length; i++) {
                if (p[i] > 0 && q[i] > 0) {  // Prevent log(0)
                    klDiv += p[i] * Math.log(p[i] / (q[i] + 1e-10));
                }
            }
            return klDiv;
        }
    } //end reducer class



    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 4 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();


        Job job = Job.getInstance(conf, "Step 4");
        job.setJarByClass(Step4.class);
        job.setMapperClass(MapperClass.class);
        // job.setCombinerClass(ReducerClass.class); //commoner don't fit here
        //job.setPartitionerClass(PartitionerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setInputFormatClass(TextInputFormat.class);

        FileInputFormat.addInputPath(job, new Path(String.format("%s/outputs/output_step3", App.s3Path)));
        FileOutputFormat.setOutputPath(job, new Path(String.format("%s/outputs/output_step4", App.s3Path)));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    } //end of main
}
