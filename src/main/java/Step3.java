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
 * measure association with the context and create four vectors, one for each association method.
 * @Input step2's output
 * Output: (Text lexeme, Text v1 v2 v3 v4)
 */
public class Step3 {


    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

        private Map<String, Long> lexemeFeatureToCount = new HashMap<>();
        private String[] vectorStretcherArr; //fetchers by lexicographic order.
        private long countL = 0; //total count of all lexemes
        private long countF = 0; //total count of all feature


        @Override
        protected void setup(Context context) throws IOException {

            lexemeFeatureToCount = Utils.retrievelexemeFeatureToCountMap();

            //creates a represention of the vectors format (all the fetchers in a lexicographic order)
            Set<String> lexemeSet = Utils.retrieveLexemeSet();
            Set<String> depLabelSet = Utils.retrieveDepLabelSet();

            // bulid vector structure
            List<String> vectorStretcherLst = new ArrayList<>(lexemeSet.size() * depLabelSet.size()); //memory assumption: the list can be stored in memory.
            for (String lexeme : lexemeSet) { //lexemes from the `word-relatedness.txt`
                for (String depLabel : depLabelSet) { //dependencies from the corpus
                    String fetcher = lexeme + "-" + depLabel;
                    if (lexemeFeatureToCount.containsKey(fetcher)) { //the fetcher is in the corpus.
                        vectorStretcherLst.add(fetcher);
                    }
                }
            }
            vectorStretcherLst.sort(String::compareTo); //fetchers by lexicographic order.
            vectorStretcherArr = vectorStretcherLst.toArray(new String[0]);

            //calculates countL and countF
            for (String key : lexemeFeatureToCount.keySet()) {
                if (lexemeSet.contains(key)) { // Correct condition
                    countL += lexemeFeatureToCount.get(key); // Correct Map lookup
                } else { //if no lexeme so this is a feature
                    countF += lexemeFeatureToCount.get(key);
                }
            }
        } //end of mapper.setup

        @Override
        public void map(LongWritable line_id, Text line, Context context) throws IOException, InterruptedException, IllegalArgumentException {
            //line format: lexeme    spaces_separated_counts(F=f, L=l)

            String[] LineFields = line.toString().split("\t"); // Tab-separated
            String lexeme = LineFields[0];
            String[] counts_fl_vector = LineFields[1].split(" "); //vector is space separated, the counts are matches to the fetchers by lexicographic order.


            ////////  Calculate the values of each vector by all the methods (5,6,7,8) ////////

            // init the vectors
            //Each vector os space separated, counts matches to the fetchers by lexicographic order.
            String[] v5 = Arrays.copyOf(counts_fl_vector, counts_fl_vector.length); //vector by method 5
            String[] v6 = Arrays.copyOf(counts_fl_vector, counts_fl_vector.length); //vector by method 6
            String[] v7 = Arrays.copyOf(counts_fl_vector, counts_fl_vector.length); //vector by method 7
            String[] v8 = Arrays.copyOf(counts_fl_vector, counts_fl_vector.length); //vector by method 8

            //method 5
            //no furthers steps are required

            //method 6
            for (int i = 0; i < v6.length; i++) {
                try {
                    if (v6[i].equals("0")) continue;
                    v6[i] = String.valueOf(Long.parseLong(v6[i]) / lexemeFeatureToCount.get(vectorStretcherArr[i]));
                    // the problem is what to do if i hava a feature that have a lexeme from the 'word-relatedness.txt' so it won't appear in the lexemeFeatureToCount.
                } catch (Exception e) {
                    throw new IllegalArgumentException("Error in line: " + line.toString(), e);
                }
            }

            // method 7 + 8
            double p_l_f;
            double p_l;
            double p_f;

            for (int i = 0; i < v7.length; i++) {  //at the start v7 and v8 are the same

                if (lexemeFeatureToCount.get(lexeme) == null) {
                    //throw new IllegalArgumentException("lexemeFeatureToCount.get(" + lexeme + ") = null");
                    return; //the lexeme is not in the corpus.
                }


                if (!v7[i].equals("0") || countF ==0 || countL==0) continue;

                p_l_f = Double.parseDouble(counts_fl_vector[i]) / countF;
                p_l = (double) lexemeFeatureToCount.get(lexeme) / countL;
                p_f = (double) lexemeFeatureToCount.get(vectorStretcherArr[i]) / countL;

                if (p_l_f==0 || p_l == 0 || p_f==0 ) continue;

                //method 7
                double value_by_7 = Math.log(p_l_f / (p_l * p_f)) / Math.log(2); //to get a log with base 2
                v7[i] = String.valueOf(value_by_7);

                //method 8
                double value_by_8 = (p_l_f - p_l * p_f) / Math.sqrt(p_l * p_f);
                v8[i] = String.valueOf(value_by_8);
            }


            // Join each vector's elements with space
            String v5Joined = String.join(" ", v5);
            String v6Joined = String.join(" ", v6);
            String v7Joined = String.join(" ", v7);
            String v8Joined = String.join(" ", v8);

            // Join all vectors with a tab between them
            String joinedVectors = v5Joined + ":" + v6Joined + ":" + v7Joined + ":" + v8Joined;
            //Output format: lexeme, v5:v6:v7:v8
            //vi is space_separated_vector
            context.write(new Text(lexeme), new Text(joinedVectors));


        }
    }


    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {

        @Override
        //fc: feature <space> count
        public void reduce(Text lexeme, Iterable<Text> vectors, Context context) throws IOException, InterruptedException {
            for (Text vector : vectors) {
                context.write(lexeme, vector);
                // return; //just one vector per lexeme
            }
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 3 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();


        Job job = Job.getInstance(conf, "Step 3");
        job.setJarByClass(Step3.class);
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

        FileInputFormat.addInputPath(job, new Path(String.format("%s/outputs/output_step2", App.s3Path)));
        FileOutputFormat.setOutputPath(job, new Path(String.format("%s/outputs/output_step3", App.s3Path)));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    } //end of main
}