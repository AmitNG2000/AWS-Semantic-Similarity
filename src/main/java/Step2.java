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
 *  Calculates (lexeme, count(F=f, L=l)) using dictionary and emi
 * @Input Google NGRAM
 * Output: (Text lexeme, Text spaces_separated_counts(F=f, L=l))
 */
public class Step2 {
    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {


        private Set<String> lexemeSet = new HashSet<>(); // Stores lexemes from Step 1
        private Set<String> depLableSet = new HashSet<>(); // Stores all seen features

        @Override
        protected void setup(Context context) throws IOException {

            lexemeSet = Utils.retrieveLexemeSet();
            depLableSet = Utils.retrieveDepLabelSet();
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            String[] fields = line.split("\t");

            String syntacticNgram = fields[1].trim();
            String totalCount = fields[2].trim();

            String[] tokens = syntacticNgram.split(" ");

            for (String token : tokens) {
                String[] tokenParts = token.split("/");
                String word = tokenParts[0].trim();
                String depLabel = tokenParts[2].trim();
                String lexeme = Utils.stemAndReturn(word);

                if (!lexemeSet.contains(lexeme)) continue;
                if (!depLableSet.contains(depLabel)) continue;

                String feature = lexeme + "-" + depLabel;
                context.write(new Text(lexeme), new Text(feature + " " + totalCount));
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Map<String, Long> lexemeFeatureToCount = Utils.retrievelexemeFeatureToCountMap();
            for (String lexeme : lexemeSet) {
                for (String depLabel : depLableSet) {
                    String feature = lexeme + "-" + depLabel;
                    // Write all possible feature combinations, using 0 for features not in corpus
                    context.write(new Text(lexeme), new Text(feature + " " + 
                        (lexemeFeatureToCount.containsKey(feature) ? lexemeFeatureToCount.get(feature) : "0"))); //Should always write 0?
                }
            }
        }
    } //end of mapper class


    /*
    public static class PartitionerClass extends Partitioner<Text, LongWritable> {
        @Override
        public int getPartition(Text key, LongWritable value, int numPartitions) {
            return key.hashCode() % numPartitions;
        }
    }
     */
    //Mapper Output format: (Text lexeme, Text feature<space><count>)
    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {

        @Override
        //fc: feature <space> count
        public void reduce(Text lexeme, Iterable<Text> fcCouples, Context context) throws IOException, InterruptedException {
            // Step 1: build a dictionary, (TreeMap to keep features sorted)
            Map<String, Long> featureCounts = new TreeMap<>();

            StringBuilder featureVector = new StringBuilder(); //can't be inside the for loop!

            for (Text fc : fcCouples) { // fc as feature counts

                String[] fcParts = fc.toString().split(" "); // value format: feature count
                // if (fcParts.length < 2) continue; // Ignore malformed keys //TODO: un-comment?
                String feature = fcParts[0];
                String count = fcParts[1];

                //Aggregate the Counts (L=l, F=f)
                featureCounts.put(feature, (featureCounts.getOrDefault(feature, 0L) + Long.parseLong(count)));
            }
            System.out.println("COUNT CHECK: " + featureCounts);
            System.err.println("COUNT CHECK: " + featureCounts);

            // From the dictionary builds a vector of the counts order by the lexicographic order of the feature's title.
            // since the map is a tree map we will get a natural order of the features. Insuring consist structure in all the lexemes.
            for (Map.Entry<String, Long> entry : featureCounts.entrySet()) {
                featureVector.append(entry.getValue()).append(" ");
            }

            // Emit: (Text lexeme, Text spaces_separated_counts(F=f, L=l))
            context.write(lexeme, new Text(featureVector.toString().trim()));
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 2 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();

        /*
        // Set S3 as the default filesystem
        conf.set("fs.defaultFS", App.s3Path);
        conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        conf.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain");

        // Check and delete output directory if it exists
        FileSystem fs = FileSystem.get(new URI(String.format("%s/outputs/output_step2", App.s3Path)), conf);
        Path outputPath = new Path(String.format("%s/outputs/output_step2", App.s3Path));
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true); // Recursively delete the output directory
        }
         */

        Job job = Job.getInstance(conf, "Step 2");
        job.setJarByClass(Step2.class);
        job.setMapperClass(MapperClass.class);
        // job.setCombinerClass(ReducerClass.class); //commoner don't fit here
        //job.setPartitionerClass(PartitionerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        //For demo testing input format
        job.setInputFormatClass(TextInputFormat.class);

        //For demo testing
        //FileInputFormat.addInputPath(job, new Path(String.format("%s/ass3inputtemp.txt" , App.s3Path))); //TODO: un-comment for demo

        //Actual NGRAM
        // Load only files 0.txt to 9.txt from s3a://biarcs/ for testing
        for (int i = 0; i <= 9; i++) {
            FileInputFormat.addInputPath(job, new Path("s3a://biarcs/" + i + ".txt"));
        }

        FileOutputFormat.setOutputPath(job, new Path(String.format("%s/outputs/output_step2", App.s3Path)));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}