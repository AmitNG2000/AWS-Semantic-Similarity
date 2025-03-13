import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.net.URI;
import java.util.TreeMap;


/**
 *  Calculates (lexeme, count(F=f, L=l)) using dictionary and emi
 * @Input input from NGRAM as lines and step1 output?
 * @Output: "(lexeme, count(F=f, L=l))" using dictionary
 */
public class Step2 {
    public class MapperClass extends Mapper<LongWritable, Text, Text, LongWritable> {


        private Set<String> lexemeSet = new HashSet<>(); // Stores lexemes from Step 1
        private Set<String> featureSet = new HashSet<>(); // Stores all seen features
        private final LongWritable countOutput = new LongWritable(0);
        private Stemmer stemmer = new Stemmer(); // Initialize Stemmer

        @Override
        protected void setup(Context context) throws IOException {
            // load lexeme_set from step1's output, build new empty feature_set
            BufferedReader reader = new BufferedReader(new FileReader("lexeme_set.txt")); //#TODO change to whatever the output name is? or its JSON?
            String line;
            while ((line = reader.readLine()) != null) {
                lexemeSet.add(line.trim()); // Store lexemes
            }
            reader.close();
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //  Read from NGRAM lines
            String line = value.toString().trim();
            String[] fields = line.split("\t"); // Tab-separated

            if (fields.length < 3) return; // Ensure correct format

            String headword = fields[0].trim();  // Extract headword (we dont care? its just first word)
            String syntacticNgram = fields[1].trim(); // Extract dependency structure
            String totalCount = fields[2].trim(); // Extract frequency count

            // Convert count
            try {
                countOutput.set(Long.parseLong(totalCount));
            } catch (NumberFormatException e) {
                System.err.println("[ERROR] Invalid count: " + totalCount);
                return;
            }

            // Process the entire syntactic N-Gram
            String[] tokens = syntacticNgram.split(" ");

            for (String token : tokens) {
                String[] tokenParts = token.split("/");

                if (tokenParts.length < 4) continue; // Skip malformed tokens

                String word = tokenParts[0].trim();
                String depLabel = tokenParts[2].trim();

                //stemming
                String lexeme = applyStemming(word);

                // only lexemes from the lexeme set?
                if (!lexemeSet.contains(lexeme)) continue;

                featureSet.add(depLabel); // Store features (for each feature related to the lexeme)
                context.write(new Text(lexeme + "-" + depLabel), countOutput); // Output: (Text lexeme, Text feature quantity)
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Ensure every lexeme has a full feature vector with zeros also
            for (String lexeme : lexemeSet) {
                for (String feature : featureSet) {
                    countOutput.set(0); // Emit missing features as 0
                    context.write(new Text(lexeme + " " + feature), countOutput); //#TODO Change this output syntax? not sure what to write here
                }
            }
        }

        // Apply stemming using Stemmer.java
        private String applyStemming(String word) {
            stemmer.add(word.toCharArray(), word.length());
            stemmer.stem();
            return new String(stemmer.getResultBuffer(), 0, stemmer.getResultLength());
        }
    }

    //aggerates the quantities
    public static class CombinerClass extends Reducer<Text, LongWritable, Text, LongWritable> { //#TODO I'm not sure i used the right parameters?

        private final LongWritable sumCount = new LongWritable();

        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;

            // Aggregate counts locally
            for (LongWritable value : values) {
                sum += value.get();
            }

            sumCount.set(sum);
            context.write(key, sumCount);
        }
    }

    public static class PartitionerClass extends Partitioner<Text, LongWritable> {
        @Override
        public int getPartition(Text key, LongWritable value, int numPartitions) {
            return key.hashCode() % numPartitions;
        }
    }

    public class ReducerClass extends Reducer<Text, LongWritable, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            // Step 1: build a dictionary, (TreeMap to keep features sorted i read)
            Map<String, Long> featureCounts = new TreeMap<>();

            for (LongWritable value : values) {
                String[] keyParts = key.toString().split("-"); // key format: lexeme-feature
                if (keyParts.length < 2) continue; // Ignore malformed keys

                String lexeme = keyParts[0];
                String feature = keyParts[1];

                //Aggregate the Counts (L=l, F=f)
                featureCounts.put(feature, featureCounts.getOrDefault(feature, 0L) + value.get());
            }

            // From the dictionary builds a vector of the quotieties order by the lexicographic order of the feature's title
            StringBuilder featureVector = new StringBuilder();
            for (Map.Entry<String, Long> entry : featureCounts.entrySet()) {
                featureVector.append(entry.getValue()).append(" ");
            }

            // Emit: (Text lexeme, Text spaces_separated_counts(F=f, L=l))
            context.write(new Text(key.toString().split("-")[0]), new Text(featureVector.toString().trim())); //#TODO Change syntax?
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 1 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();

        // Set S3 as the default filesystem
        conf.set("fs.defaultFS", "s3a://bucketassignment3");
        conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        conf.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain");

        // Check and delete output directory if it exists
        FileSystem fs = FileSystem.get(new URI(String.format("%s/outputs/output_step1", App.s3Path)), conf);
        Path outputPath = new Path(String.format("%s/outputs/output_step1", App.s3Path));
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true); // Recursively delete the output directory
        }
        Job job = Job.getInstance(conf, "Step 2");
        job.setJarByClass(Step2.class);
        job.setMapperClass(MapperClass.class);
        job.setCombinerClass(CombinerClass.class);  // Added?
        job.setPartitionerClass(PartitionerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        //FOR NGRAM INPUT
        //job.setInputFormatClass(SequenceFileInputFormat.class);

        //For demo testing input format
        job.setInputFormatClass(TextInputFormat.class);
        //For demo testing
        //FileInputFormat.addInputPath(job, new Path(String.format("%s/ass3inputtemp.txt" , App.s3Path)));

        //#TODO make it use step1 output also? im not sure how to do it
        //Actual NGRAM
        FileInputFormat.addInputPath(job, new Path("s3a://biarcs/")); // Reads all N-Gram files from S3
        FileOutputFormat.setOutputPath(job, new Path(String.format("%s/outputs/output_step2", App.s3Path)));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}