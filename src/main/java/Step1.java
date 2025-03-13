import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.avro.file.FileReader;
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
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

/**
 *  calculates count(F=f) and count(L=l) using dictionaries and emit as JSON
 * @pre word-relatedness.txt found in the S3 bucket
 * @Input input from NGRAM as lines
 * @Output: (Text dict_name, Text dict.JSON)
 */
public class Step1 {

    private static String stemAndReturn(String word){

        Stemmer stemmer = new Stemmer();

        stemmer.add(word.toCharArray(), word.length());
        stemmer.stem();
        word = new String(stemmer.getResultBuffer(), 0, stemmer.getResultLength());
        return word;
    }


    //public class Mapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

        private Set<String> lexeme_set = new HashSet<>();
        Stemmer stemmer = new Stemmer();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            // Declare resources
            S3Object s3object = App.S3.getObject(App.bucketName, "word-relatedness.txt");
            S3ObjectInputStream inputStream = s3object.getObjectContent();
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

            String line;
            while ((line = reader.readLine()) != null) {
                String[] fields = line.split("\\s+"); // Split by any whitespace (TAB or SPACE)

                // Ensure the line has at least two words
                if (fields.length < 2) continue;
                String lexeme1 = stemAndReturn(fields[0].trim());
                String lexeme2 = stemAndReturn(fields[1].trim());

                if (!lexeme1.isEmpty()) lexeme_set.add(lexeme1);
                if (!lexeme2.isEmpty()) lexeme_set.add(lexeme2);
            }

            // Close resources
            reader.close();
            inputStream.close();

            //emit the lexeme_set
            context.write(new Text("lexeme_set"), new Text(lexeme_set.toString()));
        }

        @Override
        public void map(LongWritable line_Id, Text line, Context context) throws IOException, InterruptedException {
            String input = line.toString();
            String[] parts = input.split("\t | <tab>");

            //cease<tab>cease/VB/ccomp/0 for/IN/prep/1 an/DT/det/4 boys/NN/pobj/2<tab>56<tab>1834,2

            if (parts.length < 3) {
                System.err.println("Malformed line: " + input);
                return;
            }

            // Extract components from the split parts (There 4, the forth one we don't care)
            //String headWord = parts[0];
            String syntacticNgram = parts[1];
            String totalCount = parts[2];


            //TODO: check this part.

            // Split the input sentence into tokens (word/POS/dependency/index)
            String[] tokens = syntacticNgram.split("\\s+");  // Split by whitespace (space or tab)

            // We will store words by their indices and dependencies
            String[] words = new String[tokens.length];
            String[] dependencies = new String[tokens.length];
            int[] indices = new int[tokens.length];

            // Process each token to extract the word, dependency, and index
            for (int i = 0; i < tokens.length; i++) {
                String[] staticParts = tokens[i].split("/");  // Split each token into word/POS/dependency/index

                if (parts.length == 4) {
                    words[i] = staticParts[0];  // Word (e.g., "cease")
                    dependencies[i] = staticParts[2];  // Dependency (e.g., "ccomp", "prep", "det", "pobj")
                    indices[i] = Integer.parseInt(staticParts[3]);  // Index (e.g., "0", "1", "4", "2")
                }
            }

            // Now pair each word with the next word based on the index
            for (int i = 0; i < tokens.length - 1; i++) {
                String word1 = words[i];  // Current word
                String word2 = words[i + 1];  // Next word
                String dep2 = dependencies[i + 1];  // Dependency of next word

                // Emit (word1, word2-dependency)
                context.write(new Text(stemAndReturn(word1)), new Text(stemAndReturn(word2) + "-" + dep2 + " " + String.valueOf(totalCount)));
            }
        }
    }


    //Class Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
    public static class ReducerClass extends Reducer<Text,Text,Text,Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            long sum = 0;
            for (LongWritable value : values) {
                sum += value.get();
            }
            context.write(key, new LongWritable(sum));
        }
    }

    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return key.hashCode() % numPartitions;
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
        Job job = Job.getInstance(conf, "Step 1");
        job.setJarByClass(Step1.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setCombinerClass(ReducerClass.class);
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
        FileInputFormat.addInputPath(job, new Path(String.format("%s/ass3inputtemp.txt" , App.s3Path)));
        FileOutputFormat.setOutputPath(job, new Path(String.format("%s/outputs/output_step1", App.s3Path)));

        //FROM NGRAM INPUT\OUTPUT
        //FileInputFormat.addInputPath(job, new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data")); #TODO change this to relevant corpus
        //FileOutputFormat.setOutputPath(job, new Path(String.format("%s/outputs/output_step1_word_count" , App.s3Path)));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
