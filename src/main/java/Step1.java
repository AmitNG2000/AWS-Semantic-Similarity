import org.apache.hadoop.conf.Configuration;
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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


/**
 *  Calculate the number single (w1), pairs (w1,w2) and trio (w1,w2,w3) in the corpus.
 * @pre for demo, the inoutfile is at S3 with App.<bucketName>
 * @Input split from a text file
 * @Output: ((w1), <LongWritable>), ((w1,w2), <LongWritable>), ((w1,w2,w3), <LongWritable>)
 */
public class Step1 {
    //public class Mapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
    public static class MapperClass extends Mapper<LongWritable, Text, Text, LongWritable> {
        private final static LongWritable countOutput = new LongWritable(1);
        private final Text wordsOutput = new Text(); // Reuse output Text object

        @Override
        public void map(LongWritable key, Text sentence, Context context) throws IOException, InterruptedException {
            String input = sentence.toString(); // Convert the input line to a string

            // Split the input line by tab characters
            String[] parts = input.split("<tab>");

            // Ensure the line is well-formed
            if (parts.length < 3) {
                System.err.println("Malformed line: " + input);
                //return;
            }

            // Extract components from the split parts
            String headWord = parts[0];
            String syntacticNgram = parts[1];
            String totalCount = parts[2];
            countOutput.set(Long.parseLong(totalCount));

            // Split syntactic ngram into tokens
            String[] tokens = syntacticNgram.split(" ");

            // Maps to hold the relationship between words
            Map<String, String> wordToHead = new HashMap<>();
            Map<String, String> wordToRel = new HashMap<>();

            Stemmer stemmer = new Stemmer();

            // Process each token and extract word/pos-tag/dep-label/head-index
            for (String token : tokens) {
                String[] tokenParts = token.split("/");
                if (tokenParts.length >= 4) {
                    String word = tokenParts[0];

                    // Perform stemming
                    stemmer.add(word.toCharArray(), word.length());
                    stemmer.stem();
                    word = new String(stemmer.getResultBuffer(), 0, stemmer.getResultLength());

                    String depLabel = tokenParts[2];
                    int headIndex = Integer.parseInt(tokenParts[3]);

                    // Find the head word based on head-index
                    String head = (headIndex == 0) ? "root" : tokens[headIndex - 1].split("/")[0];

                    // Perform stemming on the head word
                    stemmer.add(head.toCharArray(), head.length());
                    stemmer.stem();
                    head = new String(stemmer.getResultBuffer(), 0, stemmer.getResultLength());

                    // Store the word-to-head mapping and relationship
                    wordToHead.put(word, head);
                    wordToRel.put(word, depLabel);
                } else {
                    System.err.println("Malformed token: " + token);
                }
            }

            // Emit relationships between words along with the dependency labels
            for (String word : wordToHead.keySet()) {
                String head = wordToHead.get(word);
                String relationship = wordToRel.get(word);

                // Emit a key-value pair in the format: "word → head (relationship)" and count
                wordsOutput.set(word + " " + head + " " + relationship + " " + totalCount);
                context.write(wordsOutput, countOutput);
            }
        }
    }


    //Class Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
    public static class ReducerClass extends Reducer<Text,LongWritable,Text,LongWritable> {
        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException,  InterruptedException {
            long sum = 0;
            for (LongWritable value : values) {
                sum += value.get();
            }
            context.write(key, new LongWritable(sum));
        }
    }

    public static class PartitionerClass extends Partitioner<Text, LongWritable> {
        @Override
        public int getPartition(Text key, LongWritable value, int numPartitions) {
            return key.hashCode() % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 1 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();
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
        FileOutputFormat.setOutputPath(job, new Path(String.format("%s/output_step1", App.s3Path)));

        //FROM NGRAM INPUT\OUTPUT
        //FileInputFormat.addInputPath(job, new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data")); #TODO change this, this is not relevant
        //FileOutputFormat.setOutputPath(job, new Path(String.format("%s/outputs/output_step1_word_count" , App.s3Path)));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
