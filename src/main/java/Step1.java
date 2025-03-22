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
import java.util.HashSet;
import java.util.Set;

/**
 *  calculates count(F=f) and count(L=l) using dictionaries and emit as JSON
 * @pre word-relatedness.txt found in the S3 bucket
 * @Input input from NGRAM as lines
 * @Output: (Text dict_name, Text dict.JSON)
 */
public class Step1 {

    //public class Mapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
    public static class MapperClass extends Mapper<LongWritable, Text, Text, LongWritable> {

        private Set<String> lexemeSet = new HashSet<>();

        @Override
        protected void setup(Context context) throws IOException {
            lexemeSet = Utils.retrieveLexemeSet();
            System.out.println(lexemeSet);
        }

        @Override
        public void map(LongWritable line_Id, Text line, Context context) throws IOException, InterruptedException {
            String line_str = line.toString();
            System.out.println("[DEBUG] Line" + line_str);
            String[] parts = line_str.split("\t");

            //cease<tab>cease/VB/ccomp/0 for/IN/prep/1 an/DT/det/4 boys/NN/pobj/2<tab>56<tab>1834,2

            /*
            if (parts.length < 3) {
                System.err.println("Malformed line: " + line_str);
                return;
            }

             */

            String syntacticNgram = parts[1];
            LongWritable totalCount = new LongWritable(Long.parseLong(parts[2]));


            // Example of a syntactic-ngram:     cease/VB/ccomp/0  for/IN/prep/1  some/DT/det/4  time/NN/pobj/2 (word/POS/dependency/index)
            String[] tokens = syntacticNgram.split(" ");  // Split by whitespace

            for (String token : tokens) {
                String[] tokenParts = token.split("/");
                //if (tokenParts.length < 3) continue; //TODO: un-comment?
                String lexeme = Utils.stemAndReturn(tokenParts[0]);
                if (!lexemeSet.contains(lexeme)) continue;
                context.write(new Text(lexeme), totalCount);
                String depLabel = tokenParts[2];
                String feature = lexeme + "-" + depLabel;
                context.write(new Text(feature), totalCount);
            }
        }
    }


    //Class Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
    public static class ReducerClass extends Reducer<Text,LongWritable,Text,LongWritable> {

        @Override
        public void reduce(Text key, Iterable<LongWritable> counts, Context context) throws IOException,  InterruptedException {
            //key is lexeme or feature
            long sum = 0;
            for (LongWritable count : counts) {
                sum += count.get();
            }
            context.write(key, new LongWritable(sum));
        }
    }

    /*
    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return key.hashCode() % numPartitions;
        }
    }
     */

    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 1 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();

        /*
        // Set S3 as the default filesystem
        conf.set("fs.defaultFS", App.s3Path);
        conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        conf.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain");

        // Check and delete output directory if it exists
        FileSystem fs = FileSystem.get(new URI(String.format("%s/outputs/output_step1", App.s3Path)), conf);
        Path outputPath = new Path(String.format("%s/outputs/output_step1", App.s3Path));
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true); // Recursively delete the output directory
        }
        */


        Job job = Job.getInstance(conf, "Step 1: calculates count(F=f) and count(L=l)");
        job.setJarByClass(Step1.class);
        job.setMapperClass(MapperClass.class);
        //job.setPartitionerClass(PartitionerClass.class);
        job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(TextInputFormat.class);

        //For demo testing
        FileInputFormat.addInputPath(job, new Path(String.format("%s/ass3inputtemp.txt" , App.s3Path))); //TODO: un-comment for demo

        //Actual NGRAM input
        // FileInputFormat.addInputPath(job, new Path("s3a://biarcs/")); // Reads all N-Gram files from S3 //TODO: comment for demo
        FileOutputFormat.setOutputPath(job, new Path(String.format("%s/outputs/output_step1", App.s3Path)));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
