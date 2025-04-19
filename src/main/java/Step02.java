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


/**
 *  calculates count(F=f) and count(L=l) using dictionaries and emit as JSON
 * @pre word-relatedness.txt found in the S3 bucket
 * @Input word-relatedness.txt
 * @Output: a list off all lexeme in word-relatedness.txt
 */
public class Step02 {

    //public class Mapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable line_Id, Text line, Context context) throws IOException, InterruptedException {

            String[] fields = line.toString().split("\t"); // Tab-separated

            String syntacticNgram = fields[1].trim();

            // Process the entire syntactic N-Gram
            String[] tokens = syntacticNgram.split(" ");
            for (String token : tokens) {
                try {
                    // Token Format: cease/VB/ccomp/0
                    String[] tokenParts = token.split("/");

                    String depLabel = tokenParts[2].trim();
                    context.write(new Text(depLabel), new Text());

                } catch (Exception e) {
                    throw new IOException("Error processing token: " + token, e);
                }
            }
        }
    }


    //Class Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
    public static class ReducerClass extends Reducer<Text,Text,Text,Text> {

        @Override
        public void reduce(Text depLabel, Iterable<Text> counts, Context context) throws IOException,  InterruptedException {
            context.write(depLabel, new Text()); //don't care about the counts
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
        System.out.println("[DEBUG] STEP 02 started!");
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


        Job job = Job.getInstance(conf, "Step 02: creates depLabelSet");
        job.setJarByClass(Step02.class);
        job.setMapperClass(MapperClass.class);
        //job.setPartitionerClass(PartitionerClass.class);
        job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);


        //For demo testing
        //FileInputFormat.addInputPath(job, new Path(String.format("%s/ass3inputtemp.txt" , App.s3Path))); //TODO: un-comment for demo

        //Actual NGRAM
        // Load only files 0.txt to 9.txt from s3a://biarcs/ for testing
        for (int i = 0; i <= 9; i++) {
            FileInputFormat.addInputPath(job, new Path("s3a://biarcs/" + i + ".txt"));
        }

        FileOutputFormat.setOutputPath(job, new Path(String.format("%s/outputs/output_step02", App.s3Path)));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

