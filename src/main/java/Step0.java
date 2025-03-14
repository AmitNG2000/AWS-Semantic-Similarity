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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

import java.util.HashSet;
import java.util.Set;


/**
 *  calculates count(F=f) and count(L=l) using dictionaries and emit as JSON
 * @pre word-relatedness.txt found in the S3 bucket
 * @Input word-relatedness.txt
 * @Output: a list off all lexeme in word-relatedness.txt
 */
public class Step0 {

    //public class Mapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

        private final Stemmer stemmer = new Stemmer();

        private String stemAndReturn(String word){
            stemmer.add(word.toCharArray(), word.length());
            stemmer.stem();
            return new String(stemmer.getResultBuffer(), 0, stemmer.getResultLength());
        }

        @Override
        public void map(LongWritable line_Id, Text line, Context context) throws IOException, InterruptedException {

            String[] fields = line.toString().split("\\s+"); // Split by any whitespace (TAB or SPACE)

            if (fields.length < 2) return; // Skip malformed lines

            // Apply stemming and trim whitespace
            String lexeme1 = stemAndReturn(fields[0]).trim();
            String lexeme2 = stemAndReturn(fields[1]).trim();

            //emit the lexemes
            if (!lexeme1.isEmpty()){
                context.write(new Text(lexeme1), new Text());
            }
            if (!lexeme2.isEmpty()){
                context.write(new Text(lexeme2), new Text());
            }
        }
    }


    //Class Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
    public static class ReducerClass extends Reducer<Text,Text,Text,Text> {

        @Override
        public void reduce(Text lexeme, Iterable<Text> counts, Context context) throws IOException,  InterruptedException {
            context.write(lexeme, new Text()); //don't care about the counts
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
        System.out.println("[DEBUG] STEP 0 started!");
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


        Job job = Job.getInstance(conf, "Step 0: creates lexemeSet");
        job.setJarByClass(Step0.class);
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


        FileInputFormat.addInputPath(job, new Path(String.format("%s/word-relatedness.txt" , App.s3Path)));

        FileOutputFormat.setOutputPath(job, new Path(String.format("%s/outputs/output_step0", App.s3Path)));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

