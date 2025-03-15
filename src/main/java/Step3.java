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
 *   measure association with the context and create four vectors, one for each association method.
 * @Input step2's output
 * Output: (Text lexeme, Text v1 v2 v3 v4)
 */
public class Step3 {
    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {


        private Map<String, Long> lexemeFeatureToCount = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException {

            lexemeFeatureToCount = Utils.retrievelexemeFeatureToCountMap();
        }

        @Override
        public void map(LongWritable line_id, Text line, Context context) throws IOException, InterruptedException {
            //line format: lexeme    spaces_separated_counts(F=f, L=l)

            String[] LineFields = line.toString().split("\t"); // Tab-separated
            String lexeme = LineFields[0];
            String vector = LineFields[1];


            //context.write(new Text(lexeme), new Text(vectores);

        }


        public static class ReducerClass extends Reducer<Text, Text, Text, Text> {

            @Override
            //fc: feature <space> count
            public void reduce(Text lexeme, Iterable<Text> vectors, Context context) throws IOException, InterruptedException {
                for (Text vector : vectors) {
                    context.write(lexeme, vector);
                    return;
                }
            }
        }

        public static void main(String[] args) throws Exception {
            System.out.println("[DEBUG] STEP 2 started!");
            System.out.println(args.length > 0 ? args[0] : "no args");
            Configuration conf = new Configuration();


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
            FileInputFormat.addInputPath(job, new Path(String.format("%s/ass3inputtemp.txt", App.s3Path)));

            //Actual NGRAM
            //FileInputFormat.addInputPath(job, new Path("s3a://biarcs/")); // Reads all N-Gram files from S3
            //FileInputFormat.addInputPath(job, new Path(String.format("%s/outputs/output_step1", App.s3Path)));  // Add Step 1 input

            FileOutputFormat.setOutputPath(job, new Path(String.format("%s/outputs/output_step2", App.s3Path)));

            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
    }
}