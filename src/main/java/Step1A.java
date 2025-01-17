import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Step1A: Extract Unique Dependency Types
 *
 * @Input: Step1 output - lines of the format: "word1 word2 dependency count"
 * @Output: Unique dependency types
 */
public class Step1A {
    // Mapper Class
    public static class MapperClass extends Mapper<Object, Text, Text, NullWritable> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Split the input line
            String[] fields = value.toString().split("\\s+");
            if (fields.length == 4) {
                String dependency = fields[2]; // Extract the dependency type
                context.write(new Text(dependency), NullWritable.get()); // Emit dependency type as key
            }
        }
    }

    // Reducer Class
    public static class ReducerClass extends Reducer<Text, NullWritable, Text, NullWritable> {
        @Override
        public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            // Emit unique dependency types
            context.write(key, NullWritable.get());
        }
    }

    // Driver
    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 1A started!");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step 1A");

        job.setJarByClass(Step1A.class);
        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // Input: Step1 output
        FileInputFormat.addInputPath(job, new Path(String.format("%s/outputs/output_step1", App.s3Path)));

        // Output: Unique dependency types
        FileOutputFormat.setOutputPath(job, new Path(String.format("%s/outputs/output_step1A", App.s3Path)));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
