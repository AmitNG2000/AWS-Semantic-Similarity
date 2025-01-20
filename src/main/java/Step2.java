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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.net.URI;
import java.util.*;

public class Step2 {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\\s+");
            if (fields.length == 4) {
                String wordPair = fields[0] + "," + fields[1]; // Combine word1 and word2
                String dependency = fields[2]; // Dependency label
                String count = fields[3]; // Dependency count

                // Emit word pair as key, dependency:count as value
                context.write(new Text(wordPair), new Text(dependency + ":" + count));
            }
        }
    }

    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            // Partition by the first word in the word pair
            String[] words = key.toString().split(",");
            return words[0].hashCode() % numPartitions;
        }
    }

    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        private List<String> dependencyTypes = new ArrayList<>();

        @Override
        protected void setup(Context context) throws IOException {
            // Load dependency types from the distributed cache
            Path[] paths = context.getLocalCacheFiles();
            for (Path path : paths) {
                try (BufferedReader reader = new BufferedReader(new FileReader(path.toString()))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        dependencyTypes.add(line.trim());
                    }
                }
            }
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Step 1: Initialize a vector with zeros
            int[] vector = new int[dependencyTypes.size()]; // Fixed-size vector based on dependency types

            // Step 2: Aggregate counts for each dependency type
            Map<String, Integer> dependencyCounts = new HashMap<>();
            for (Text value : values) {
                String[] depCount = value.toString().split(":");
                String dependency = depCount[0];
                int count = Integer.parseInt(depCount[1]);
                dependencyCounts.put(dependency, dependencyCounts.getOrDefault(dependency, 0) + count);
            }

            // Step 3: Fill the vector
            for (int i = 0; i < dependencyTypes.size(); i++) {
                String depType = dependencyTypes.get(i); // Dependency type for this position
                vector[i] = dependencyCounts.getOrDefault(depType, 0); // Fill with count or 0 if not present
            }

            StringBuilder result = new StringBuilder();
            for (int i = 0; i < vector.length; i++) {
                result.append(vector[i]);
                if (i < vector.length - 1) { // Only add a space if it's not the last element
                    result.append(" ");
                }
            }
            context.write(key, new Text(result.toString())); //key = w1 w2 dep-label
        }
    }


    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 2 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();

        // Set S3 as the default filesystem
        conf.set("fs.defaultFS", "s3a://bucketassignment3");
        conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        conf.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain");

        // Check and delete output directory if it exists
        FileSystem fs = FileSystem.get(new URI(String.format("%s/outputs/output_step2", App.s3Path)), conf);
        Path outputPath = new Path(String.format("%s/outputs/output_step2", App.s3Path));
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true); // Recursively delete the output directory
        }

        // Job setup
        Job job = Job.getInstance(conf, "Step 2");
        job.setJarByClass(Step2.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Add dependency types file to the distributed cache
        job.addCacheFile(new Path(String.format("%s/outputs/output_step1A/part-r-00000", App.s3Path)).toUri());

        // Input and output paths
        FileInputFormat.addInputPath(job, new Path(String.format("%s/outputs/output_step1", App.s3Path)));
        FileOutputFormat.setOutputPath(job, new Path(String.format("%s/outputs/output_step2", App.s3Path)));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
