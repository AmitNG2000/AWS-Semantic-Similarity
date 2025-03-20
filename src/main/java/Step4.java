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
 * For each pair, build a 24-dimensions vector.
 * Each entry denotes the similarity score for these two words according to one combination of association with context and vector similarity measures (4X6).
 * @Input step3's output
 * Output: (Text lexeme, Text space_separated_vector)
 */
public class Step4 {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

        private Set<String> lexemeSet = new HashSet<>();

        @Override
        protected void setup(Context context) throws IOException {

            lexemeSet = Utils.retrieveLexemeSet();
        } //end of mapper.setup

        @Override
        public void map(LongWritable line_id, Text line, Context context) throws IOException, InterruptedException {
            // line format: lexeme <tab> v5:v6:v7:v8
            // vi is space-separated vector

            String[] lineParts = line.toString().split("\t");
            String lexeme1 = lineParts[0];
            String vectors = lineParts[1];

            for (String lexeme2 : lexemeSet) {
                // Use compareTo for lexicographic comparison
                if (lexeme1.compareTo(lexeme2) < 0) {
                    // lexeme1 comes before lexeme2 lexicographically
                    context.write(new Text(lexeme1 + " " + lexeme2), new Text(vectors));
                } else {
                    // lexeme2 comes before lexeme1 lexicographically
                    context.write(new Text(lexeme2 + " " + lexeme1), new Text(vectors));
                }
            }
        } //end map()
    }


    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text lexemes, Iterable<Text> unitedVectors, Context context) throws IOException, InterruptedException {
            //input format: lexeme1 lexeme2, v5:v6:v7:v8
            //we expect 2 values per key
            //String[lexeme][vi][entry]

            String[][][] vectors = new String[2][4][]

            for (Text uniteVector : unitedVectors) {
                Long[] v5 = null;
            }

        } //end reduce()
    } //end reducer class

    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 4 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();


        Job job = Job.getInstance(conf, "Step 4");
        job.setJarByClass(Step3.class);
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

        FileOutputFormat.setOutputPath(job, new Path(String.format("%s/outputs/output_step4", App.s3Path)));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    } //end of main
}
