import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Utils {

    //attributes
    private static final Stemmer stemmer = new Stemmer();
    private static final AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
            .withRegion("us-east-1")
            .build();

    //methods
    public static String stemAndReturn(String word) {
        stemmer.add(word.toCharArray(), word.length());
        stemmer.stem();
        return new String(stemmer.getResultBuffer(), 0, stemmer.getResultLength());
    }

    private static Set<String> retrieveSetFromFile(String bucketName, String fileName) throws IOException {
        Set<String> mySet = new HashSet<>();

        // Retrieve file from S3
        S3Object s3object = s3Client.getObject(bucketName, fileName);
        S3ObjectInputStream inputStream = s3object.getObjectContent();
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

        if (!reader.ready()) {
            throw new IOException("retrieveSetFromFile: S3 file is empty: " + fileName);
        }

        String line;
        while ((line = reader.readLine()) != null) {
            String word = line.split("\t")[0].trim();
            if (!word.isEmpty()) { // Skip empty lines
                String lexeme = stemAndReturn(line.trim());
                mySet.add(lexeme);
            }
        }

        // Manually close resources
        reader.close();
        inputStream.close();

        if (mySet.isEmpty()) {
            throw new IOException("retrieveSetFromFile:  Loaded mySet is empty.");
        }

        return mySet;
    }


    /**
     * Creates a set from step01's output.
     * @return LexemeSet
     * @throws IOException
     */
    public static Set<String> retrieveLexemeSet() throws IOException {

        String fileKey = "outputs/output_step01/part-r-00000";
        Set<String> lexemeSet = retrieveSetFromFile(App.bucketName, fileKey);
        System.out.println("[DEBUG] retrieveLexemeSet " + lexemeSet); //TODO: delete after demo
        return lexemeSet;
    }

    /**
     * Creates a set from step02's output.
     * @return depLabel set contain all the dependencies from the corpus.
     * @throws IOException
     */
    public static Set<String> retrieveDepLabelSet() throws IOException {

        String fileKey = "outputs/output_step02/part-r-00000";
        Set<String> depLabelSet = retrieveSetFromFile(App.bucketName, fileKey);
        System.out.println("[DEBUG] retrieveDepLabelSet " + depLabelSet); //TODO: delete after demo
        return depLabelSet;
    }

    /**
     *
     * @return lexemeFeatureToCountMap containing all the lexemes and feature in the corpus.
     * @throws IOException
     */
    public static Map<String, Long> retrievelexemeFeatureToCountMap() throws IOException{
        Map<String,Long> lexemeFeatureToCountMap = new HashMap<>();

        String fileKey = "outputs/output_step1/part-r-00000";

        // Retrieve file from S3
        S3Object s3object = s3Client.getObject(App.bucketName, fileKey);
        S3ObjectInputStream inputStream = s3object.getObjectContent();
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

        if (!reader.ready()) {
            throw new IOException("retrievelexemeFeatureToCountMap: S3 file is empty: " + fileKey);
        }

        String line;
        while ((line = reader.readLine()) != null) {
            String[] lineParts = line.trim().split("\t");
            String lexemeOrFeature = lineParts[0];
            Long count = Long.valueOf(lineParts[1]);
            lexemeFeatureToCountMap.put(lexemeOrFeature,count);
        }

        // Manually close resources
        reader.close();
        inputStream.close();

        if (lexemeFeatureToCountMap.isEmpty()) {
            throw new IOException("retrievelexemeFeatureToCountMap: Loaded map is empty.");
        }

        return lexemeFeatureToCountMap;
    }

}
