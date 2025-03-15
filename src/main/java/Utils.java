import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
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
        Set<String> lexemeSet = new HashSet<>();

        // Retrieve file from S3
        S3Object s3object = s3Client.getObject(bucketName, fileName);
        S3ObjectInputStream inputStream = s3object.getObjectContent();
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

        String line;
        while ((line = reader.readLine()) != null) {
            String word = line.trim();
            if (!word.isEmpty()) { // Skip empty lines
                String lexeme = stemAndReturn(line.trim());
                lexemeSet.add(lexeme);
            }
        }

        // Manually close resources
        reader.close();
        inputStream.close();

        return lexemeSet;
    }


    /**
     * Creates a set from step0's output.
     * @return LexemeSet
     * @throws IOException
     */
    public static Set<String> retrieveLexemeSet() throws IOException {
        String bucketName = App.bucketName;  // Use correct bucket name
        String fileName = "outputs/output_step0"; // Correct path inside the bucket
        return retrieveSetFromFile(bucketName, fileName);
    }
}
