import java.io.*;
import java.util.*;

public class FormatNGramTemplate {

    public static void main(String[] args) {
        // Specify the input file name
        String inputFileName = "ass3inputtemp.txt"; // Replace with your actual file name

        // Read the file and process each line
        try (BufferedReader br = new BufferedReader(new FileReader(inputFileName))) {
            String line;
            while ((line = br.readLine()) != null) {
                processLine(line);
            }
        } catch (IOException e) {
            System.err.println("Error reading the file: " + e.getMessage());
        }
    }

    // Method to process each line of the file
    private static void processLine(String input) {
        // Split the input line by tab characters
        String[] parts = input.split("<tab>");

        // Extract components from the split parts
        String headWord = parts[0];
        String syntacticNgram = parts[1];
        String totalCount = parts[2];

        // Split syntactic ngram into tokens
        String[] tokens = syntacticNgram.split(" ");

        // Maps to hold the relationship between words
        Map<String, String> wordToHead = new HashMap<>();
        Map<String, String> wordToRel = new HashMap<>();

        Stemmer stemmer = new Stemmer();
        System.out.println(stemmer.toString()); // Output: run
        // Process each token and extract word/pos-tag/dep-label/head-index
        for (String token : tokens) {
            // Split the token by '/' to get word, pos-tag, dep-label, head-index
            String[] tokenParts = token.split("/");
            if (tokenParts.length >= 4) {
                String word = tokenParts[0];

                stemmer.add(word.toCharArray(), word.length());
                stemmer.stem(); // Perform stemming

                // The result will be stored in the Stemmer's internal buffer
                word = new String(stemmer.getResultBuffer(), 0, stemmer.getResultLength());
                String depLabel = tokenParts[2];
                int headIndex = Integer.parseInt(tokenParts[3]);

                // Find the head word based on head-index
                String head = (headIndex == 0) ? "root" : tokens[headIndex - 1].split("/")[0];

                stemmer.add(head.toCharArray(), head.length());
                stemmer.stem(); // Perform stemming

                // The result will be stored in the Stemmer's internal buffer
                head = new String(stemmer.getResultBuffer(), 0, stemmer.getResultLength());

                // Store the word-to-head mapping and relationship
                wordToHead.put(word, head);
                wordToRel.put(word, depLabel);
            } else {
                System.err.println("Malformed token: " + token);
            }
        }

        // Print relationships between words along with the dependency labels and total count
        System.out.println("Total Count: " + totalCount);
        for (String word : wordToHead.keySet()) {
            String head = wordToHead.get(word);
            String relationship = wordToRel.get(word);
            System.out.println(word + " → " + head + " (" + relationship + ")");
        }
        System.out.println();  // For better separation between lines
    }
}
