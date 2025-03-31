import java.io.*;
import java.util.*;

public class Step5ConvertToARFF {

    public static String stem(String word) {
        return Utils.stemAndReturn(word);
    }

    public static void main(String[] args) throws IOException {
        String step4OutputDir = "resources/results";
        String goldStandardPath = "resources/word-relatedness.txt";
        String arffOutputPath = "semantic_similarity.arff";

        // Load gold standard as a HashMap
        Map<String, Boolean> goldStandard = new HashMap<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(goldStandardPath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\t");
                if (parts.length >= 3) {
                    String word1 = stem(parts[0].trim());
                    String word2 = stem(parts[1].trim());
                    Boolean isRelated = Boolean.parseBoolean(parts[2].trim());

                    //consist structure
                    if (word1.compareTo(word2) > 0) {
                        String tmp = word1;
                        word1 = word2;
                        word2 = tmp;
                    }

                    goldStandard.put(word1 + "\t" + word2, isRelated);
                }
            }
        }

        // Write ARFF
        try (PrintWriter writer = new PrintWriter(new FileWriter(arffOutputPath))) {
            writer.println("@RELATION word_relatedness\n");

            for (int i = 1; i <= 24; i++) {
                writer.println("@ATTRIBUTE measure_" + i + " NUMERIC");
            }
            writer.println("@ATTRIBUTE class {TRUE,FALSE}\n");
            writer.println("@DATA");

            File dir = new File(step4OutputDir);
            File[] partFiles = dir.listFiles((d, name) -> name.startsWith("part-r-"));
            if (partFiles == null) return;

            for (File partFile : partFiles) {
                try (BufferedReader reader = new BufferedReader(new FileReader(partFile))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        String[] parts = line.split("\t");
                        if (parts.length < 2) continue;

                        String[] words = parts[0].split(" ");
                        if (words.length != 2) continue;

                        String word1 = stem(words[0].trim());
                        String word2 = stem(words[1].trim());

                        if (word1.compareTo(word2) > 0) {
                            String tmp = word1;
                            word1 = word2;
                            word2 = tmp;
                        }

                        String key = word1 + "\t" + word2;
                        Boolean label = goldStandard.get(key);
                        if (label == null) continue;

                        String[] features = parts[1].trim().split("\\s+");
                        if (features.length != 24) continue;

                        StringBuilder row = new StringBuilder();
                        for (int i = 0; i < 24; i++) {
                            if (i > 0) row.append(",");
                            row.append(features[i]);
                        }
                        row.append(",").append(label ? "TRUE" : "FALSE");

                        writer.println(row);
                    }
                }
            }
        }

        System.out.println("ARFF file created successfully: " + arffOutputPath);
    }
}
