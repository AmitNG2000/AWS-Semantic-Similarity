<!-- Markdown file -->
<!-- In VS code, use ctrl + shift + v to see preview -->
<!-- In IntelliJ, Click the "Preview" icon (top-right) or use Ctrl/Cmd + Shift + A and search for "Markdown Preview." -->

<br/>

# README ASSIGNMENT 3
## Authors

Naveh Vaz Dias Hadas:  209169424 </br>
Amit Ner-Gaon: 211649801

## Intro
This project is the third assignment in the *Distributed System Programming: Scale Out with Cloud Computing and Map-Reduce* course at Ben-Gurion University in 2025. Assignment instructions can be found in [assignment3.pdf](resources/assignment3.pdf).
This project focuses on *semantic similarity classification* using *MapReduce* and *Machine Learning*.
The project is based on the paper [Comparing Measures of Semantic Similarity](https://ieeexplore.ieee.org/document/4588492). We modify the algorithm and use the [Google Syntactic N-Grams](https://commondatastorage.googleapis.com/books/syntactic-ngrams/index.html) as the corpus.
Before processing, we use a [Porter Stemmer](https://vijinimallawaarachchi.com/2017/05/09/porter-stemming-algorithm/) to obtain the lexeme of each word. We define a feature as a pair consisting of a lexeme and a dependency label. For each lexeme, the system builds a representative vector where each entry represents the count of a specific feature.
Then, for each pair of lexemes, the system constructs a 24-dimensional vector representing the distance between the lexemes' vectors, evaluated using four measures of association with context and six measures of vector similarity.
Finally, we use [WEKA](https://ml.cms.waikato.ac.nz/weka/) to train a classifier and evaluate the system's accuracy, referring to [word-relatedness.txt](resources/word-relatedness.txt) as ground truth.

## How to run
- Configure your AWS credentials.  
- Create a bucket with `App.bucketname` and upload the steps' JAR files to `bucket/jars/`.  
- In the S3 bucket, delete the `log/` and `outputs/` folders if they exist.  
- Upload `word-relatedness.txt` to the S3 bucket. If an example corpus is needed, upload `s3inputtemp.txt` to S3.
- Run `App`

## Extra Info
- To read the output file directly from an S3 bucket without downloading it to your local system, you can use the following command:

```bs
aws s3 cp s3://bucketassignment3/output_step1/part-r-00000 - | cat
```

## System Architecture

### Terminology

* **Feature:** a pair consisting of a lexeme and a dependency label.

#### Counts:
* **count(F):** The total number of features.
* **count(F = f):** The number of occurrences of a specific feature *f*.
* **count(L):** The total number of lexemes.
* **count(L = l):** The number of occurrences of a specific lexeme *l*.
* **count(F = f, L = l):** The number of times the specific feature *f* appears with the specific lexeme *l*.



### Intro
The system consists of four parts:

1. **Step01, Step02 – Preprocessing:** Filter the relevant lexemes and features.
2. **Step1, Step2 – Corpus Statistics:** Calculate `count(F = f)`, `count(L = l)`, and `count(F = f, L = l)`.
3. **Step3, Step4 – Algorithm Calculation:** Measure association with context and compute vector similarity.
4. **Step4 – Assessment:** Evaluate the model's accuracy.


### Steps
* **Step 01**: create a `LexemeSet` with the all lexemes in `word-relatedness.txt`.
* **Step 02**: create a `DepLabelSet` with the all dependencies label in the `corpus`.
* **Step 1**: calculates count(F=f) and count(L=l) at the `corpus`. Used for creating `lexemeFeatureToCountMap`. Output: (Text feature/lexeme, LongWritable quantity).
* **Step 2**: for each lexeme presented in both the `corpus` and `word-relatedness.txt`, calculates a vector of counts(F=f,L=l). The step uses `TreeMap` to create a lexicographically ordered map, ensuring a consistent structure for all lexeme vectors. Output: (Text lexeme, Text spaces_separated_counts(F=f, L=l))
* **Step 3**: measure association with the context and create four vectors, one for each association method. Output: (Text lexeme, Text v5:v6:v7:v8, vi is space separated vector).
* **Step 4**: using fuzzy join, for each lexemes pair, create a 24-dimensional vector that measures vector similarity (distance) using six distance measure methods. Output: (Text lexeme, Text paces_separated_vector)
* **Step 5:** (Not part of the MapReduce pattern) Convert the result to ARFF type and using Weka to assess the model's accuracy.

## Memory Assumptions
As instructed, we assume that the word pairs in the gold-standard dataset `word-relatedness.txt` can be stored in memory. This assumption was used in steps 1 and 2 to build the lexeme set and in step 3 to perform a mapper-side join with the data from step 1's output.  

## Input and Output Example

[Run Example on a Small Corpus](resources/InpuOutputExample)


## Results

### System's Results
[Step4's Output](resources/results)

### Evaluation

Using `WEKA` we trained a model on the `word-relatedness.txt` dataset. Then we used the tried model to evaluate the system's results.
 </br> **(insert the weka report)**

### Reports
**(insert 10% and 100% reports)**


## Improvements suggestion
With the benefit of hindsight, we would like to suggest a few improvements to the system architecture:

* **Unify Step01, Step02, and Step1 into a single step.** The updated step will:
    1. **`Mapper.setup()`**: Create a set of all lexemes in `word-relatedness.txt`.
    2. **`Mapper.map()`**: Process the `corpus` as input and, for each lexeme present in the set from `setup()`, calculate `count(L = l)` and `count(F = f)`. Emit with a tag to indicate whether it is a lexeme or a feature.
    3. **`Reducer.reduce()`**: Sum up all counts and, using the tag, build two dictionaries, one for lexemes and one for features, mapping each lexeme/feature to its count.

* We believe that using a JSON format or another textual representation of a dictionary would be a good practice instead of manually implementing the parsing.  










