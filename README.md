<!-- Markdown file -->
<!-- In VS code, use ctrl + shift + v to see preview -->
<!-- In IntelliJ, Click the "Preview" icon (top-right) or use Ctrl/Cmd + Shift + A and search for "Markdown Preview." -->

<br/>

# README ASSIGNMENT 3
## Credits

Naveh Vaz Dias Hadas:  209169424 </br>
Amit Ner-Gaon: 211649801

## Intro
This project focuses on *semantic similarity classification* using *MapReduce* and **machine learning**. Based on the paper *Comparing Measures of Semantic Similarity*, we modify the algorithm to process *Google Syntactic N-Grams* as corpus. The system builds 24-dimensional feature vectors for word pairs using syntactic dependencies and trains a classifier to distinguish similar from non-similar words. The model is evaluated using *WEKA* tool.

## How to run
- Configure your AWS credentials.  
- Create a bucket with `App.bucketname` and upload the steps' JAR files to `bucket/jars/`.  
- In the S3 bucket, delete the `log/` and `outputs/` folders if they exist.  
- Upload `gold-standard.txt` to the S3 bucket. Also, update `s3inputtemp.txt` in S3.
- Run `App`

## Extra Info
- Our new bucket for this assignment is called "bucketassignment3"
- To read the output file directly from an S3 bucket without downloading it to your local system, you can use the following command:

```java
aws s3 cp s3://bucketassignment3/output_step1/part-r-00000 - | cat
```

## Steps
* **Step 0**: create a list with the lexemes in the `word-relatedness.txt`.
* **Step 1**: calculates count(F=f) and count(L=l). Output: (Text feature/lexeme, LongWritable quantity).
* **Step 2**: calculates for each lexeme vector of counts(F=f,L=l). Output: (Text lexeme, Text spaces_separated_counts(F=f, L=l))
* **Step 3**: measure association with the context and create four vectors, one for each association method.
* **Step 4**: for each pair, create a 24-dimensional vector that measures vector similarity (distance) using six distance measure methods.
* **Step 5:** (Not part of the MapReduce pattern) Using Weka to assess the model's accuracy.

## Memory Assumptions
As instructed, we assume that the word pairs in the gold-standard dataset can be stored in memory. This assumption was used in steps 1 and 2 to build the lexeme set and in step 3 to perform a mapper-side join with the data from step 1's output.  

## Resources
[Comparing Measures of Semantic Similarity](https://ieeexplore.ieee.org/document/4588492) </br>
[Google Syntactic N-Grams](https://commondatastorage.googleapis.com/books/syntactic-ngrams/index.html)  </br>
[WEKA](https://ml.cms.waikato.ac.nz/weka/)
[Porter Stemmer](https://vijinimallawaarachchi.com/2017/05/09/porter-stemming-algorithm/)










