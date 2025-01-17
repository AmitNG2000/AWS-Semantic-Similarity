<!-- Markdown file -->
<!-- In VS code, use ctrl + shift + v to see preview -->
<!-- In IntelliJ, Click the "Preview" icon (top-right) or use Ctrl/Cmd + Shift + A and search for "Markdown Preview." -->

<br/>

# README ASSIGNMENT 3
## Information
Names:
עמית נר גאון - 211649801
<br/>
נווה וזדיאס הדס - 209169424

## How to run
- Add step1.jar to jars/
- Make sure you deleted the log/ and output_step1/ folders from s3 bucketassignment3
- if changed ass3inputtemp.txt dont forget to update it in s3
- Run Step (like job 2)
- 
## Extra info for now
- Our new bucket for this assignment is called "bucketassignment3"
- For now, we use <tab> as text and not \t in the input file because for some reason it doesn't want to work with \t from textfile
- Command to read the output file directly to the terminal without downloading it:
aws s3 cp s3://bucketassignment3/output_step1/part-r-00000 -
