import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
Author: Soowhan Park
Email: sp6682@nyu.edu
Title: Page Rank Algorithm Simulation
File: PageRankMapper.java
*/

// The major portion of the code is about parsing the input and reformat it to the output
// Input: <LongWritable, Text> to read .txt file
// Output: <Text, Text> for string format which will be parsed again in reducer
public class PageRankMapper extends Mapper<LongWritable, Text, Text, Text> { 
    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        String line = value.toString(); // read input line
        String[] line_split_lst = line.split(" "); // split by empty spaces and store into array
        String page = line_split_lst[0]; // first index of array is always a page
        float pr = Float.parseFloat(line_split_lst[line_split_lst.length - 1]); // last index of array is always page rank value
        String[] outlinks = new String[line_split_lst.length - 2]; // initialize a new array that only inlcludes outlinks from page

        // store only outlinks and delete page and pr from the input list
        for (int i = 1; i < outlinks.length + 1; i++)
            outlinks[i -1] = line_split_lst[i];

        int n_outlinks = outlinks.length; // number of outlinks will be used as a denominator
        String val = page + "," + (pr/n_outlinks); // value is formatted as "page + , + page rank value". Ex) "A, 0.166667/2"
        String original_outlinks = ""; // store the outlinks same as the input. Ex) "C J", or "A B". 
        
        // update the outlinks initailzed above
        for (int i = 0; i < n_outlinks; i++){
            context.write(new Text(outlinks[i]), new Text(val));
            if (i != n_outlinks - 1){   // if not located at the last index, include white space to be used as a delimeter
                original_outlinks += (outlinks[i] + " ");
            }else{  // if located at the last index, ignore white space
                original_outlinks += outlinks[i];
            }
        }
        // output the <Key(Text), Value(Text)> pairs to reducer
        context.write(new Text(page), new Text(original_outlinks));
    }
}