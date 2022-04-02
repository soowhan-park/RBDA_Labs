import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
Author: Soowhan Park
Email: sp6682@nyu.edu
Title: Page Rank Algorithm Simulation
File: PageRankReducer.java
*/

// Input: <Text, Text>, same as the output format of Mapper
// Output: <NullWritable, Text>, put empty placeholder for the key and output the value to create the reusable output for iterations
public class PageRankReducer extends Reducer<Text, Text, NullWritable, Text> {
@Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        float curr_pr = 0; // initialize a page rank score 
        String outlinks = "";  // store the outlinks from the mapper for the ouptut
        for (Text value : values) {
            String v = value.toString(); // convert Text to string 
            if (v.contains(",")){ // find the string with pr value
                String [] v_lst = v.split(",");
                curr_pr += Float.parseFloat(v_lst[1]); // update the corresponding key's page rank scores by adding them all
            }else{
                outlinks = v; //if the string does not contain comma, then it must be an outlink
            }
        }
        String output = key.toString() + " " + outlinks + " " + (curr_pr);  // initialize the final output. Must match with input format

        // key is just a placeholder so NullWrtiable is useful in this case.
        // value is the final output and it must be readable from the mapper because we need to read the previous output during iteration
        context.write(NullWritable.get(), new Text(output));
    }
}
