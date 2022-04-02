import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
Author: Soowhan Park
Email: sp6682@nyu.edu
Title: Page Rank Algorithm Simulation
File: PageRank.java
*/

public class PageRank {
    public static void main(String[] args) throws Exception {
        String new_path = "/user/sp6682/lab3_output/out_put"; // set a path instead of reading the command line linput path

        // Run the map/reduce 3 times by reading the output from the previous iteration.
        for(int i = 0; i < 3; i++){
            Job job = Job.getInstance();
            if (i == 0){ // case for the first iteration when the input file is not a previous output
                job.setJarByClass(PageRank.class);
                job.setJobName("Page Rank Algorithm");

                FileInputFormat.addInputPath(job, new Path("/user/sp6682/lab3_output/prinputs.txt"));
                new_path += 0; // add a flag to the path
                FileOutputFormat.setOutputPath(job, new Path(new_path));

                job.setMapperClass(PageRankMapper.class);
                job.setReducerClass(PageRankReducer.class);

                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(Text.class);

                job.setOutputKeyClass(NullWritable.class);
                job.setOutputValueClass(Text.class);
                job.setNumReduceTasks(1);
                job.waitForCompletion(true);
                
            }else{ // case for the i > 0 to read the output from the previous iteration. 
                job.setJarByClass(PageRank.class);
                job.setJobName("Page Rank Algorithm");
                FileInputFormat.addInputPath(job, new Path(new_path + "/part-r-00000")); // the output fileaname will be path + "part-r-00000"
                new_path += 0; // add a flag to the path
                FileOutputFormat.setOutputPath(job, new Path(new_path));

                job.setMapperClass(PageRankMapper.class);
                job.setReducerClass(PageRankReducer.class);

                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(Text.class);

                job.setOutputKeyClass(NullWritable.class);
                job.setOutputValueClass(Text.class);
                job.setNumReduceTasks(1);
                job.waitForCompletion(true);
            }
        }
        System.exit(0); // exit after running 3 times 
    }
}