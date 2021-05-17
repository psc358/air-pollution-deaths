import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CleanDeathRate {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
			System.err.println("Usage: Clean <input path> <output path>");
			System.exit(-1);
		}


        Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", ",");
        Job job = Job.getInstance(conf, "Clean Death Rate");
        job.setJarByClass(CleanDeathRate.class);
        // Setting reducer to one for only one output file
		job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(CleanDeathRateMapper.class);
        job.setReducerClass(CleanDeathRateReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}