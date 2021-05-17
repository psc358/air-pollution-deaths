import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CleanDeathRateMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text word = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] data = line.split(",");

        if (!data[0].equals("Entity")) {
            String row = "";

            if (data.length == 38 || data.length == 40) {
                if(data[0].equals("\"Central Europe") && data[1].equals(" Eastern Europe")) {
                    // "Central Europe, Eastern Europe, and Central Asia"
                    row = data[0].substring(1) + " " + data[1] + " " + data[2].substring(0,16) + "," + data[4] + "," + data[8] + "," + data[31] + "," + data[30];
        
                } else if (data[0].equals("\"Southeast Asia") && data[1].equals(" East Asia")) {
                    // "Southeast Asia, East Asia, and Oceania"
                    row = data[0].substring(1) + " " + data[1] + " " + data[2].substring(0,11) + "," + data[4] + "," + data[8] + "," + data[31] + "," + data[30];
                    
                } else {
                    row = data[0] + "," + data[2] + "," + data[6] + "," + data[29] + "," + data[28];
                }

                Text out = new Text(row);
                context.write(new Text(""), out);
            }
        }
    }
}