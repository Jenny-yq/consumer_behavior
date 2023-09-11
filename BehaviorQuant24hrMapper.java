import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import javax.naming.Context;

public class BehaviorQuant24hrMapper
        extends Mapper<LongWritable, Text, Text, IntWritable> {


    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        String[] info = line.split(";");
        String firstHalf = info[0];
        String behavior = firstHalf[3];
        String time = info[1];
        String[] timeArr = time.split(" ");
        String hr = timeArr[1];
        String[] hrArr = hr.split(":");
        String hour = hrArr[0];

        context.write(new Text(hour + " " + behavior), new IntWritable(1));
    }
}