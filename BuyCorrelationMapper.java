import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import javax.naming.Context;

public class BuyCorrelationMapper
        extends Mapper<LongWritable, Text, Text, Text> {


    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        String[] info = line.split(",");
        String user_id = info[0];
        String item_id = info[1];
        String behavior = info[3];
        String time = info[4];

        context.write(new Text(user_id + " " + item_id), new Text(behavior + " " + time));
    }
}
