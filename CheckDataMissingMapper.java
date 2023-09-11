import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import javax.naming.Context;

public class CheckDataMissingMapper
        extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final int MISSING = 9999;

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        String[] info = line.split(",");
        String user_id = info[0];
        String item_id = info[1];
        String category_id = info[2];
        String behavior_type = info[3];
        String timestamp = info[4];

        if (user_id.equals("")) {
            context.write(new Text("user_id_missing"), new IntWritable(1));
        }
        if (item_id.equals("")) {
            context.write(new Text("item_id_missing"), new IntWritable(1));
        }
        if (category_id.equals("")) {
            context.write(new Text("category_id_missing"), new IntWritable(1));
        }
        if (behavior_type.equals("")) {
            context.write(new Text("behavior_type_missing"), new IntWritable(1));
        }
        if (timestamp.equals("")) {
            context.write(new Text("timestamp_missing"), new IntWritable(1));
        }
    }
}