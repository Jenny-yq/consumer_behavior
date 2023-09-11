import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import javax.naming.Context;

public class TotalActivityReducer
        extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        int totalActivities = 0;
        for (IntWritable value : values) {
            totalActivities += value.get();
        }

        context.write(new Text("total activities"), new IntWritable(totalActivities));
    }
}