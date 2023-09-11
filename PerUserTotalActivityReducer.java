import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import javax.naming.Context;

public class PerUserTotalActivityReducer
        extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        int totalMissing = 0;
        for (IntWritable value : values) {
            totalMissing += value.get();
        }
        context.write(key, new IntWritable(totalMissing));
    }
}