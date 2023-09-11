import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import javax.naming.Context;

public class TotalUserReducer
        extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        int totalUsers = 0;
        for (IntWritable value : values) {
            totalUsers += value.get();
        }

        context.write(new Text("total users"), new IntWritable(totalUsers));
    }
}