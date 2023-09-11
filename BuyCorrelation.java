import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BuyCorrelation{

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: Check Data Missing <input path> <output path>");
            System.exit(-1);
        }

        Job job = Job.getInstance();
        job.setJarByClass(BuyCorrelation.class);
        job.setJobName("Buy Correlation");
        job.getConfiguration().set("mapreduce.output.textoutputformat.separator", ":");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(BuyCorrelationMapper.class);
        job.setCombinerClass(BuyCorrelationReducer.class);
        job.setReducerClass(BuyCorrelationReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(1);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

