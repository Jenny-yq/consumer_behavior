import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BehaviorQuant24hr {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: Check Data Missing <input path> <output path>");
            System.exit(-1);
        }

        Job job = Job.getInstance();
        job.setJarByClass(BehaviorQuant24hr.class);
        job.setJobName("Behavior Quantity 24 hr");
        job.getConfiguration().set("mapreduce.output.textoutputformat.separator", ":");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(BehaviorQuant24hrMapper.class);
        job.setCombinerClass(BehaviorQuant24hrReducer.class);
        job.setReducerClass(BehaviorQuant24hrReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(1);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
