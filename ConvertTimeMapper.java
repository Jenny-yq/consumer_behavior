import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.text.DateFormat;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import javax.naming.Context;

public class ConvertTimeMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        String[] info = line.split(",");
        long unix_seconds = Long.parseLong(info[4]);
        Date date = new Date(unix_seconds*1000L);
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");
        format.setTimeZone(TimeZone.getTimeZone("GMT+8"));
        String formattedDate = format.format(date);

        StringBuffer temp = new StringBuffer();
        for(int i = 0; i < info.length - 1; i++){
            temp.append(info[i]);
            temp.append(",");
        }
        String newKey = temp.toString();

        context.write(new Text(newKey), new Text(formattedDate));
    }

}

