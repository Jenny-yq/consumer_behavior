import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import javax.naming.Context;

public class BuyCorrelationReducer
        extends Reducer<Text, Text, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        int minTime = Integer. MAX_VALUE;

        for (Text value : values) {
            String info = value.toString();
            String[] infoArr = info.split(" ");
            String behavior = infoArr[0];
            int time = Integer.parseInt(infoArr[1]);
            if(behavior.equals("buy")){
                minTime = time < minTime ? time : minTime;
            }
        }

        int pvBeforeBuy = 0;
        int cartBeforeBuy = 0;
        int favBeforeBuy = 0;

        for (Text value : values) {
            String info = value.toString();
            String[] infoArr = info.split(" ");
            String behavior = infoArr[0];
            int time = Integer.parseInt(infoArr[1]);
            if(behavior.equals("pv") && (time < minTime)){
                pvBeforeBuy += 1;
            }
            else if(behavior.equals("cart") && (time < minTime)){
                cartBeforeBuy += 1;
            }
            else if(behavior.equals("fav") && (time < minTime)){
                favBeforeBuy += 1;
            }
        }

        String user_id = key.toString().split(" ")[0];
        String item_id = key.toString().split(" ")[1];

        context.write(new Text(user_id + " " + item_id + " " + "pv before buy"), new IntWritable(pvBeforeBuy));
        context.write(new Text(user_id + " " + item_id + " " + "cart before buy"), new IntWritable(cartBeforeBuy));
        context.write(new Text(user_id + " " + item_id + " " + "fav before buy"), new IntWritable(favBeforeBuy));
    }
}
