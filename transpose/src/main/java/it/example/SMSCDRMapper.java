package it.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Created by Trink0 on 16/01/14.
 */
public class SMSCDRMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text status = new Text();
    private final static IntWritable addOne = new IntWritable(1);

    protected void map(LongWritable key, Text value, Context context)
            throws java.io.IOException, InterruptedException {
        String[] tokens = value.toString().split(";");

        if (tokens.length == 5) {
            String code = tokens[4];
            Integer cdr = Integer.parseInt(tokens[1]);

            if (cdr == 1){
                status.set(code);
                context.write(status, addOne);
            }
        }
    }
}
