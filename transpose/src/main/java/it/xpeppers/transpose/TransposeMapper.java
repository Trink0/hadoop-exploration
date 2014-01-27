package it.xpeppers.transpose;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Created by Trink0 on 16/01/14.
 */
public class TransposeMapper extends Mapper<LongWritable, Text, Text, Text> {

    private String separator = "0x1f";

    protected void map(LongWritable key, Text value, Context context)
            throws java.io.IOException, InterruptedException {
        String[] tokens = value.toString().split(separator);
        if (tokens.length < 7)
            return; // Throw Exception ?
        Text groupKey = new Text(tokens[3]);
        String values = String.format("%s,%s", tokens[5], tokens[6]);
        context.write(groupKey, new Text(values));
    }

    public void setSeparator(String separator) {
        this.separator = separator;
    }
}
