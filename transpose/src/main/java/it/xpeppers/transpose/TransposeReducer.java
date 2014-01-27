package it.xpeppers.transpose;

import it.xpeppers.utils.TextArrayWritable;
import org.apache.commons.collections.IteratorUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.List;

/**
 * Created by Trink0 on 16/01/14.
 */
public class TransposeReducer extends Reducer<Text, Text, Text, ArrayWritable> {
    private String separator = "\\x1f";

    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        ArrayWritable result = new TextArrayWritable();
        //List<Text> myList = IteratorUtils.toList(values.iterator());
        Text[] textValues = new Text[] {new Text(""), new Text(""), new Text("")};
        for (Text rowValue : values) {
            String[] row = rowValue.toString().split(separator);
            int index = getIndex(row[0]);
            textValues[index] = new Text(row[1]);
        }
        result.set(textValues);
        context.write(key, result);
    }
    private int getIndex(String attributeName){
        if (attributeName.equals("CHANNEL_TYPE"))
            return 0;
        else if (attributeName.equals("CD_ORDINE"))
            return 1;
        else
            return 2;
    }
    public void setSeparator(String separator) {
        this.separator = separator;
    }
}
