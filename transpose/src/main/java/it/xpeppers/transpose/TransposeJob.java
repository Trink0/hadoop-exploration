package it.xpeppers.transpose;

import it.xpeppers.utils.TextArrayWritable;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by Trink0 on 21/01/14.
 */
public class TransposeJob extends Configured {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: TransposeJob <input path> <output path>");
            System.exit(-1);
        }
        Job job = createJob();
        defineFileInputAndOutput(job, args);
        setMapperAndReducerClass(job);
        setOutpuKeyAndValueClass(job);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static void setOutpuKeyAndValueClass(Job job) {
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(TextArrayWritable.class);
    }

    private static void setMapperAndReducerClass(Job job) {
        job.setMapperClass(TransposeMapper.class);
        job.setReducerClass(TransposeReducer.class);
    }

    private static void defineFileInputAndOutput(Job job, String[] args) throws IOException {
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
    }

    private static Job createJob() throws IOException {
        Job job = new Job();
        job.setJarByClass(TransposeJob.class);
        job.setJobName("TransposeJob attribute job");
        return job;
    }
}
