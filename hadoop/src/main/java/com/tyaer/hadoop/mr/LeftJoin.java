package com.tyaer.hadoop.mr;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.List;
import java.util.Vector;

/**
 * Created by Twin on 2017/8/8.
 */
public class LeftJoin {
    public static final String DELIMITER = ",";

    static class LeftJoinMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String filePath = ((FileSplit) context.getInputSplit()).getPath().toString();
            String line = value.toString();
            if (StringUtils.isBlank(line)) {
                return;
            }
            if (filePath.contains("employee")) {
                String[] split = line.split(DELIMITER);
                if (split.length < 2) {
                    return;
                } else {
                    context.write(new Text(split[0]), new Text("a:" + split[1]));
                }
            } else if (filePath.contains("salary")) {
                String[] lines = line.split(DELIMITER);
                if (lines.length < 2) return;
                String company_id = lines[0];
                String salary = lines[1];
                context.write(new Text(company_id), new Text("b:" + salary));
            }
        }

    }

    static class LeftJoinReduce extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Vector<String> vector1 = new Vector<>();
            Vector<String> vector2 = new Vector<>();

            for (Text text : values) {
                String value = text.toString();
                if (value.startsWith("a:")) {
                    vector1.add(value.substring(2));
                } else if (value.startsWith("b:")) {
                    vector2.add(value.substring(2));
                }
            }
            for (String a : vector1) {
                if (vector2.size() == 0) {
                    context.write(key, new Text(a + DELIMITER + "null"));
                } else {
                    for (String b : vector2) {
                        context.write(key, new Text(a + DELIMITER + b));
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        GenericOptionsParser optionparser = new GenericOptionsParser(conf, args);
        conf = optionparser.getConfiguration();

        Job job = Job.getInstance(conf, "leftjoin");
        job.setJarByClass(LeftJoin.class);
        FileInputFormat.addInputPaths(job, conf.get("input_dir"));
        Path out = new Path(conf.get("output_dir"));
        FileOutputFormat.setOutputPath(job, out);
        job.setNumReduceTasks(conf.getInt("reduce_num",2));

        job.setMapperClass(LeftJoinMapper.class);
        job.setReducerClass(LeftJoinReduce.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        conf.set("mapred.textoutputformat.separator", ",");

        boolean b = job.waitForCompletion(true);
        System.out.println(b);
    }
}
