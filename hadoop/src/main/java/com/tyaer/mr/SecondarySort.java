package com.tyaer.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

public class SecondarySort {

    public static class IntPair implements WritableComparable<IntPair> {
        int first;
        int second;

        public int getFirst() {
            return first;
        }

        public void setFirst(int first) {
            this.first = first;
        }

        public int getSecond() {
            return second;
        }

        public void setSecond(int second) {
            this.second = second;
        }

        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.write(first);
            dataOutput.write(second);
        }

        public void readFields(DataInput dataInput) throws IOException {
            first = dataInput.readInt();
            second = dataInput.readInt();
        }

        public int compareTo(IntPair intPair) {
            if (first != intPair.first) {
                return first < intPair.first ? -1 : 1;
            } else if (second != intPair.second) {
                return second < intPair.second ? -1 : 1;
            } else {
                return 0;
            }
        }

        @Override
        public int hashCode() {
            return first * 157 + second;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (this == obj) {
                return true;
            }
            if (obj instanceof IntPair) {
                IntPair intPair = (IntPair) obj;
                return intPair.first == first && intPair.second == second;
            } else {
                return true;
            }
        }

        public static class FirstPartitioner extends Partitioner<IntPair, IntWritable> {

            public int getPartition(IntPair key, IntWritable intWritable, int partitionNum) {
                return Math.abs(key.getFirst() * 127) % partitionNum;
            }
        }

        public static class GroupingComparator extends WritableComparator {

            protected GroupingComparator() {
                super(IntPair.class, true);
            }

            @Override
            public int compare(WritableComparable a, WritableComparable b) {
                IntPair a1 = (IntPair) a;
                IntPair b1 = (IntPair) b;
                int first1 = a1.getFirst();
                int first2 = b1.getFirst();
                return first1 == first2 ? 0 : (first1 < first2 ? -1 : 1);
            }
        }

        public static class Map extends Mapper<LongWritable, Text, IntPair, IntWritable> {
            private final IntPair intPair = new IntPair();
            private final IntWritable intWritable = new IntWritable();

            public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                String line = value.toString();
                StringTokenizer stringTokenizer = new StringTokenizer(line);
                int left = 0;
                int right = 0;
                if (stringTokenizer.hasMoreTokens()) {
                    left = Integer.parseInt(stringTokenizer.nextToken());
                    if (stringTokenizer.hasMoreTokens()) {
                        right = Integer.parseInt(stringTokenizer.nextToken());
                    }
                    intPair.setFirst(left);
                    intPair.setSecond(right);
                    intWritable.set(right);
                    context.write(intPair, intWritable);
                }
            }
        }

        public static class Reduce extends Reducer<IntPair, IntWritable, Text, IntWritable> {
            private final Text left = new Text();
            private static final Text SEPARATOR = new Text("----------------------------------------");

            @Override
            protected void reduce(IntPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
                context.write(SEPARATOR, null);
                left.set(String.valueOf(key.getFirst()));
                for (IntWritable value : values) {
                    context.write(left, value);
                }
            }
        }

        public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
            System.out.println(123);
            Configuration configuration = new Configuration();
            Job job = Job.getInstance(configuration);
            job.setJarByClass(SecondarySort.class);
            job.setMapperClass(Map.class);
            job.setReducerClass(Reduce.class);
            job.setPartitionerClass(FirstPartitioner.class);
            job.setGroupingComparatorClass(GroupingComparator.class);
            job.setMapOutputKeyClass(IntPair.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            FileInputFormat.setInputPaths(job, new Path("D:\\IdeaProjects\\~Twin\\Twin-Study\\file\\test1.txt"));
            FileOutputFormat.setOutputPath(job, new Path("D:\\IdeaProjects\\~Twin\\Twin-Study\\file\\test2.txt"));
            boolean b = job.waitForCompletion(true);
            System.exit(b ? 0 : 1);
        }
    }
}
