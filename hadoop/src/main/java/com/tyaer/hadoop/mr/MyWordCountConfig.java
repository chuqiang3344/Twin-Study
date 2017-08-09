package com.tyaer.hadoop.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.net.URI;

/**
 * 打包到处方式，参数在执行时手动输入 而非在代码中固定写死
 * 实现单词计数功能
 * 测试文件 hello内容为:
 * hello   you
 * hello   me  me  me
 *
 * @author zm
 */
public class MyWordCountConfig extends Configured implements Tool {

    static String FILE_ROOT = "";
    static String FILE_INPUT = "";
    static String FILE_OUTPUT = "";

    public static void main(String[] args) throws Exception {

        ToolRunner.run(new MyWordCountConfig(), args);
    }

    @Override
    public int run(String[] args) throws Exception {

        FILE_ROOT = args[0];
        FILE_INPUT = args[1];
        FILE_OUTPUT = args[2];

        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI(FILE_ROOT), conf);
        Path outpath = new Path(FILE_OUTPUT);
        if (fileSystem.exists(outpath)) {
            fileSystem.delete(outpath, true);
        }

        // 0 定义干活的人
        Job job = Job.getInstance(conf);
        //打包运行必须执行的方法
        job.setJarByClass(MyWordCountConfig.class);
        // 1.1 告诉干活的人 输入流位置     读取hdfs中的文件。每一行解析成一个<k,v>。每一个键值对调用一次map函数
        FileInputFormat.setInputPaths(job, FILE_INPUT);
        // 指定如何对输入文件进行格式化，把输入文件每一行解析成键值对
        job.setInputFormatClass(TextInputFormat.class);

        //1.2 指定自定义的map类
        job.setMapperClass(MyMapper2.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //1.3 分区
        job.setNumReduceTasks(1);

        //1.4 TODO 排序、分组    目前按照默认方式执行
        //1.5 TODO 规约

        //2.2 指定自定义reduce类
        job.setReducerClass(MyReducer2.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //2.3 指定写出到哪里
        FileOutputFormat.setOutputPath(job, outpath);
        job.setOutputFormatClass(TextOutputFormat.class);

        // 让干活的人干活
        job.waitForCompletion(true);
        return 0;
    }

}

/**
 * 继承mapper 覆盖map方法，hadoop有自己的参数类型
 * 读取hdfs中的文件。每一行解析成一个<k,v>。每一个键值对调用一次map函数，
 * 这样，对于文件hello而言，调用MyMapper方法map后得到结果：
 * <hello,1>,<you,1>,<hello,1>,<me,1>
 * 方法后，得到结果为：
 * KEYIN,      行偏移量
 * VALUEIN,    行文本内容(当前行)
 * KEYOUT,     行中出现的单词
 * VALUEOUT    行中出现单词次数,这里固定写为1
 */
class MyMapper2 extends Mapper<LongWritable, Text, Text, LongWritable> {

    @Override
    protected void map(LongWritable k1, Text v1, Context context)
            throws IOException, InterruptedException {

        String[] v1s = v1.toString().split(" ");
        for (String word : v1s) {
            context.write(new Text(word), new LongWritable(1));
        }
    }
}

/**
 * <hello,{1,1}>,<me,{1}>,<you,{1}>, 每个分组调用一次 reduce方法
 * <p>
 * KEYIN,     行中出现单词
 * VALUEIN,   行中出现单词个数
 * KEYOUT,    文件中出现不同单词
 * VALUEOUT   文件中出现不同单词总个数
 */
class MyReducer2 extends Reducer<Text, LongWritable, Text, LongWritable> {

    protected void reduce(Text k2, Iterable<LongWritable> v2s, Context ctx)
            throws IOException, InterruptedException {
        long times = 0L;
        for (LongWritable l : v2s) {
            times += l.get();
        }
        ctx.write(k2, new LongWritable(times));
    }

}
