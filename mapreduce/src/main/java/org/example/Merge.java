package org.example;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Merge {

    //重载map函数，直接将输入中的value复制到输出数据的key上
    public static class Map extends Mapper<Object, Text, Text, Text>{
        private static Text text = new Text();
        public void map(Object key, Text value, Context context) throws IOException,InterruptedException{
            text = value;
            context.write(text, new Text(""));//括号内容作为中间结果扔出去交给shuffle处理
        }
    }

    //重载reduce函数，直接将输入中的key复制到输出数据的key上
    public static class Reduce extends Reducer<Text, Text, Text, Text>{
        public void reduce(Text key, Iterable<Text> values, Context context ) throws IOException,InterruptedException{
            context.write(key, new Text(""));
        }
    }

    public static void main(String[] args) throws Exception{

        // TODO Auto-generated method stub
        Configuration conf = new Configuration();//程序运行时的参数
        conf.set("fs.default.name","hdfs://localhost:9000");
        String[] otherArgs = new String[]{"/input","/output"}; /* 直接设置输入参数 */
        if (otherArgs.length != 2) {
            System.err.println("Usage: wordcount <in><out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf,"Merge and duplicate removal");//设置环境参数
        job.setJarByClass(Merge.class);//设置整个程序的类名
        job.setMapperClass(Map.class);//添加Mapper类
        job.setCombinerClass(Reduce.class);//设置Combiner类
        job.setReducerClass(Reduce.class);//添加Reducer类
        job.setOutputKeyClass(Text.class);//设置输出类型
        job.setOutputValueClass(Text.class);//设置输出类型
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));//设置输入原始文件文件路径
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));//设置输出文件路径
        //Job运行是通过job.waitForCompletion(true)，true表示将运行进度等信息及时输出给用户，false的话只是等待作业结束
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }

}