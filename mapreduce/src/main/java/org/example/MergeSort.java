package org.example;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class MergeSort {

    //map函数读取输入中的value，将其转化成IntWritable类型，最后作为输出key
    public static class Map extends Mapper<Object, Text, IntWritable, IntWritable>{

        private static IntWritable data = new IntWritable();
        public void map(Object key, Text value, Context context) throws IOException,InterruptedException{
            String text = value.toString();
            data.set(Integer.parseInt(text));//将括号内容复制给data对象
            context.write(data, new IntWritable(1));//括号内容作为中间结果扔出去交给shuffle处理
        }
    }

    //reduce函数将map输入的key复制到输出的value上，然后根据输入的value-list中元素的个数决定key的输出次数,定义一个全局变量line_num来代表key的位次
    public static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>{
        private static IntWritable line_num = new IntWritable(1);

        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException,InterruptedException{
            for(IntWritable val : values){
                context.write(line_num, key);
                line_num = new IntWritable(line_num.get() + 1);
            }
        }
    }

    //自定义Partition函数，此函数根据输入数据的最大值和MapReduce框架中Partition的数量获取将输入数据按照大小分块的边界，然后根据输入数值和边界的关系返回对应的Partiton ID
    public static class Partition extends Partitioner<IntWritable, IntWritable>{
        public int getPartition(IntWritable key, IntWritable value, int num_Partition){
            int Maxnumber = 65223;//int型的最大数值
            int bound = Maxnumber/num_Partition+1;
            int keynumber = key.get();//从key的序列类型转换成int类型
            for (int i = 0; i<num_Partition; i++){
                if(keynumber<bound * (i+1) && keynumber>=bound * i){
                    return i;
                }
            }
            return -1;// 表示返回一个代数值，一般用在子函数结尾。按照程序开发的一般惯例，表示该函数失败；
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
        Job job = Job.getInstance(conf,"Merge and sort");//设置环境参数
        job.setJarByClass(MergeSort.class);//设置整个程序的类名
        job.setMapperClass(Map.class);//添加Mapper类
        job.setReducerClass(Reduce.class);//添加Reducer类
        job.setPartitionerClass(Partition.class);//添加Partitioner类
        job.setOutputKeyClass(IntWritable.class);//设置输出类型
        job.setOutputValueClass(IntWritable.class);//设置输出类型
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));//设置输入原始文件文件路径
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));//设置输出文件路径
        //Job运行是通过job.waitForCompletion(true)，true表示将运行进度等信息及时输出给用户，false的话只是等待作业结束
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }

}