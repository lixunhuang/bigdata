package mapreduce;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MergeAndDeduplicate {

    public static class MergeAndDeduplicateMapper extends Mapper<Object, Text, Text, Text> {

        private final static Text line = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            line.set(value);
            context.write(line, new Text(""));
        }
    }

    public static class MergeAndDeduplicateReducer extends Reducer<Text, Text, Text, Text> {

        private final static Text emptyText = new Text("");

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.write(key, emptyText);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "MergeAndDeduplicate");
        job.setJarByClass(MergeAndDeduplicate.class);
        job.setMapperClass(MergeAndDeduplicateMapper.class);
        job.setCombinerClass(MergeAndDeduplicateReducer.class);
        job.setReducerClass(MergeAndDeduplicateReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        String inputPath1 = "1";
        String inputPath2 = "2";
        String outputPath = "3";
        FileInputFormat.addInputPath(job, new Path(inputPath1));
        FileInputFormat.addInputPath(job, new Path(inputPath2));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
