package org.ljnet.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class WC {
    public static class wcmap extends Mapper<LongWritable,Text,Text,IntWritable> {
        IntWritable count=new IntWritable(1);
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split=value.toString().split("\t");
            for(String word:split){
                context.write(new Text(word),count);
            }
        }
    }
    public static class wcreduce extends Reducer<Text,IntWritable,Text,IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
           int wc=0;
            for(IntWritable count:values){
                wc++;
            }
            context.write(new Text(key),new IntWritable(wc));
        }
    }
    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
        /**
         * 解决hdfs用户权限问题，由于集群是有clouera manager 安装，修改hadoop hdfs-site.xml配置文件不生效。
         * 可以直接给hdfs所需目录权限使用命令：sudo -uhdfs  hadoop dfs -chmod 777 /data.chw
         */
        String INPUT_PATH="/data.chw/hivetest.txt";
        String OUTPUI_PATH="/data.chw/mr.out";
        Configuration conf=new Configuration();
        conf.set("fs.defaultFS","hdfs://hadoop01.ljnet.com");
        Job job=Job.getInstance(conf,"wc");
        FileSystem fs=FileSystem.get(new URI(OUTPUI_PATH),conf);
        Path path=new Path(OUTPUI_PATH);
        if(fs.exists(path)){
            fs.delete(path);
        }
        job.setJarByClass(WC.class);
        job.setMapperClass(wcmap.class);
        job.setReducerClass(wcreduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job,new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job,new Path(OUTPUI_PATH));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    //    boolean re=job.waitForCompletion(true);
    }
}
