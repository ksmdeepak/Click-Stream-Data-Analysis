package com.org;


import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.FloatWritable;

import utils.MiscUtils;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


//import org.apache.log4j.Logger;

public class ClickStream extends Configured implements Tool {

//    private static final Logger LOG = Logger.getLogger(ClickStream.class);

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new ClickStream(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "most clicked");
        job.setJarByClass(this.getClass());
        // Use TextInputFormat, the default unless job.setInputFormatClass is used

        int jobId = Integer.parseInt(args[2]);
        if(jobId==1){
            FileInputFormat.addInputPath(job, new Path(args[0]));

            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            job.setMapperClass(Map.class);
            job.setReducerClass(Reduce.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            job.setNumReduceTasks(1);
            return job.waitForCompletion(true) ? 0 : 1;
        }
        else if (jobId==2){
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            job.setMapperClass(MapHours.class);
            job.setReducerClass(ReduceHours.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            job.setNumReduceTasks(1);
            return job.waitForCompletion(true) ? 0 : 1;
        }
        else if (jobId==3){
//            JobControl jobControl = new JobControl("jobChain");

            Job job1 = Job.getInstance(conf);
            job1.setJarByClass(this.getClass());
            FileInputFormat.addInputPath(job1, new Path(args[0]));
            FileOutputFormat.setOutputPath(job1, new Path(args[1]));

            job1.setMapperClass(ClicksMapper.class);
            job1.setReducerClass(ClicksReducer.class);
            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(IntWritable.class);
            job1.setNumReduceTasks(1);
            return job1.waitForCompletion(true) ? 0 : 1;

//            ControlledJob cj1 = new ControlledJob(conf);
//            cj1.setJob(job1);
//            jobControl.addJob(cj1);
//
//            Configuration conf2 = getConf();
//
//            Job job2 = Job.getInstance(conf2);
//            job2.setJarByClass(this.getClass());
//
//            FileInputFormat.setInputPaths(job2, new Path(args[0] + "/buys.txt"));
//            FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/temp2"));
//
//            job2.setMapperClass(BuysMapper.class);
//            job2.setReducerClass(BuysReducer.class);
//            job2.setOutputKeyClass(Text.class);
//            job2.setOutputValueClass(Text.class);
//            job2.setNumReduceTasks(1);
//
//            ControlledJob cj2 = new ControlledJob(conf2);
//            cj2.setJob(job2);
//            jobControl.addJob(cj2);
//
//
//
//            Configuration conf3 = getConf();
//
//            Job job3 = Job.getInstance(conf3);
//            job2.setJarByClass(this.getClass());
//
//            FileInputFormat.setInputPaths(job2, new Path(args[1]));
//            FileOutputFormat.setOutputPath(job2, new Path(args[1]));
//            job3.setMapperClass(SuccessRatioMapper.class);
//            job3.setReducerClass(SuccessRatioReducer.class);
//            job3.setOutputKeyClass(Text.class);
//            job3.setOutputValueClass(FloatWritable.class);
//            job3.setNumReduceTasks(1);
//
//            ControlledJob cj3 = new ControlledJob(conf3);
//            cj3.setJob(job3);
//            cj3.addDependingJob(cj1);
//            cj3.addDependingJob(cj2);
//            jobControl.addJob(cj3);
//
//            Thread jobControlThread = new Thread(jobControl);
//            jobControlThread.start();
//            while (!jobControl.allFinished()) {
//                System.out.println("Jobs in waiting state: " + jobControl.getWaitingJobList().size());
//                System.out.println("Jobs in ready state: " + jobControl.getReadyJobsList().size());
//                System.out.println("Jobs in running state: " + jobControl.getRunningJobList().size());
//                System.out.println("Jobs in success state: " + jobControl.getSuccessfulJobList().size());
//                System.out.println("Jobs in failed state: " + jobControl.getFailedJobList().size());
//                try {
//                    Thread.sleep(5000);
//                } catch (Exception e) {
//
//                }
//
//            }
//            System.exit(0);
//            return (job1.waitForCompletion(true) ? 0 : 1);
//

        }

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private long numRecords = 0;

        @Override
        public void map(LongWritable offset, Text lineText, Context context)
                throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String line = lineText.toString();
            Text currentWord = new Text();
            List<String> input = Arrays.asList(line.split(","));
            int month = 4;
            int inputMonth = Integer.parseInt(input.get(1).substring(5,7));

            if(inputMonth==month){
                currentWord = new Text(input.get(2));
                context.write(currentWord,one);
            }
        }
    }

    public static class MapHours extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(LongWritable offset, Text lineText, Context context)
                throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String line = lineText.toString();
            Text currentWord = new Text();
            List<String> input = Arrays.asList(line.split(","));
            String inputHour = (input.get(1).substring(11,13));
            currentWord = new Text(inputHour);
            int value = Integer.parseInt(input.get(3)) * Integer.parseInt(input.get(4));
            context.write(currentWord,new IntWritable(value));

        }
    }

    public static class ClicksMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
//        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(LongWritable offset, Text lineText, Context context)
                throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String line = lineText.toString();
            List<String> input = Arrays.asList(line.split(","));
            Text currentWord = new Text(input.get(2));
            if(input.size()==4)
                context.write(currentWord,new IntWritable(0));
            else
                context.write(currentWord,new IntWritable(1));

        }
    }

//    public static class BuysMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
//        private final static IntWritable one = new IntWritable(1);
//        private Text word = new Text();
//
//        @Override
//        public void map(LongWritable offset, Text lineText, Context context)
//                throws IOException, InterruptedException {
//            Configuration conf = context.getConfiguration();
//            String line = lineText.toString();
//            List<String> input = Arrays.asList(line.split(","));
//            Text currentWord = new Text(input.get(2));
//            context.write(currentWord,one);
//
//        }
//    }
//
//    public static class SuccessRatioMapper extends Mapper<LongWritable, Text, Text, Text> {
//
//        @Override
//        public void map(LongWritable offset, Text lineText, Context context)
//                throws IOException, InterruptedException {
//            Configuration conf = context.getConfiguration();
//            String line = lineText.toString();
//            List<String> input = Arrays.asList(line.split("\t",-1));
//            Text currentWord = new Text(input.get(0));
//            String count = input.get(1);
//            context.write(currentWord,new Text(count));
//
//        }
//    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        private java.util.Map<Text,IntWritable> countMap = new HashMap<Text, IntWritable>();

        @Override
        public void reduce(Text word, Iterable<IntWritable> counts, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable count : counts) {
                sum += count.get();
            }
            countMap.put(new Text(word), new IntWritable(sum));

        }
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {


            java.util.Map<Text,IntWritable> sortedHashMap = MiscUtils.sortByValues(countMap);

            int counter = 0;
            for (Text key: sortedHashMap.keySet()) {
                if (counter ++ == 20) {
                    break;
                }
                context.write(key,sortedHashMap.get(key));
            }
        }


    }

    public static class ReduceHours extends Reducer<Text, IntWritable, Text, IntWritable> {
        private java.util.Map<Text,IntWritable> countMap = new HashMap<Text, IntWritable>();

        @Override
        public void reduce(Text word, Iterable<IntWritable> counts, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable count : counts) {
                sum += count.get();
            }
            countMap.put(new Text(word), new IntWritable(sum));

        }
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {


            java.util.Map<Text,IntWritable> sortedHashMap = MiscUtils.sortByValues(countMap);

            for (Text key: sortedHashMap.keySet()) {

                context.write(key,sortedHashMap.get(key));
            }
        }


    }

    public static class ClicksReducer extends Reducer<Text, IntWritable, Text, FloatWritable> {

        private java.util.Map<Text,FloatWritable> countMap = new HashMap<Text, FloatWritable>();

        @Override
        public void reduce(Text word, Iterable<IntWritable> counts, Context context)
                throws IOException, InterruptedException {
            int buys = 0;
            int clicks=0;
            for (IntWritable count : counts) {
                int value = count.get();
                if(value==0)
                    clicks++;
                else
                    buys++;
            }
            countMap.put(new Text(word), new FloatWritable((float)(buys)/(float)(clicks)));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {


            java.util.Map<Text,FloatWritable> sortedHashMap = MiscUtils.sortByValues(countMap);

            int counter = 0;

            for (Text key: sortedHashMap.keySet()) {
                if(counter++ ==100)
                    break;
                context.write(key,sortedHashMap.get(key));
            }
        }



    }

//    public static class BuysReducer extends Reducer<Text, IntWritable, Text, Text> {
//
//        @Override
//        public void reduce(Text word, Iterable<IntWritable> counts, Context context)
//                throws IOException, InterruptedException {
//            int sum = 0;
//            for (IntWritable count : counts) {
//                sum += count.get();
//            }
//            String value = sum+"_"+"B";
//            context.write(new Text(word), new Text(value));
//
//        }
//
//    }

//    public static class SuccessRatioReducer extends Reducer<Text, Text, Text, FloatWritable> {
//        private java.util.Map<Text,FloatWritable> countMap = new HashMap<Text, FloatWritable>();
//
//        @Override
//        public void reduce(Text word, Iterable<Text> counts, Context context)
//                throws IOException, InterruptedException {
//            int buys = 0;
//            int clicks = 0 ;
//            for (Text count : counts) {
//                String value = count.toString();
//                List<String> data = Arrays.asList(value.split("_"));
//                if (data.get(1).equals('C'))
//                    clicks = Integer.parseInt(data.get(0));
//                else
//                    buys = Integer.parseInt(data.get(0));
//
//            }
//            countMap.put(new Text(word), new FloatWritable((float)(buys)/clicks));
//
//        }
//        @Override
//        protected void cleanup(Context context) throws IOException, InterruptedException {
//
//
//            java.util.Map<Text,FloatWritable> sortedHashMap = MiscUtils.sortByValues(countMap);
//
//            for (Text key: sortedHashMap.keySet()) {
//
//                context.write(key,sortedHashMap.get(key));
//            }
//        }
//
//
//    }

}

