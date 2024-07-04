package it.unipi.hadoop;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class LetterFrequency2JobCombiner {

    public static class DoublePercentWritable extends DoubleWritable {
        public DoublePercentWritable() { // needed by Hadoop
            super();
        }
        public DoublePercentWritable(double value) {
            super(value);
        }

        @Override
        public String toString() {
            // access the first and second element of the array
            return String.format("%.2f", this.get() * 100) + "%";
        }
    }

    public static void main(String[] args) throws Exception {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "count characters");
            job.setJarByClass(LetterFrequency2JobCombiner.class);
            job.setMapperClass(CharacterCounterMapper.class);
            job.setCombinerClass(CharacterCounterCombiner.class);
            job.setReducerClass(CharacterCounterReducer.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(LongWritable.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            String tempoutput = args[1] + "_temp";
            FileOutputFormat.setOutputPath(job, new Path(tempoutput));
            job.setNumReduceTasks(args.length > 2 ? Integer.parseInt(args[2]) : 1);
            if (!job.waitForCompletion(true)) { // true is to set verbose mode
                System.exit(1);
            }
            Configuration conf2 = new Configuration();

            long total = 0;
            Path tempPath = new Path(tempoutput + "/part-r-00000");
            FileSystem fs = FileSystem.get(conf);
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(tempPath)));
            String line;
            while ((line = br.readLine()) != null) { // should be only one
                String[] parts = line.split("\t");
                total += Long.parseLong(parts[1]); // line is in the form of key '\t' value
            }
            br.close();
            assert total > 0;
            conf2.setLong("total", total);

            Job job2 = Job.getInstance(conf2, "letter frequency");
            job2.setJarByClass(LetterFrequency2JobCombiner.class);
            job2.setMapperClass(LetterFrequencyMapper.class);
            job2.setCombinerClass(LetterFrequencyCombiner.class);
            job2.setReducerClass(LetterFrequencyReducer.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(DoublePercentWritable.class);
            FileInputFormat.addInputPath(job2, new Path(args[0]));
            FileOutputFormat.setOutputPath(job2, new Path(args[1]));
            job2.setNumReduceTasks(args.length > 2 ? Integer.parseInt(args[2]) : 1);
            System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }

    // ==================== CharacterCounter ====================

    public static class CharacterCounterMapper extends Mapper<Object, Text, IntWritable, LongWritable>{
        IntWritable count = new IntWritable(0);
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            for (char c : value.toString().toCharArray()) {
                if (c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z') {
                    context.write(count, new LongWritable(1));
                }
            }
        }
    }

    public static class CharacterCounterCombiner extends Reducer<IntWritable, LongWritable,IntWritable, LongWritable> {
        @Override
        public void reduce(IntWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }
            context.write(key, new LongWritable(sum));
        }
    }

    public static class CharacterCounterReducer extends Reducer<IntWritable, LongWritable, IntWritable, LongWritable> {
        @Override
        public void reduce(IntWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }
            context.write(key, new LongWritable(sum));
        }
    }

    // ==================== LetterFrequency ====================

    public static class LetterFrequencyMapper extends Mapper<Object, Text, Text, DoublePercentWritable>{
        Text letter = null;
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            for (char c : value.toString().toCharArray()) {
                if (c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z') {
                    c = Character.toLowerCase(c);
                    letter = new Text(String.valueOf(c));
                    context.write(letter, new DoublePercentWritable(1));
                }
            }
        }
    }

    public static class LetterFrequencyCombiner extends Reducer<Text, DoublePercentWritable,Text, DoublePercentWritable> {
        @Override
        public void reduce(Text key, Iterable<DoublePercentWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            for (DoublePercentWritable val : values) {
                sum += val.get();
            }
            context.write(key, new DoublePercentWritable(sum));
        }
    }

    public static class LetterFrequencyReducer extends Reducer<Text, DoublePercentWritable, Text, DoublePercentWritable> {
        @Override
        public void reduce(Text key, Iterable<DoublePercentWritable> values, Context context) throws IOException, InterruptedException {
            long total = context.getConfiguration().getLong("total", 1);
            double sum = 0;
            for (DoublePercentWritable val : values) {
                sum += val.get();
            }
            context.write(key, new DoublePercentWritable(sum / total));
        }
    }

}
