package it.unipi.hadoop;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class LetterFrequencyInMapperCombiner {
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
        Job job = Job.getInstance(conf, "letter frequency");
        job.setJarByClass(LetterFrequencyInMapperCombiner.class);
        job.setMapperClass(LetterFrequencyMapper.class);
        job.setMapOutputValueClass(ArrayPrimitiveWritable.class);
        job.setReducerClass(LetterFrequencyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoublePercentWritable.class);
        job.setNumReduceTasks(args.length > 2 ? Integer.parseInt(args[2]) : 1);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1); // true is for verbose mode
    }

    public static class LetterFrequencyMapper extends Mapper<Object, Text, Text, ArrayPrimitiveWritable>{
        private Map<Character, Long> letterCountMap;
        private Text letter = new Text();

        @Override
        protected void setup(Context context) {
            letterCountMap = new HashMap<>();
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            for (char c : line.toCharArray()) {
                if (c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z') {
                    c = Character.toLowerCase(c);
                    letterCountMap.put('#', letterCountMap.getOrDefault('#', 0L) + 1);
                    letterCountMap.put(c, letterCountMap.getOrDefault(c, 0L) + 1);
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<Character, Long> entry : letterCountMap.entrySet()) {
                letter.set(entry.getKey().toString());
                ArrayPrimitiveWritable values = new ArrayPrimitiveWritable();
                values.set(new long[]{entry.getValue(), letterCountMap.get('#')});
                context.write(letter, values);
            }
        }
    }

    public static class LetterFrequencyReducer extends Reducer<Text, ArrayPrimitiveWritable, Text, DoublePercentWritable> {
        @Override
        public void reduce(Text key, Iterable<ArrayPrimitiveWritable> values, Context context) throws IOException, InterruptedException {
            if (key.toString().equals("#")) { // skip the total count as it is expected to be 100%
                return;
            }
            long sum = 0;
            long sumTotal = 0;
            for (ArrayPrimitiveWritable val : values) {
                long[] longValues =(long[]) val.get();
                sum += longValues[0];
                sumTotal += longValues[1];
            }
            DoublePercentWritable result = new DoublePercentWritable((double) sum / sumTotal);
            context.write(key, result);
        }
    }

}
