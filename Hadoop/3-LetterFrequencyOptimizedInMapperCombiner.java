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
import java.util.EnumMap;
import java.util.Map;

public class LetterFrequencyOptimizedInMapper {

    private enum Letter {
        a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z
    }

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
        job.setJarByClass(LetterFrequencyOptimizedInMapper.class);
        job.setMapperClass(LetterFrequencyMapper.class);
        job.setMapOutputValueClass(ArrayPrimitiveWritable.class);
        job.setReducerClass(LetterFrequencyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoublePercentWritable.class);
        job.setNumReduceTasks(args.length > 2 ? Integer.parseInt(args[2]) : 1);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class LetterFrequencyMapper extends Mapper<Object, Text, Text, ArrayPrimitiveWritable> {
        private final EnumMap<Letter, Long> letterCountMap = new EnumMap<>(Letter.class);
        private long totalLetters;

        @Override
        protected void setup(Context context) {
            totalLetters = 0;
            for (Letter letter : Letter.values()) {
                letterCountMap.put(letter, 0L);
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            for (char c : line.toCharArray()) {
                if (c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z') {
                    c = Character.toLowerCase(c);
                    Letter letter = Letter.valueOf(String.valueOf(c).toLowerCase());
                    letterCountMap.put(letter, letterCountMap.get(letter) + 1);
                    totalLetters++;
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<Letter, Long> entry : letterCountMap.entrySet()) {
                Text letter = new Text(entry.getKey().name());
                ArrayPrimitiveWritable values = new ArrayPrimitiveWritable(new long[]{
                        entry.getValue(),
                        totalLetters
                });
                context.write(letter, values);
            }
        }
    }

    public static class LetterFrequencyReducer extends Reducer<Text, ArrayPrimitiveWritable, Text, DoublePercentWritable> {
        @Override
        public void reduce(Text key, Iterable<ArrayPrimitiveWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            long sumTotal = 0;
            for (ArrayPrimitiveWritable val : values) {
                long[] floatValues = (long[]) val.get();
                sum += floatValues[0];
                sumTotal += floatValues[1];
            }
            DoublePercentWritable result = new DoublePercentWritable((double) sum / sumTotal);
            context.write(key, result);
        }
    }
}
