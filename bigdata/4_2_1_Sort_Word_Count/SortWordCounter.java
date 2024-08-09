import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;

public class SortWordCounter {

    public static class TokenMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text text = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                text.set(itr.nextToken());
                context.write(text, one);
            }
        }
    }

    public static class CountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private List<CountPair> counts = new ArrayList<>();

        // Inner class to hold key-value pairs and implement Comparable
        private static class CountPair implements Comparable<CountPair> {
            Text key;
            int count;

            CountPair(Text key, int count) {
                this.key = key;
                this.count = count;
            }

            @Override
            public int compareTo(CountPair o) {
                return Integer.compare(o.count, this.count); // Descending order
            }
        }

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            counts.add(new CountPair(new Text(key), sum));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Sort the list in descending order by count
            Collections.sort(counts);

            // Output the sorted results
            for (CountPair pair : counts) {
                context.write(pair.key, new IntWritable(pair.count));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration cf = new Configuration();
        Job job = Job.getInstance(cf, "string count");
        job.setJarByClass(SortWordCounter.class);

        job.setMapperClass(TokenMapper.class);
        job.setReducerClass(CountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

