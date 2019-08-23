import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

public class Seo extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int rc = ToolRunner.run(new Seo(), args);
        System.exit(rc);
    }

    @Override
    public int run(String[] args) throws Exception {
        String N = "0";
        if(args.length == 3){
            N = args[2];
        }
        Job job = GetJobConf(getConf(), args[0], args[1], N);

        return job.waitForCompletion(true) ? 0 : 1;
    }


    public static class SeoPartitioner extends Partitioner<HostQueryPair, IntWritable> {
        @Override
        public int getPartition(HostQueryPair key, IntWritable val, int numPartitions) {
            return Math.abs(key.getFirst().hashCode()) % numPartitions;
        }
    }

    public static class KeyComparator extends WritableComparator {
        protected KeyComparator() {
            super(HostQueryPair.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            return ((HostQueryPair)a).compareTo((HostQueryPair)b);
        }
    }

    public static class SeoGrouper extends WritableComparator {
        protected SeoGrouper() {
            super(HostQueryPair.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            Text a_first = ((HostQueryPair)a).getFirst();
            Text b_first = ((HostQueryPair)b).getFirst();
            return a_first.compareTo(b_first);
        }
    }

    public static String pair_to_string(String query, Integer i){
        return query + "\t" + i.toString();
    }


    public static class SeoMapper extends Mapper<LongWritable, Text, HostQueryPair, Text>
    {
        private static String get_Domain(String url) {
            url = url.replace("https://", "");
            url = url.replace("http://", "");
            url = url.replace(" ", "");
            url = url.replace("://", "");
            url = url.replace("www.", "");

            return url.split("/")[0];
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split("\t");
            context.write(new HostQueryPair(get_Domain(parts[1]), parts[0]), new Text(pair_to_string(parts[0], 1)));
        }
    }


    public static class SeoReducer extends Reducer<HostQueryPair, Text, Text, Text> {
        int N;

        @Override
        protected void setup(Reducer.Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            N = Integer.parseInt(conf.get("N_count"));
        }

        @Override
        protected void reduce(HostQueryPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Integer sum = 0;
            Integer max = 0;
            String query = "";
            String temp;
            String pair = "";
            String temp_query;
            int temp_count;

            for(Text value : values){
                temp = value.toString();
                temp_query = temp.split("\t")[0];
                temp_count = Integer.parseInt(temp.split("\t")[1]);
                if(pair.equals(temp_query)){
                    sum += temp_count;
                }else{
                   if(sum > max && sum >= N) {
                       max = sum;
                       query = pair;
                   }
                   sum = temp_count;
                   pair = temp_query;
                }

            }
            if(max != 0){
                context.write(key.getFirst(), new Text(query + "\t" + max.toString()));
            }
        }
    }

    Job GetJobConf(Configuration conf, String input, String out_dir, String N) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path outputPath = new Path(out_dir);

        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }
        conf.set("N_count", N);

        Job job = Job.getInstance(conf);
        job.setJarByClass(Seo.class);
        job.setJobName(Seo.class.getCanonicalName());

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(out_dir));

        job.setMapperClass(SeoMapper.class);
        job.setReducerClass(SeoReducer.class);

        job.setPartitionerClass(SeoPartitioner.class);
        job.setSortComparatorClass(KeyComparator.class);
        job.setGroupingComparatorClass(SeoGrouper.class);

        job.setMapOutputKeyClass(HostQueryPair.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job;
    }
}
