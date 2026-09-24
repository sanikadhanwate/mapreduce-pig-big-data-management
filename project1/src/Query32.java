import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Query 3.2: For every country code report
 * CountryCode, NumberOfCustomers, MinTransTotal, MaxTransTotal
 * Done in ONE map-reduce job: the small customers file is also shipped to every
 * transaction mapper through the distributed cache (map-side lookup of country).
 */
public class Query32 {
    public static class CustomerMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        private final IntWritable outKey = new IntWritable();
        private final Text outValue = new Text("C,1");

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] f = value.toString().split(",", -1);
            if (f.length != 6) {
                return;
            }
            outKey.set(Integer.parseInt(f[4]));
            context.write(outKey, outValue);
        }
    }

    public static class TransactionMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        private final Map<Integer, Integer> customerToCountry = new HashMap<>();
        private final IntWritable outKey = new IntWritable();
        private final Text outValue = new Text();

        @Override
        protected void setup(Context context) throws IOException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles == null || cacheFiles.length == 0) {
                throw new IOException("Customers file not found in distributed cache");
            }
            FileSystem fs = FileSystem.get(context.getConfiguration());
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                    fs.open(new Path(cacheFiles[0])), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] f = line.split(",", -1);
                    if (f.length == 6) {
                        customerToCountry.put(Integer.parseInt(f[0]), Integer.parseInt(f[4]));
                    }
                }
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] f = value.toString().split(",", -1);
            if (f.length != 5) {
                return;
            }
            Integer country = customerToCountry.get(Integer.parseInt(f[1]));
            if (country == null) {
                return;
            }
            outKey.set(country);
            outValue.set("T," + f[2] + "," + f[2]);
            context.write(outKey, outValue);
        }
    }

    public static class CountryCombiner extends Reducer<IntWritable, Text, IntWritable, Text> {
        private final Text outValue = new Text();

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            long customers = 0;
            boolean hasTrans = false;
            double min = Double.MAX_VALUE;
            double max = -Double.MAX_VALUE;

            for (Text v : values) {
                String[] p = v.toString().split(",");
                if (p[0].equals("C")) {
                    customers += Long.parseLong(p[1]);
                } else {
                    hasTrans = true;
                    min = Math.min(min, Double.parseDouble(p[1]));
                    max = Math.max(max, Double.parseDouble(p[2]));
                }
            }
            if (customers > 0) {
                outValue.set("C," + customers);
                context.write(key, outValue);
            }
            if (hasTrans) {
                outValue.set("T," + min + "," + max);
                context.write(key, outValue);
            }
        }
    }

    public static class CountryReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        private final Text outValue = new Text();

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            long customers = 0;
            boolean hasTrans = false;
            double min = Double.MAX_VALUE;
            double max = -Double.MAX_VALUE;

            for (Text v : values) {
                String[] p = v.toString().split(",");
                if (p[0].equals("C")) {
                    customers += Long.parseLong(p[1]);
                } else {
                    hasTrans = true;
                    min = Math.min(min, Double.parseDouble(p[1]));
                    max = Math.max(max, Double.parseDouble(p[2]));
                }
            }
            if (!hasTrans) {
                min = 0.0;
                max = 0.0;
            }
            outValue.set(customers + ","
                    + String.format(Locale.US, "%.2f", min) + ","
                    + String.format(Locale.US, "%.2f", max));
            context.write(key, outValue);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: Query32 <customers path> <transactions path> <output path>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", ",");

        Job job = Job.getInstance(conf, "Query3.2 - Customers and Min/Max TransTotal per Country");
        job.setJarByClass(Query32.class);

        job.addCacheFile(new Path(args[0]).toUri());

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CustomerMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TransactionMapper.class);

        job.setCombinerClass(CountryCombiner.class);
        job.setReducerClass(CountryReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}