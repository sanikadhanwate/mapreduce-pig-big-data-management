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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Query 3.3: For each (Age Range, Gender) group report
 * Age Range, Gender, MinTransTotal, MaxTransTotal, AvgTransTotal
 */
public class Query33 {

    public static class AgeGenderMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Map<Integer, String> customerGroup = new HashMap<>();
        private final Text outKey = new Text();
        private final Text outValue = new Text();

        private static String getAgeRange(int age) {
            if (age < 10) return "[10,20)";
            if (age < 20) return "[10,20)";
            if (age < 30) return "[20,30)";
            if (age < 40) return "[30,40)";
            if (age < 50) return "[40,50)";
            if (age < 60) return "[50,60)";
            return "[60,70]";
        }

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
                    // ID,Name,Age,Gender,CountryCode,Salary
                    String[] f = line.split(",", -1);
                    if (f.length != 6) {
                        continue;
                    }
                    try {
                        int customerId = Integer.parseInt(f[0]);
                        int age = Integer.parseInt(f[2]);
                        String gender = f[3];
                        String range = getAgeRange(age);
                        
                        customerGroup.put(customerId, range + "," + gender);
                    } catch (NumberFormatException e) {
                    }
                }
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // TransID,CustID,TransTotal,TransNumItems,TransDesc
            String[] f = value.toString().split(",", -1);
            if (f.length != 5) {
                return;
            }

            try {
                int customerId = Integer.parseInt(f[1]);
                String group = customerGroup.get(customerId);
                if (group == null) {
                    return;
                }

                double transactionTotal = Double.parseDouble(f[2]);

                outKey.set(group);
                outValue.set(transactionTotal + "," + transactionTotal + "," + transactionTotal + ",1");
                context.write(outKey, outValue);
            } catch (NumberFormatException e) {
            }
        }
    }

    public static class StatsCombiner extends Reducer<Text, Text, Text, Text> {
        private final Text outValue = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            double min = Double.MAX_VALUE;
            double max = -Double.MAX_VALUE;
            double sum = 0.0;
            long count = 0;

            for (Text v : values) {
                String[] p = v.toString().split(",");
                min = Math.min(min, Double.parseDouble(p[0]));
                max = Math.max(max, Double.parseDouble(p[1]));
                sum += Double.parseDouble(p[2]);
                count += Long.parseLong(p[3]);
            }
            outValue.set(min + "," + max + "," + sum + "," + count);
            context.write(key, outValue);
        }
    }

    public static class StatsReducer extends Reducer<Text, Text, Text, Text> {
        private final Text outValue = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            double min = Double.MAX_VALUE;
            double max = -Double.MAX_VALUE;
            double sum = 0.0;
            long count = 0;

            for (Text v : values) {
                String[] p = v.toString().split(",");
                min = Math.min(min, Double.parseDouble(p[0]));
                max = Math.max(max, Double.parseDouble(p[1]));
                sum += Double.parseDouble(p[2]);
                count += Long.parseLong(p[3]);
            }

            if (count == 0) {
                return;
            }

            double average = sum / count;

            outValue.set(String.format(Locale.US, "%.2f", min) + ","
                    + String.format(Locale.US, "%.2f", max) + ","
                    + String.format(Locale.US, "%.2f", average));
            
            context.write(key, outValue);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: Query33 <customers path> <transactions path> <output path>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", ",");

        Job job = Job.getInstance(conf, "Project1 Query 3.3 - Age Range and Gender Stats");
        job.setJarByClass(Query33.class);

        job.addCacheFile(new Path(args[0]).toUri());
        FileInputFormat.addInputPath(job, new Path(args[1]));

        job.setMapperClass(AgeGenderMapper.class);
        job.setCombinerClass(StatsCombiner.class);
        job.setReducerClass(StatsReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(1);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}