import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import java.util.Locale;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Query 1: For each customer report
 * CustomerID, Name, Salary, NumOfTransactions, TotalSum, MinItems
 * Reduce-side join on CustomerID.
 */
public class Query31 {
    public static class CustomerMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        private final IntWritable outKey = new IntWritable();
        private final Text outValue = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] f = value.toString().split("," , -1);
            if (f.length != 6) {
                return;
            }
            outKey.set(Integer.parseInt(f[0]));
            outValue.set("C," + f[1] + "," + f[5]);
            context.write(outKey, outValue);
        }
    }

    public static class TransactionMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        private final IntWritable outKey = new IntWritable();
        private final Text outValue = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] f = value.toString().split(",", -1);
            if (f.length != 5) {
                return;
            }
            outKey.set(Integer.parseInt(f[1]));
            outValue.set("T,1," + f[2] + "," + f[3]);
            context.write(outKey, outValue);
        }
    }

    public static class JoinCombiner extends Reducer<IntWritable, Text, IntWritable, Text> {
        private final Text outValue = new Text();

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            long count = 0;
            double sum = 0.0;
            int minItems = Integer.MAX_VALUE;

            for (Text v : values) {
                String s = v.toString();
                if (s.startsWith("C,")) {
                    context.write(key, v);
                } else {
                    String[] p = s.split(",");
                    count += Long.parseLong(p[1]);
                    sum += Double.parseDouble(p[2]);
                    minItems = Math.min(minItems, Integer.parseInt(p[3]));
                }
            }
            if (count > 0) {
                outValue.set("T," + count + "," + sum + "," + minItems);
                context.write(key, outValue);
            }
        }
    }

    public static class JoinReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        private final Text outValue = new Text();

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String name = null;
            String salary = null;
            long count = 0;
            double sum = 0.0;
            int minItems = Integer.MAX_VALUE;

            for (Text v : values) {
                String[] p = v.toString().split(",");
                if (p[0].equals("C")) {
                    name = p[1];
                    salary = p[2];
                } else {
                    count += Long.parseLong(p[1]);
                    sum += Double.parseDouble(p[2]);
                    minItems = Math.min(minItems, Integer.parseInt(p[3]));
                }
            }

            if (name == null) {
                return; 
            }
            if (count == 0) {
                minItems = 0;
            }

            outValue.set(name + "," + salary + "," + count + ","
                    + String.format( Locale.US, "%.2f", sum) + "," + minItems);
            context.write(key, outValue);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: Query1 <customers path> <transactions path> <output path>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", ",");

        Job job = Job.getInstance(conf, "Query31 - Customer/Transaction Join");
        job.setJarByClass(Query31.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CustomerMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TransactionMapper.class);

        job.setCombinerClass(JoinCombiner.class);
        job.setReducerClass(JoinReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}