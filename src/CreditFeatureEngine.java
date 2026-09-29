import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CreditFeatureEngine {

    public static class StatementMapper extends Mapper<LongWritable, Text, CustomerDateKey, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty() || line.startsWith("customer_ID")) {
                return;
            }

            String[] parts = line.split(",");
            if (parts.length >= 6) {
                String customerId = parts[0].trim();
                String statementDate = parts[1].trim();

                CustomerDateKey compositeKey = new CustomerDateKey(customerId, statementDate);
                String payload = parts[2].trim() + "," + parts[3].trim() + "," + parts[4].trim() + "," + parts[5].trim();
                context.write(compositeKey, new Text(payload));
            }
        }
    }

    public static class FeatureReducer extends Reducer<CustomerDateKey, Text, Text, Text> {
        @Override
        protected void reduce(CustomerDateKey key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            
            int statementCount = 0;
            int currentDelinquentStreak = 0;
            int maxDelinquentStreak = 0;
            
            double totalSpend = 0.0;
            double lastSpend = 0.0;
            double totalDebtToPayment = 0.0;

            for (Text val : values) {
                String[] fields = val.toString().split(",");
                double balance = Double.parseDouble(fields[0]);
                double spend = Double.parseDouble(fields[1]);
                double payment = Double.parseDouble(fields[2]);
                int delinquent = Integer.parseInt(fields[3]);

                statementCount++;
                totalSpend += spend;
                lastSpend = spend;

                if (delinquent == 1) {
                    currentDelinquentStreak++;
                    if (currentDelinquentStreak > maxDelinquentStreak) {
                        maxDelinquentStreak = currentDelinquentStreak;
                    }
                } else {
                    currentDelinquentStreak = 0;
                }

                double ratio = (payment > 0.0) ? (balance / payment) : (balance / 1.0);
                totalDebtToPayment += ratio;
            }

            double avgSpend = (statementCount > 0) ? (totalSpend / statementCount) : 0.0;
            double spendAcceleration = (avgSpend > 0.0) ? (lastSpend / avgSpend) : 1.0;
            double avgDebtRatio = (statementCount > 0) ? (totalDebtToPayment / statementCount) : 0.0;

            String featureVector = String.format("%d,%.4f,%.4f,%.4f,%d",
                    statementCount, avgSpend, spendAcceleration, avgDebtRatio, maxDelinquentStreak);

            context.write(new Text(key.getCustomerId()), new Text(featureVector));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: CreditFeatureEngine <input_path> <output_path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Credit Risk & Delinquency Feature Engine");
        job.setJarByClass(CreditFeatureEngine.class);

        job.setMapperClass(StatementMapper.class);
        job.setPartitionerClass(CustomerPartitioner.class);
        job.setGroupingComparatorClass(CustomerGroupingComparator.class);
        job.setReducerClass(FeatureReducer.class);

        job.setMapOutputKeyClass(CustomerDateKey.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
