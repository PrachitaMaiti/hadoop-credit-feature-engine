import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CustomerPartitioner extends Partitioner<CustomerDateKey, Text> {
    @Override
    public int getPartition(CustomerDateKey key, Text value, int numPartitions) {
        return (key.getCustomerId().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}
