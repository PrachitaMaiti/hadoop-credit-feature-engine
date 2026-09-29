import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CustomerGroupingComparator extends WritableComparator {
    protected CustomerGroupingComparator() {
        super(CustomerDateKey.class, true);
    }

    @Override
    @SuppressWarnings("rawtypes")
    public int compare(WritableComparable w1, WritableComparable w2) {
        CustomerDateKey k1 = (CustomerDateKey) w1;
        CustomerDateKey k2 = (CustomerDateKey) w2;
        return k1.getCustomerId().compareTo(k2.getCustomerId());
    }
}
