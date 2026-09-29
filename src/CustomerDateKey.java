import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

public class CustomerDateKey implements WritableComparable<CustomerDateKey> {
    private String customerId;
    private String statementDate;

    public CustomerDateKey() {
        this.customerId = "";
        this.statementDate = "";
    }

    public CustomerDateKey(String customerId, String statementDate) {
        this.customerId = customerId;
        this.statementDate = statementDate;
    }

    public String getCustomerId() { return customerId; }
    public String getStatementDate() { return statementDate; }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(customerId);
        out.writeUTF(statementDate);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.customerId = in.readUTF();
        this.statementDate = in.readUTF();
    }

    @Override
    public int compareTo(CustomerDateKey other) {
        int cmp = this.customerId.compareTo(other.customerId);
        if (cmp != 0) return cmp;
        return this.statementDate.compareTo(other.statementDate);
    }

    @Override
    public String toString() {
        return customerId + "\t" + statementDate;
    }
}
