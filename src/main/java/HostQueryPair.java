import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import javax.annotation.Nonnull;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;



public class HostQueryPair implements WritableComparable<HostQueryPair> {
    private Text first;
    private Text second;

    public HostQueryPair() {
        set(new Text(), new Text());
    }

    public HostQueryPair(String first, String second) {
        set(new Text(first), new Text(second));
    }

    public HostQueryPair(Text first, Text second) {
        set(first, second);
    }

    private void set(Text a, Text b) {
        first = a;
        second = b;
    }

    public Text getFirst() {
        return first;
    }

    public Text getSecond() {
        return second;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        first.write(out);
        second.write(out);
    }

    @Override
    public int compareTo(@Nonnull HostQueryPair obj) {
        int cmp = first.compareTo(obj.first);
        return (cmp == 0) ? second.compareTo(obj.second) : cmp;
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        first.readFields(dataInput);
        second.readFields(dataInput);
    }

    @Override
    public int hashCode() {
        return first.hashCode();
    }

    @Override
    public String toString() {
        return first + "\t" + second;
    }
}