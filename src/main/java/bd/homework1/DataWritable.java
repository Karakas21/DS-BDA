package bd.homework1;
import java.io.*;
import org.apache.hadoop.io.*;
/**
  * Класс, содержащий выходные данные в следующем формате:
  * - Количество байтов, полученных с IP
  * - Количество запросов
  * - Среднее значение для IP
*/
public class DataWritable implements Writable {
    private IntWritable bytes;
    private IntWritable requests;
    private FloatWritable averageBytes;
    public DataWritable(){
        this.averageBytes = new FloatWritable(0.0f);
        this.bytes = new IntWritable(0);
        this.requests = new IntWritable(0);
    }
    public DataWritable(FloatWritable avg, IntWritable bytes, IntWritable reqs){
        this.averageBytes = avg;
        this.bytes = bytes;
        this.requests = reqs;
    }
    public FloatWritable getAverageBytes(){
        return this.averageBytes;
    }
    public IntWritable getSumBytes(){
        return this.bytes;
    }
    public IntWritable getRequestsCount(){
        return this.requests;
    }
    @Override
    public void write(DataOutput dataOut) throws IOException {
        bytes.write(dataOut);
        requests.write(dataOut);
        averageBytes.write(dataOut);
    }
    @Override
    public void readFields(DataInput dataIn) throws IOException {
        bytes.readFields(dataIn);
        requests.readFields(dataIn);
        averageBytes.readFields(dataIn);
    }

    @Override
    public int hashCode(){
        return bytes.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof DataWritable) {
            DataWritable other = (DataWritable) o;
            return  bytes.equals(other.bytes) &&
                    requests.equals(other.requests) &&
                    averageBytes.equals(other.averageBytes);
        }
        return false;
    }
    // toString() override is for formatted output line in .csv from self
    @Override
    public String toString(){
        return bytes.toString() + "," + requests.toString() + "," + averageBytes.toString();
    }

    // writes into self captured data from reducer
    public void set(IntWritable bytes, IntWritable requests, FloatWritable avg) {
        this.averageBytes = avg;
        this.bytes = bytes;
        this.requests = requests;
    }
}
