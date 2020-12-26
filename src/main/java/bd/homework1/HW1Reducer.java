package bd.homework1;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer:
 * - складывает все байты из Mapper, сумма вывода
 * - складывает количество всех запросов, полученных от маппера, вычитая общее количество запросов с конкретного IP
 * - среднее количество байтов, полученных с конкретного IP
 */

public class HW1Reducer extends Reducer<Text, DataWritable, Text, DataWritable> {
    private IntWritable rBytes = new IntWritable();
    private IntWritable rRequests = new IntWritable();
    private FloatWritable averageBytes = new FloatWritable();
    private DataWritable data = new DataWritable();

    @Override
    public void reduce(Text key, Iterable<DataWritable> values, Context context) throws IOException, InterruptedException {
        int sumBytes = 0;
        int requestsCount = 0;

        for(DataWritable i : values){
            sumBytes += i.getSumBytes().get();
            requestsCount += i.getRequestsCount().get();
        }
        float avg_bytes = 0;

        if(requestsCount != 0) {
            avg_bytes = (float) sumBytes / requestsCount;
        }
        else{
            avg_bytes = 0.0f;
        }

        rBytes.set(sumBytes);
        rRequests.set(requestsCount);
        averageBytes.set(avg_bytes);

        data.set(rBytes, rRequests, averageBytes);
        context.write(key, data);
    }
}
