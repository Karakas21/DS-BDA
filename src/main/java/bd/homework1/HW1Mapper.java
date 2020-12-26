package bd.homework1;

import com.sun.jersey.core.header.reader.HttpHeaderReader;
import com.sun.org.apache.xpath.internal.operations.Bool;
import eu.bitwalker.useragentutils.Browser;
import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.*;

/**
 * Mapper:
 * Анализирует строку из журнала, получает количество IP и байтов
 * Среднее количество IP-адресов, которые будет учитываться в Reducer.
 */
public class HW1Mapper extends Mapper<Object, Text, Text, DataWritable> {

    private Text word = new Text();
    private IntWritable bytes = new IntWritable(0);
    private IntWritable requests = new IntWritable(1);
    private FloatWritable avg = new FloatWritable(0.0f);
    private DataWritable data = new DataWritable();
    private static boolean isInt(String strNum) {
        try {
            int d = Integer.parseInt(strNum);
        } catch (NumberFormatException | NullPointerException nfe) {
            return false;
        }
        return true;
    }

    public static boolean verifyIP(String IP){
        boolean flag = true;
        String[] arr = IP.split("\\.");
        flag = (arr.length == 4);
        for(String i: arr){
            flag = flag && isInt(i);
        }
        return flag;
    }
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] arr = line.split(" ");
        try {
            String IP = arr[0];
            String bytes_count = arr[8];
            if(verifyIP(IP) && isInt(bytes_count)) {
                bytes.set(Integer.parseInt(bytes_count));
                requests.set(1);

                word.set(arr[0]);
                data.set(bytes, requests, avg);
                context.write(word, data);
            }
        }
        catch (Exception e) {
            System.out.println("ERROR: " + e.getMessage());
        }
    }
}