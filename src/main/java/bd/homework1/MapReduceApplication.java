package bd.homework1;

import lombok.extern.log4j.Log4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;


@Log4j
public class MapReduceApplication {

    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            throw new RuntimeException("You should specify input and output folders! Optionally num of reducers in 3 arg");
        }

        // specifies output format
        Configuration conf = new Configuration();
        conf.set("mapreduce.output.SequenseFileoutputformat.separator", ",");

        // let's job to know about classes of it's parts such as mapper, reducer and others
        int reducersCount = 1;
        if (args.length >= 3) {
            int parsedInt = Integer.parseInt(args[2]);
            reducersCount = parsedInt;
        }
        Job job = Job.getInstance(conf, "Bytes stats");
        job.setJarByClass(DataWritable.class);
        job.setMapperClass(HW1Mapper.class);
        job.setReducerClass(HW1Reducer.class);
        job.setNumReduceTasks(reducersCount);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DataWritable.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        // set input & output directory with paths entered in args
        Path outputDirectory = new Path(args[1]);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, outputDirectory);

        log.info("=====================JOB STARTED=====================");
        long startingTime = System.currentTimeMillis();
        job.waitForCompletion(true);
        long endingTime = System.currentTimeMillis() - startingTime;
        log.info("=====================JOB ENDED=====================");
        log.info("It took " + String.valueOf(endingTime) + "time"); // prints overall time
    }
}
