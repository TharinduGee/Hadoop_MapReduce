import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.net.URI; 

public class BusiestHourDriver {
    public static void main(String[] args) throws Exception {
        // Consider the distributed cache as third argument
        if (args.length != 3) { 
            System.err.println("Usage: TopStationsBusiestTimeDriver <input path> <output path> <HDFS path to top_stations.txt>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Busiest Hour for Top Stations");


        job.addCacheFile(new URI(args[2] + "#top_stations.txt"));

        job.setJarByClass(BusiestHourDriver.class);
        job.setMapperClass(BusiestHourMapper.class);
        job.setCombinerClass(BusiestHourReducer.class);
        job.setReducerClass(BusiestHourReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1])); 

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}