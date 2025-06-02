import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PopularStationsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text stationName = new Text();
    // Indexes of the start and end station columns
    private static final int START_STATION_NAME_IDX = 4;
    private static final int END_STATION_NAME_IDX = 6;


    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();

        // Skip header row
        // Check header with first and last column name
        if (key.get() == 0 && line.contains("ride_id,rideable_type")) { 
            return;
        }

        String[] parts = line.split(",", -1); // -1 to keep trailing empty strings

        String startStation = parts[START_STATION_NAME_IDX].trim();
        String endStation = parts[END_STATION_NAME_IDX].trim();

        if (!startStation.isEmpty() && !startStation.equalsIgnoreCase("NULL") && !startStation.equalsIgnoreCase("NA")) {
            stationName.set(startStation);
            context.write(stationName, one);
        }

        if (!endStation.isEmpty() && !endStation.equalsIgnoreCase("NULL") && !endStation.equalsIgnoreCase("NA")) {
            stationName.set(endStation);
            context.write(stationName, one);
        }
    }
}
