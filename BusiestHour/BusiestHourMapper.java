import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.time.DayOfWeek;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

public class BusiestHourMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

     private final static IntWritable one = new IntWritable(1);
     private Text outputKey = new Text();
     private Set<String> topStations = new HashSet<>();

     // Indexes of the start time and start station columns
     private static final int STARTED_AT_IDX = 2;
     private static final int START_STATION_NAME_IDX = 4;
     private static final DateTimeFormatter CSV_DATE_TIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

     @Override
     protected void setup(Context context) throws IOException, InterruptedException {
        URI[] cacheFiles = context.getCacheFiles(); // Get all cache file URIs
        if (cacheFiles != null && cacheFiles.length > 0) {
            for (URI cacheFileUri : cacheFiles) {
                String symlinkName = new Path(cacheFileUri).getName();

                // Use the URI fragment directly if available and what we expect
                String fragment = cacheFileUri.getFragment();
                if (fragment != null && fragment.equals("top_stations.txt")) {
                    symlinkName = fragment;
                } else if (!symlinkName.equals("top_stations.txt")) {
                    // This check helps if multiple files are in cache.
                    System.out.println("Inspecting cache file URI: " + cacheFileUri
                     + ", symlinkName based on path: " + new Path(cacheFileUri).getName() + ", fragment: " + fragment);
                    if (! (new Path(cacheFileUri).getName().equals("top_stations.txt") 
                    || (fragment != null && fragment.equals("top_stations.txt")) ) ) {
                        continue;
                    }
                    if (fragment == null) {
                        symlinkName = new Path(cacheFileUri).getName();
                    }
                }


                System.out.println("Attempting to read from cached file (symlink): " + symlinkName);
                BufferedReader reader = new BufferedReader(new FileReader(symlinkName)); 
                String line;
                while ((line = reader.readLine()) != null) {
                    topStations.add(line.trim());
                    System.out.println("Loaded from cache: " + line.trim()); 
                }
                reader.close();
                System.out.println("Finished reading from symlink: " + symlinkName);
                break; 
            }
        } else {
            throw new IOException("No files found in Distributed Cache.");
        }

        if (topStations.isEmpty()) {
            System.err.println("Error: Top stations set is empty.");
            throw new IOException("Error: Top stations set is empty.");
        } else {
            System.out.println("Loaded " + topStations.size() + " top stations from cache.");
        }
     }

     @Override
     protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
          String line = value.toString();

          // Skip header row
          // Check header with first and last column name
          if (key.get() == 0 && line.contains("ride_id,rideable_type")) {
               return;
          }

          // -1 to keep trailing empty strings
          String[] parts = line.split(",", -1);

          String startStation = parts[START_STATION_NAME_IDX].trim();

          // Only process if this station is one of the top stations
          if (topStations.contains(startStation)) {
               String startedAtStr = parts[STARTED_AT_IDX].trim().replace("\"", "");
               try {
                    LocalDateTime startedAt = LocalDateTime.parse(startedAtStr, CSV_DATE_TIME_FORMATTER);
                    DayOfWeek day = startedAt.getDayOfWeek();
                    int hour = startedAt.getHour();

                    String mapOutputKey = String.format("%s_%s_%02d", startStation, day.toString(), hour);
                    outputKey.set(mapOutputKey);
                    context.write(outputKey, one);

               } catch (DateTimeParseException e) {
                    System.err.println("DateTime format error : " + startedAtStr + " at line: " + line);
               }
          }
    }
}