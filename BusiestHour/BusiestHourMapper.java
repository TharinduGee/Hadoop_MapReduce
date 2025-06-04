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
            DateTimeFormatter.ofPattern("M/d/yyyy h:mm:ss a", Locale.ENGLISH);

     @Override
     protected void setup(Context context) throws IOException, InterruptedException {
          // Setup the top stations as distributed cash for the mapper
          URI[] cacheFiles = context.getCacheFiles();
          if (cacheFiles != null && cacheFiles.length > 0) {
               for (URI cacheFile : cacheFiles) {
                    // File name shoud be end with top_stations.txt
                    if (cacheFile.getPath().endsWith("top_stations.txt")) {
                         Path filePath = new Path(cacheFile.getPath());
                         BufferedReader reader = new BufferedReader(new FileReader(filePath.toString()));
                         String line;
                         while ((line = reader.readLine()) != null) {
                         topStations.add(line.trim());
                         }
                         reader.close();
                         break;
                    }
               }
          } else {
               throw new IOException("Distributed Cache file 'top_stations.txt' not found.");
          }
          if (topStations.isEmpty()){
               System.err.println("Top stations set is empty.");
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