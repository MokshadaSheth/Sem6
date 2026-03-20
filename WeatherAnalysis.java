import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WeatherAnalysis {

    // 1. MAPPER CLASS
    public static class WeatherMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            
            // Skip the header row
            if (line.contains("STATION") || line.trim().isEmpty()) return;

            // Split by comma (adjust to "\\s+" if your file is space-delimited)
            String[] columns = line.split(",");

            try {
                String stationId = columns[0].trim();
                String temp = columns[6].trim();  // TEMP
                String dewp = columns[8].trim();  // DEWP
                String wdsp = columns[16].trim(); // WDSP

                // Emit Station as Key, and values joined by comma
                context.write(new Text(stationId), new Text(temp + "," + dewp + "," + wdsp));
            } catch (Exception e) {
                // Skip malformed lines
            }
        }
    }

    // 2. REDUCER CLASS
    public static class WeatherReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double sumTemp = 0, sumDewp = 0, sumWdsp = 0;
            int count = 0;

            for (Text val : values) {
                String[] parts = val.toString().split(",");
                double t = Double.parseDouble(parts[0]);
                double d = Double.parseDouble(parts[1]);
                double w = Double.parseDouble(parts[2]);

                // Filter out NOAA missing data indicators (usually 999.9 or 99.9)
                if (t < 90.0 && d < 90.0 && w < 90.0) { 
                    sumTemp += t;
                    sumDewp += d;
                    sumWdsp += w;
                    count++;
                }
            }

            if (count > 0) {
                String result = String.format("AvgTemp: %.2f | AvgDewp: %.2f | AvgWind: %.2f", 
                                (sumTemp / count), (sumDewp / count), (sumWdsp / count));
                context.write(key, new Text(result));
            }
        }
    }

    // 3. DRIVER (Main Method)
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: WeatherAnalysis <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Weather Station Averages");
        
        job.setJarByClass(WeatherAnalysis.class);
        job.setMapperClass(WeatherMapper.class);
        job.setReducerClass(WeatherReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
