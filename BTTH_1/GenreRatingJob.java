import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class GenreRatingJob {

    // ================== MAPPER ==================
    public static class MyMapper extends Mapper<LongWritable, Text, IntWritable, Text>{

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            String[] parts = line.split(",");

            // ratings file: userId,movieId,rating,timestamp
            if(parts.length == 4){
                try{
                    int movieId = Integer.parseInt(parts[1].trim());
                    String rating = parts[2].trim();

                    context.write(new IntWritable(movieId),
                            new Text("R|" + rating));
                }catch(Exception e){}
            }

            // movies file: movieId,title,genres
            else if(parts.length >= 3){
                try{
                    int movieId = Integer.parseInt(parts[0].trim());
                    String genres = parts[parts.length - 1].trim();

                    context.write(new IntWritable(movieId),
                            new Text("M|" + genres));
                }catch(Exception e){}
            }
        }
    }

    // ================== REDUCER ==================
    public static class MyReducer extends Reducer<IntWritable, Text, Text, Text>{

        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String genres = "";
            double sum = 0;
            int count = 0;

            for(Text val : values){
                String v = val.toString();

                if(v.startsWith("M|")){
                    genres = v.substring(2);
                }
                else if(v.startsWith("R|")){
                    try{
                        sum += Double.parseDouble(v.substring(2));
                        count++;
                    }catch(Exception e){}
                }
            }

            if(count == 0 || genres.equals("")) return;

            double avg = sum / count;

            String[] genreList = genres.split("\\|");

            for(String g : genreList){
                context.write(new Text(g.trim()),
                        new Text(avg + "," + count));
            }
        }
    }

    // ================== FINAL REDUCER ==================
    public static class FinalReducer extends Reducer<Text, Text, Text, Text>{

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            double totalSum = 0;
            int totalCount = 0;

            for(Text val : values){
                try{
                    String[] parts = val.toString().split(",");
                    double avg = Double.parseDouble(parts[0]);
                    int count = Integer.parseInt(parts[1]);

                    totalSum += avg * count;
                    totalCount += count;
                }catch(Exception e){}
            }

            if(totalCount == 0) return;

            double finalAvg = totalSum / totalCount;

            context.write(key,
                    new Text("AverageRating:" + finalAvg + " (" + totalCount + ")"));
        }
    }

    // ================== MAIN ==================
    public static void main(String[] args) throws Exception {

        // 🔥 FIX LỖI args
        if (args.length < 2) {
            System.err.println("Usage: GenreRatingJob <input> <output>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Genre Rating");

        job.setJarByClass(GenreRatingJob.class);

        job.setMapperClass(MyMapper.class);

        // ⚠️ DÙNG FINAL REDUCER
        job.setReducerClass(FinalReducer.class);

        // ⚠️ KHÔNG dùng combiner ở bài này
        // job.setCombinerClass(MyReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}