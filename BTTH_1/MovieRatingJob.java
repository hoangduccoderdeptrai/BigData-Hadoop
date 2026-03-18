import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MovieRatingJob {

    // ================== MAPPER ==================
    public static class RatingMapper extends Mapper<LongWritable, Text, IntWritable, FloatWritable>{

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();

            if(line.isEmpty()) return;

            String[] p = line.split(",");

            if(p.length < 3) return;

            try {
                int movieId = Integer.parseInt(p[1].trim());
                float rating = Float.parseFloat(p[2].trim());

                context.write(new IntWritable(movieId), new FloatWritable(rating));
            } catch(Exception e){
                // bỏ dòng lỗi
            }
        }
    }

    // ================== REDUCER ==================
    public static class RatingReducer extends Reducer<IntWritable, FloatWritable, IntWritable, Text>{

        String maxMovie = "";
        double maxRating = Double.MIN_VALUE;

        public void reduce(IntWritable key, Iterable<FloatWritable> values, Context context)
                throws IOException, InterruptedException {

            int count = 0;
            float sum = 0;

            for(FloatWritable v : values){
                sum += v.get();
                count++;
            }

            if(count == 0) return;

            double avg = sum / count;

            // in từng movie
            context.write(key, new Text("AverageRating:" + avg + " TotalRatings:" + count));

            // tìm max
            if(avg > maxRating){
                maxRating = avg;
                maxMovie = key.toString();
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException{
            context.write(new IntWritable(-1),
                    new Text("Highest rated movie ID:" + maxMovie + " rating:" + maxRating));
        }
    }

    // ================== MAIN ==================
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Movie Rating");

        job.setJarByClass(MovieRatingJob.class);
        job.setMapperClass(RatingMapper.class);
        job.setReducerClass(RatingReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(FloatWritable.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        // 🔥 QUAN TRỌNG
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}