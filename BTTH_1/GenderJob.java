
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GenderJob {

    public static class MapperClass extends Mapper<LongWritable,Text,IntWritable,FloatWritable>{

        IntWritable movie=new IntWritable();
        FloatWritable rating=new FloatWritable();

        public void map(LongWritable key,Text value,Context context)
        throws IOException,InterruptedException{

            String[] p=value.toString().split(",");
            if(p.length<3) return;

            movie.set(Integer.parseInt(p[1]));
            rating.set(Float.parseFloat(p[2]));

            context.write(movie,rating);
        }
    }

    public static class ReducerClass extends Reducer<IntWritable,FloatWritable,IntWritable,Text>{

        public void reduce(IntWritable key,Iterable<FloatWritable> values,Context context)
        throws IOException,InterruptedException{

            float sum=0;
            int count=0;

            for(FloatWritable v:values){
                sum+=v.get();
                count++;
            }

            double avg=sum/count;

            context.write(key,new Text("Avg:"+avg));
        }
    }

    public static void main(String[] args) throws Exception{

        Configuration conf=new Configuration();
        Job job=Job.getInstance(conf,"Gender Analysis");

        job.setJarByClass(GenderJob.class);
        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        System.exit(job.waitForCompletion(true)?0:1);
    }
}
