import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Percentage {
    private static int counter = 0; // Initilize a counter to calculate the number of time the mapper was called

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String [] row = value.toString().split(";"); // Split the data into Rows
            String [] mf = row[1].split(","); // Split the second row (m,f) to count
            for(int i =0; i<mf.length; i++){
                word.set(mf[i]); // Count the number of female and male
                context.write(word, one);
            }
            counter++; // Count the number of time the mapper was called
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            int per = 0; // Initialize the percentage to zero
            for (IntWritable val : values) {
                sum += val.get(); // Reduce to count the number of m and f
            }

            if (counter>0){ // if statement to be sure that we are not dividing by zero
                per = sum*100/counter; // Compute the percentage
                result.set(per);
            }else{
                result.set(sum); // If the counter = 0 we return the number of f and m, which is 0
            }
            context.write(key, result);


        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(Percentage.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}