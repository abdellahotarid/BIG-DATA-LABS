package edu.supmti.hadoop.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WCMapper extends Mapper<Object, Text, Text, IntWritable> {

    public void map(Object key, Text value, Context context)
            throws java.io.IOException, InterruptedException {
        // récuperer la ligne
        String line = value.toString();
        // séparer les mots fragments
        String[] w = line.split(" ");

        for (String word : w) {
            // écrir le mot avec la valeur
            context.write(new Text(word), new IntWritable(1));
        }
    }
}
