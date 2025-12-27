package edu.supmti.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        // initilisation du compteur
        int sum = 0;
        // parcourir les valeurs et additionner
        for (IntWritable val : values) {
            sum += val.get();
        }
        // écrir le mot avec son nombre d'occurrences la valeur
        context.write(key, new IntWritable(sum));
    }
}