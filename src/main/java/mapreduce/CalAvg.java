package mapreduce;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class CalAvg {

    public static class Map extends Mapper<Object, Text, Text, CustomAverageTuple> {
        private CustomAverageTuple averageTuple = new CustomAverageTuple();
        private Text departmentName = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String data = value.toString();
            String[] field = data.split(",", -1);
            double salary = 0;
            if (null != field && field.length == 9 && field[7].length() > 0) {
                salary = Double.parseDouble(field[7]);
                averageTuple.setAverage(salary);
                averageTuple.setCount(1);
                departmentName.set(field[3]);
                context.write(departmentName, averageTuple);
            }

        }
    }

    public static class Reduce extends Reducer<Text, CustomAverageTuple, Text, CustomAverageTuple> {
        private CustomAverageTuple result = new CustomAverageTuple();

        public void reduce(Text key, Iterable<CustomAverageTuple> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0;
            long count = 0;
            for (CustomAverageTuple customAverageTuple : values) {
                sum = sum + customAverageTuple.getAverage() * customAverageTuple.getCount();
                count = count + customAverageTuple.getCount();
            }
            result.setCount(count);
            result.setAverage(sum / count);
            context.write(new Text(key.toString()), result);
        }
    }

    public static void main(String[] args) {

    }
}
