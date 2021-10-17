package mapper_reducer_hw2.mapper_reducer_hw2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;


import java.io.IOException;
import java.util.Iterator;

public class MeanEstimatorMR {
    //решил сделать текст, потому что собственный writable класс для передачи массива так легко не завёлся
    public static class MeanChunkMapper extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //сплитим по переводу строки - написал это на случай если начну передавать чанки из нескольких строк
            String[] rows = value.toString().split("\\r?\\n");
            double priceSum = 0;
            double priceCount = 0;
            for (String row : rows) {
                //обработка одной строки
                //вошедшую строку разбиваем по запятым
                String[] fields = row.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*[^\\n]$)");
                //Скипаем если заголовок или баганная строка
                if (fields.length < 9 || fields[0].equals("id")) {
                    return;
                } else { //нормальные строки обрабатываем
                    double price = Double.parseDouble(fields[9]);
                    priceSum+=price;
                    priceCount++;
                }
            }
            //выводим инфу по цене
            Text wordOut = new Text("price");
            Text output = new Text(priceSum+" "+ priceCount);
            context.write(wordOut, output);
        }
    }

    public static class MeanReducer extends Reducer<Text, Text, Text, DoubleWritable> {
        public void reduce(Text field, Iterable<Text> prices, Context context) throws IOException, InterruptedException{
            double top = 0;
            double bottom = 0;
            Iterator<Text> iterator = prices.iterator();

            while(iterator.hasNext()){
                String[] elements = iterator.next().toString().split(" ");
                top+= Double.parseDouble(elements[0]) * Double.parseDouble(elements[1]);//сумму умножаем на количество, добавляем к числителю
                bottom+=Double.parseDouble(elements[1]); //количество учитываем в знаменателе
            }
            //выводим результат
            double result = top/bottom;
            DoubleWritable output = new DoubleWritable(result);
            context.write(field, output);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length!=2){
            System.err.println("Usage: WordCount <input_file>, <output_file>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "Word Count");
        job.setJarByClass(MeanEstimatorMR.class);
        job.setMapperClass(MeanChunkMapper.class);
        job.setReducerClass(MeanReducer.class);
        job.setNumReduceTasks(1);

        //input
        FileInputFormat.addInputPath(job,new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        boolean status = job.waitForCompletion(true);

        if (status){
            System.exit(0);
        }
        else{
            System.exit(1);
        }
    }
}
