package mapper_reducer_hw2.mapper_reducer_hw2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class VarEstimatorMR {
    //решил сделать текст, потому что собственный writable класс для передачи массива так легко не завёлся
    public static class VarChunkMapper extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //сплитим по переводу строки - написал это на случай если начну передавать чанки из нескольких строк
            String[] rows = value.toString().split("\\r?\\n");
            double priceSum = 0;
            double priceCount = 0;
            List<Double> values = new ArrayList<Double>();
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
                    values.add(price);
                }
            }

            double mean = priceSum/priceCount;

            // The variance
            double variance = 0;
            for (int i = 0; i < values.size(); i++) {
                variance += Math.pow(values.get(i) - mean, 2);
            }
            variance /= priceCount;

            //выводим инфу по цене
            Text wordOut = new Text("price");
            Text output = new Text(priceSum+" "+ priceCount + " "+ variance); //s c v
            context.write(wordOut, output);
        }
    }

    public static class VarReducer extends Reducer<Text, Text, Text, DoubleWritable> {
        public void reduce(Text field, Iterable<Text> prices, Context context) throws IOException, InterruptedException{

            double v_j = 0;
            double c_j=0;
            double m_j=0;

            Iterator<Text> iterator = prices.iterator();

            while(iterator.hasNext()){
                String[] elements = iterator.next().toString().split(" ");
                double m_k = Double.parseDouble(elements[0])/Double.parseDouble(elements[1]);
                double c_k = Double.parseDouble(elements[1]);
                double v_k = Double.parseDouble(elements[2]);

                double v_i_first = (c_j*v_j + c_k*v_k)/(c_j+c_k);
                double v_i_second = c_j*c_k*Math.pow((m_j-m_k)/(c_j+c_k), 2);
                double v_i = v_i_first + v_i_second;

                double m_i = (c_j*m_j +c_k*m_k)/(c_j+c_k);

                v_j = v_i;
                m_j = m_i;
                c_j+=c_k;
            }

            //выводим результат
            DoubleWritable output = new DoubleWritable(v_j);
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
        job.setJarByClass(VarEstimatorMR.class);
        job.setMapperClass(VarChunkMapper.class);
        job.setReducerClass(VarReducer.class);
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
