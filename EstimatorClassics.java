package mapper_reducer_hw2.mapper_reducer_hw2;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class EstimatorClassics {
    public static void main(String[] args) throws IOException {
        String pathToCsv = args[0];
        BufferedReader csvReader = new BufferedReader(new FileReader(pathToCsv));
        String row = null;
        double price_sum = 0;
        double price_count = 0;
        List<Double> values = new ArrayList<Double>();
        while ((row = csvReader.readLine()) != null) {
            String[] data = row.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*[^\\n]$)");
            try {
                double price = Double.parseDouble(data[9]);
                price_sum += price;
                price_count++;
                values.add(price);
            } catch (Exception e) {
            }
        }
        csvReader.close();
        double mean = price_sum / price_count;

        // The variance
        double variance = 0;
        for (int i = 0; i < values.size(); i++) {
            variance += Math.pow(values.get(i) - mean, 2);
        }
        variance /= price_count;

        System.out.println(mean);
        System.out.println(variance);
    }
}
