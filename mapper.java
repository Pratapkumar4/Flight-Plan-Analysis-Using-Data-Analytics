import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AirlinePriceMapper extends Mapper<Object, Text, Text, DoubleWritable> {

    private Text airlineName = new Text();
    private DoubleWritable price = new DoubleWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split("\\|\\|");
        String airline = tokens[0];
        double priceValue = Double.parseDouble(tokens[1]);

        airlineName.set(airline);
        price.set(priceValue);

        context.write(airlineName, price);
    }
}
