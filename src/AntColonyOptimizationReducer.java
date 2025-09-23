import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class AntColonyOptimizationReducer extends Reducer<Text, Text, Text, Text> {
    private MultipleOutputs<Text, Text> multipleOutputs;
    private Double rho;

    @Override
    protected void setup(Context context) throws IOException {
        // Get parameters
        Configuration conf = context.getConfiguration();
        rho = conf.getDouble("parameter.rho", 0.0);
        multipleOutputs = new MultipleOutputs<>(context);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        if (key.toString().equals("shortest")) {
            reduceShortest(key, values, context);
            return;
        }
        reduceGraph(key, values, context);
    }

    private void reduceShortest(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        // Find shortest path
        String shortestPath = "";
        Double shortestDistance = Double.POSITIVE_INFINITY;
        for (Text value : values) {
            String[] content = value.toString().split(",");
            String path = content[0];
            Double distance = Double.parseDouble(content[1]);
            if (distance < shortestDistance) {
                shortestPath = path;
                shortestDistance = distance;
            }
        }

        // Emit results
        Text result = new Text(String.format("%s,%f", shortestPath, shortestDistance));
        multipleOutputs.write("shortest", key, result, "shortest");
    }

    private void reduceGraph(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        // Combine pheromone deltas
        Double weight = 0.0;
        Double tau = 0.0;
        Double sigma = 0.0;
        for (Text value : values) {
            String[] split = value.toString().split(",");
            weight = Double.parseDouble(split[0]);
            tau = Double.parseDouble(split[1]);
            sigma += Double.parseDouble(split[2]);
        }
        tau = (1 - rho) * tau + sigma;

        // Emit results
        Text result = new Text(String.format("%f,%f", weight, tau));
        multipleOutputs.write("graph", key, result, "graph");
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
    }
}