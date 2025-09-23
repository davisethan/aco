import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class AntColonyOptimizationCombiner extends Reducer<Text, Text, Text, Text> {
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
        context.write(key, new Text(String.format("%s,%f", shortestPath, shortestDistance)));
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

        // Emit results
        context.write(key, new Text(String.format("%f,%f,%f", weight, tau, sigma)));
    }
}