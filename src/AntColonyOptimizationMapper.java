import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.URI;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class AntColonyOptimizationMapper extends Mapper<Object, Text, Text, Text> {
    private Double alpha;
    private Double beta;
    private Double phi;
    private Integer graphSize;
    private Graph graph;

    @Override
    protected void setup(Context context) throws IOException {
        // Get parameters
        Configuration conf = context.getConfiguration();
        alpha = conf.getDouble("parameter.alpha", 0.0);
        beta = conf.getDouble("parameter.beta", 0.0);
        phi = conf.getDouble("parameter.phi", 0.0);
        graphSize = conf.getInt("parameter.graph.size", 0);

        // Build graph
        // Read cached graph from local disk
        URI[] cacheFiles = context.getCacheFiles();
        Path graphPath = new Path(cacheFiles[0].getPath());
        BufferedReader reader = new BufferedReader(new FileReader(new File(graphPath.getName())));
        graph = new Graph(reader, graphSize);
        reader.close();
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Find path of ant
        List<Integer> path = graph.findPath(alpha, beta);
        Double distance = graph.findDistance(path);

        // Emit path
        Text content = new Text(String.format("%s,%f", stringifyPath(path), distance));
        context.write(new Text("shortest"), content);

        // Emit visited edges
        Set<String> visited = new HashSet<String>();
        for (int i = 1; i < graphSize; i++) {
            // Collect results
            Integer source = path.get(i - 1);
            Integer target = path.get(i);
            Double weight = graph.getWeight(source, target);
            Double tau = graph.getPheromone(source, target);
            Double delta = phi / distance;

            // Emit results
            String edge = String.format("%d,%d", source, target);
            context.write(new Text(edge), new Text(String.format("%f,%f,%f", weight, tau, delta)));
            visited.add(edge);
        }

        // Emit unvisited edges
        for (String edge : graph.getEdges()) {
            if (visited.contains(edge)) {
                continue;
            }

            // Collect results
            String[] split = edge.split(",");
            Integer source = Integer.parseInt(split[0]);
            Integer target = Integer.parseInt(split[1]);
            Double weight = graph.getWeight(source, target);
            Double tau = graph.getPheromone(source, target);
            Double delta = 0.0;

            // Emit results
            context.write(new Text(edge), new Text(String.format("%f,%f,%f", weight, tau, delta)));
        }
    }

    private String stringifyPath(List<Integer> path) {
        StringBuilder result = new StringBuilder();
        for (int vertex : path) {
            char content = vertex < 26 ? (char) ('A' + vertex) : (char) ('0' + (vertex - 26));
            result.append(content);
        }
        return result.toString();
    }
}