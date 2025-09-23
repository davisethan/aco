import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Graph {
    private Double[][] weights;
    private Double[][] pheromones;

    public Graph(BufferedReader graphBufferedReader, Integer graphSize) throws IOException {
        // Initialize graph
        weights = new Double[graphSize][graphSize];
        pheromones = new Double[graphSize][graphSize];
        String line;
        while ((line = graphBufferedReader.readLine()) != null) {
            // Find values
            String[] split = line.split(",");
            Integer source = Integer.parseInt(split[0]);
            Integer target = Integer.parseInt(split[1]);
            Double weight = Double.parseDouble(split[2]);
            Double pheromone = Double.parseDouble(split[3]);

            // Insert values
            weights[source][target] = weight;
            weights[target][source] = weight;
            pheromones[source][target] = pheromone;
            pheromones[source][target] = pheromone;
        }
    }

    public List<Integer> findPath(Double alpha, Double beta) {
        List<Integer> path = new ArrayList<>();
        Set<Integer> visited = new HashSet<>();
        Random random = new Random();
        Integer graphSize = weights.length;

        // Find initial vertex
        Integer vertex = random.nextInt(graphSize);
        path.add(vertex);
        visited.add(vertex);

        // Find remaining vertexes
        while (path.size() < graphSize) {
            // Get unvisited neighbors
            List<Integer> neighbors = IntStream.range(0, graphSize)
                    .filter(v -> !visited.contains(v))
                    .boxed()
                    .collect(Collectors.toList());

            // Find attractiveness of edge to unvisited neighbors
            Integer content = vertex;
            List<Double> attractiveness = neighbors.stream()
                    .mapToDouble(n -> Math.pow(pheromones[content][n], alpha) * Math.pow(1 / weights[content][n], beta))
                    .boxed()
                    .collect(Collectors.toList());

            // Find sum of attractiveness
            Double sum = attractiveness.stream().mapToDouble(Double::doubleValue).sum();

            // Find probability of each edge
            List<Double> probabilities = attractiveness.stream()
                    .mapToDouble(a -> a / sum)
                    .boxed()
                    .collect(Collectors.toList());

            // Find cumulative sum of probabilities
            List<Double> cdf = new ArrayList<>();
            probabilities.stream()
                    .reduce(0.0, (acc, cur) -> {
                        cdf.add(acc + cur);
                        return acc + cur;
                    });

            // Find next vertex
            Double target = random.nextDouble();
            Integer index = IntStream.range(0, cdf.size())
                    .filter(i -> target <= cdf.get(i))
                    .findFirst()
                    .orElse(-1);
            vertex = neighbors.get(index);
            path.add(vertex);
            visited.add(vertex);
        }

        return path;
    }

    public Double findDistance(List<Integer> path) {
        return IntStream.range(1, path.size())
                .mapToDouble(i -> {
                    Integer source = path.get(i - 1);
                    Integer target = path.get(i);
                    return weights[source][target];
                })
                .sum();
    }

    public List<String> getEdges() {
        Integer graphSize = weights.length;
        return IntStream.range(0, graphSize)
                .boxed()
                .flatMap(row -> IntStream.range(0, graphSize)
                        .mapToObj(col -> row == col ? "" : String.format("%d,%d", row, col)))
                .filter(edge -> !edge.isEmpty())
                .collect(Collectors.toList());
    }

    public Double getWeight(Integer source, Integer target) {
        return weights[source][target];
    }

    public Double getPheromone(Integer source, Integer target) {
        return pheromones[source][target];
    }
}