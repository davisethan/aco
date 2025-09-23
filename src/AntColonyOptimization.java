import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class AntColonyOptimization {
    public static void main(String[] args) throws Exception {
        // Validate input
        if (args.length != 9) {
            System.err.println(
                    "Usage: AntColonyOptimization <iterations> <input-path> <output-path> <graph-path> <graph-size> <alpha> <beta> <phi> <rho>");
            System.exit(-1);
        }

        // Assign arguments
        Integer iterations = Integer.parseInt(args[0]);
        String inputPath = args[1];
        String outputPath = args[2];
        String initialGraphPath = args[3];
        Integer graphSize = Integer.parseInt(args[4]);
        Double alpha = Double.parseDouble(args[5]);
        Double beta = Double.parseDouble(args[6]);
        Double phi = Double.parseDouble(args[7]);
        Double rho = Double.parseDouble(args[8]);
        String currentGraphPath = initialGraphPath;
        Double shortestDistance = Double.POSITIVE_INFINITY;

        // Start timer
        long startTime = System.currentTimeMillis();

        // Initialize job configurations
        Configuration conf = new Configuration();
        conf.setInt("parameter.graph.size", graphSize);
        conf.setDouble("parameter.alpha", alpha);
        conf.setDouble("parameter.beta", beta);
        conf.setDouble("parameter.phi", phi);
        conf.setDouble("parameter.rho", rho);
        conf.setDouble("parameter.shortest.distance", shortestDistance);

        // Iterate shortest path search
        for (int index = 0; index < iterations; index++) {
            // Initialize job
            Job job = Job.getInstance(conf, "Ant Colony Optimization");
            job.setJarByClass(AntColonyOptimization.class);
            job.setMapperClass(AntColonyOptimizationMapper.class);
            job.setCombinerClass(AntColonyOptimizationCombiner.class);
            job.setReducerClass(AntColonyOptimizationReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.addCacheFile(new Path(currentGraphPath).toUri());
            MultipleOutputs.addNamedOutput(job, "shortest", TextOutputFormat.class, Text.class, Text.class);
            MultipleOutputs.addNamedOutput(job, "graph", TextOutputFormat.class, Text.class, Text.class);
            TextInputFormat.addInputPath(job, new Path(inputPath));
            TextOutputFormat.setOutputPath(job, new Path(String.format("%s/%d", outputPath, index)));

            // Execute job
            job.waitForCompletion(true);

            // Print shortest path search progress
            System.out.printf("Iteration %d\n", index);
            String currentShortestPath = String.format("%s/%d/shortest-r-00000", outputPath, index);
            conf.set("parameter.shortest.path", currentShortestPath);
            findShortestPath(conf);

            // Prepare next job
            currentGraphPath = String.format("%s/%d/graph-r-00000", outputPath, index);
        }

        // End timer
        long endTime = System.currentTimeMillis();
        System.out.printf("Elapsed Time (ms): %d\n", endTime - startTime);
    }

    private static void findShortestPath(Configuration conf) throws IOException {
        // Read parameters
        Double shortestDistance = conf.getDouble("parameter.shortest.distance", 0.0);
        String currentShortestPath = conf.get("parameter.shortest.path", "");

        // Read graph from HDFS
        Path shortestPath = new Path(currentShortestPath);
        FileSystem fileSystem = FileSystem.get(conf);
        BufferedReader reader = new BufferedReader(new InputStreamReader(fileSystem.open(shortestPath)));
        String line = reader.readLine();
        reader.close();

        // Find shortest distance
        String[] content = line.split(",");
        String path = content[1];
        Double distance = Double.parseDouble(content[2]);

        // Print new shortest path
        if (distance < shortestDistance) {
            shortestDistance = distance;
            conf.setDouble("parameter.shortest.distance", shortestDistance);
            System.out.printf("New Shortest Path %s\n", path);
            System.out.printf("New Shortest Distance %f\n", shortestDistance);
        }
    }
}