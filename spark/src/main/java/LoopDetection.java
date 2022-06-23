import com.google.common.collect.Lists;
import com.google.common.io.Files;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.*;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.Tuple3;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.reflect.ClassTag$;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;
import scala.runtime.AbstractFunction3;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;


public class LoopDetection {
    static String HADOOP_COMMON_PATH = "C:\\Users\\Victor\\Development\\BDMA\\UPC\\SDM\\DistributedGraphsLab\\src\\main\\resources";

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", HADOOP_COMMON_PATH);

        SparkConf conf = new SparkConf().setAppName("SparkGraphs_II").setMaster("local[*]");
        JavaSparkContext ctx = new JavaSparkContext(conf);
        ctx.setCheckpointDir(Files.createTempDir().getAbsolutePath());

        SQLContext sqlCtx = new SQLContext(ctx);

        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.storage.BlockManager").setLevel(
                Level.ERROR);

        runAlgorithm(ctx, sqlCtx);
    }

    private static void runAlgorithm(JavaSparkContext ctx, SQLContext sqlCtx) {
        JavaRDD<Tuple2<Object, Tuple2<ArrayList, ArrayList>>> V = createVertexDataset(ctx, sqlCtx);
        JavaRDD<Edge<Tuple2<Float, Integer>>> E = createEdgeDataset(ctx, sqlCtx);
        Graph<Tuple2<ArrayList, ArrayList>, Tuple2<Float, Integer>> G = Graph.apply(
                V.rdd(),
                E.rdd(),
                new Tuple2<>(Lists.newArrayList(), Lists.newArrayList()),
                StorageLevel.MEMORY_ONLY(),
                StorageLevel.MEMORY_ONLY(),
                ClassTag$.MODULE$.apply(Object.class),
                ClassTag$.MODULE$.apply(Tuple2.class));

        GraphOps<Tuple2<ArrayList, ArrayList>, Tuple2<Float, Integer>> ops = new GraphOps<>(
                G,
                ClassTag$.MODULE$.apply(Tuple2.class),
                ClassTag$.MODULE$.apply(Tuple2.class)
        );

        ArrayList<ArrayList<Tuple3<Long, Float, Integer>>> initialMessage = Lists.newArrayList();
        initialMessage.add(Lists.newArrayList(new Tuple3<>(0L, 0.0F, 0)));
        List<Tuple2<Object, Tuple2<ArrayList, ArrayList>>> loops = ops.pregel(
                        initialMessage,
                        Integer.MAX_VALUE,
                        EdgeDirection.Out(),
                        new VProg(),
                        new sendMsg(),
                        new merge(),
                        ClassTag$.MODULE$.apply(ArrayList.class)
                )
                .vertices()
                .toJavaRDD()
                .collect();
        for (Tuple2<Object, Tuple2<ArrayList, ArrayList>> node : loops) {
            for (ArrayList<Tuple3<Long, Float, Integer>> path : (ArrayList<ArrayList<Tuple3<Long, Float, Integer>>>) node._2._2) {
                for (Tuple3<Long, Float, Integer> step : path.stream().skip(1).collect(Collectors.toList())) {
                    System.out.print(longToCode(step._1()) + " (" + step._2() + ", " + step._3() + ") -> ");
                }
                System.out.println(longToCode((Long) node._1));
            }
        }
    }


    private static Long codeToLong(String code) {
        char[] chars = code.toCharArray();
        long value = 0L;
        for (char c : chars) {
            value *= 100L;
            value += ((int) c - (int) 'A');
        }
        return value;
    }

    private static String longToCode(Long id) {
        StringBuilder a = new StringBuilder();
        while (id > 0) {
            a.append((char) (id % 100 + (int) 'A'));
            id /= 100;
        }
        return a.reverse().toString();
    }

    private static JavaRDD<Edge<Tuple2<Float, Integer>>> createEdgeDataset(JavaSparkContext ctx, SQLContext sqlCtx) {
        // Read edges file
        return ctx.textFile("src/main/resources/fake_agg_no_headers.csv")
                .map(row -> row.split(",")).sample(false, 0.001, 9)
                .map(row -> new Edge<>(codeToLong(row[0]), codeToLong(row[1]), new Tuple2<>(Float.parseFloat(row[2]), Integer.parseInt(row[3]))));
    }

    private static JavaRDD<Tuple2<Object, Tuple2<ArrayList, ArrayList>>> createVertexDataset(JavaSparkContext ctx, SQLContext sqlCtx) {
        // Read vertices file
        return ctx.textFile("src/main/resources/fake_agg_no_headers.csv")
                .map(row -> row.split(","))
                .flatMap(row -> Lists.newArrayList(row[0], row[1]).iterator())
                .distinct()
                .map(s -> new Tuple2<>(codeToLong(s), new Tuple2<>(Lists.newArrayList(), Lists.newArrayList())));
    }

    private static class VProg extends AbstractFunction3<
            Object,
            Tuple2<ArrayList, ArrayList>,
            ArrayList<ArrayList<Tuple3<Long, Float, Integer>>>,
            Tuple2<ArrayList, ArrayList>>
            implements Serializable {
        @Override
        public Tuple2<ArrayList, ArrayList>
        apply(Object vertexID,
              Tuple2<ArrayList, ArrayList> vertexValue,
              ArrayList<ArrayList<Tuple3<Long, Float, Integer>>> message) {
//            System.out.println("Vertex " + vertexID + " received " + message + " " + message.get(0).getClass());
            ArrayList<ArrayList<Tuple3<Long, Float, Integer>>> newPaths = Lists.newArrayList();
            for (ArrayList<Tuple3<Long, Float, Integer>> path : message) {
                // Check if is a full loop
                if (path.size() > 2 && path.get(1)._1().equals(vertexID)) {
                    vertexValue._2.add(path);
                    continue;
                }
                // Check for inner loops
                boolean loop = false;
                for (Tuple3<Long, Float, Integer> step : path) {
                    if (step._1().equals(vertexID)) {
                        // Avoid inner loops
                        loop = true;
                        break;
                    }
                }

                // Keep path as valid
                if (!loop) {
                    newPaths.add(path);
                }
            }
            return new Tuple2<>(newPaths, vertexValue._2);
        }
    }

    private static class sendMsg extends AbstractFunction1<
            EdgeTriplet<Tuple2<ArrayList, ArrayList>, Tuple2<Float, Integer>>,
            Iterator<Tuple2<Object, ArrayList<ArrayList<Tuple3<Long, Float, Integer>>>>>> implements Serializable {
        @Override
        public Iterator<Tuple2<Object, ArrayList<ArrayList<Tuple3<Long, Float, Integer>>>>>
        apply(
                EdgeTriplet<Tuple2<ArrayList, ArrayList>, Tuple2<Float, Integer>> triplet) {
            Tuple2<Object, Tuple2<ArrayList, ArrayList>> source = triplet.toTuple()._1();

            Tuple2<Float, Integer> edge = triplet.toTuple()._3();

            ArrayList<Tuple2<Object, ArrayList<ArrayList<Tuple3<Long, Float, Integer>>>>> messages = Lists.newArrayList();
            for (ArrayList<Tuple3<Long, Float, Integer>> path : (ArrayList<ArrayList<Tuple3<Long, Float, Integer>>>) source._2._1) {
                Tuple3<Long, Float, Integer> lastStep = path.get(path.size() - 1);
                ArrayList<Tuple3<Long, Float, Integer>> newPath = (ArrayList<Tuple3<Long, Float, Integer>>) path.clone();
                newPath.add(new Tuple3<>(triplet.srcId(), edge._1, edge._2));
                if (edge._1 > lastStep._2()) {
                    ArrayList<ArrayList<Tuple3<Long, Float, Integer>>> newMessage = Lists.newArrayList();
                    newMessage.add(newPath);
                    messages.add(new Tuple2<>(triplet.dstId(), newMessage));
                }
            }
            return JavaConverters.asScalaIteratorConverter(messages.iterator()).asScala();
        }
    }

    private static class merge extends AbstractFunction2<
            ArrayList<ArrayList<Tuple3<Long, Float, Integer>>>,
            ArrayList<ArrayList<Tuple3<Long, Float, Integer>>>,
            ArrayList<ArrayList<Tuple3<Long, Float, Integer>>>
            > implements Serializable {
        @Override
        public ArrayList<ArrayList<Tuple3<Long, Float, Integer>>> apply(ArrayList<ArrayList<Tuple3<Long, Float, Integer>>> o, ArrayList<ArrayList<Tuple3<Long, Float, Integer>>> o2) {
            ArrayList<ArrayList<Tuple3<Long, Float, Integer>>> result = Lists.newArrayList();
            result.addAll(o);
            result.addAll(o2);
            return result;
        }
    }
}
