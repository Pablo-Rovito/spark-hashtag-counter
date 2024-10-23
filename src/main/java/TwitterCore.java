import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

public class TwitterCore {
    private static final List<String> FORBIDDEN_WORDS = List.of("caca", "pedo", "culo", "pis", "mierda");

    public static List<Tuple2<Integer, String>> getTopics(JavaRDD<String> tweets) {
        return tweets
                .map(tweet -> tweet.replace("#", " #"))            // separo los hashtag entre sí
                .map(tweet -> tweet.split("[ .,_;:()¿?¡!<>+@-]+"))            // separo las palabras guardando los #
                .flatMap(stringArray -> Arrays.asList(stringArray).iterator())      // convierte array de palabras en stream de palabras
                .map(String::toLowerCase)                                           // a minúsculas
                .filter(str -> str.contains("#"))                                   // me quedo con los topic
                .map(str -> str.substring(1))                             // saco los #
                .filter(str -> FORBIDDEN_WORDS.stream().noneMatch(str::contains))   // me fijo que no haya palabra prohibida
                .mapToPair(str -> new Tuple2<>(str, 1))
                .reduceByKey(Integer::sum)
                .mapToPair(TwitterCore::invert)
                .sortByKey(false)
                .take(10);
    }

    public static Tuple2<Integer, String> invert(Tuple2<String, Integer> tuple) {
        return new Tuple2<>(tuple._2, tuple._1);
    }

    public static void main(String[] args) throws IOException {
        long startTime = Calendar.getInstance().getTimeInMillis();
        System.out.println("program initialized");

        List<String> listOfTweets = Files.readString(Path.of("tweets.txt"))
                .lines()
                .filter(line -> !line.isBlank())
                .filter(line -> line.contains("#"))
                .map(String::toLowerCase)
                .collect(Collectors.toList());

        // Paso 1: Abrir una conexión con el cluster de Spark
        //      El obj JavaSparkContext es el que refleja una conexión con el cluster
        //      Para crearlo necesitamos suministrar un objeto del tipo SparkConf

        final SparkConf connectionConfig = new SparkConf();
        connectionConfig.setAppName("TrendingTopicGetter");
        connectionConfig.setMaster("local[2]"); // Típicamente spark://IP:7077

        JavaSparkContext connection = new JavaSparkContext(connectionConfig);
        // Para local, poner "local" y arranca cada vez q le doy a play. Opcional... "local[n]" selecciona cuantos cores usa.
        // Paso 2: Convertir mis datos en un objeto RDD
        //      Los RDD se generan desde la conexión al cluster.
        JavaRDD<String> tweetsRDD = connection.parallelize(listOfTweets);

        // Paso 3: Aplicar map-reduce

        List<Tuple2<Integer, String>> topics = getTopics(tweetsRDD);

        // Paso 4: Procesar el resultado, o sea, guardarlo en una BDD, fichero, imprimirlo, etc.
        topics.forEach(tuple -> System.out.println(tuple._2 + ": " + tuple._1));

        // Paso 5: Cerrar conexión con el cluster
        connection.close();

        System.err.println((double) (Calendar.getInstance().getTimeInMillis() - startTime)/1000);

    }
}
