package org.mdp.spark.cli;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.Function3;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import scala.Tuple5;

import java.io.Serializable;
import java.util.List;

/**
 * Get the average ratings of TV series from IMDb.
 *
 * This is the Java 8 version with lambda expressions.
 */
public class mensual_liquor_volume_per_capita implements Serializable {
    /**
     * This will be called by spark
     */
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:/Program Files/Hadoop/");

        if(args.length != 3) {
            System.err.println("Usage arguments: inputPath outputPath");
            System.exit(0);
        }
        new mensual_liquor_volume_per_capita().run(args[0],args[1], args[2]);
    }

    /**
     * The task body
     */
    public void run(String inputFilePath_1, String inputFilePath_2, String outputFilePath) {
        /*
         * This is the address of the Spark cluster.
         * [*] means use all the cores available.
         * This can be overridden later when we call the application from the cluster.
         * See {@see http://spark.apache.org/docs/latest/submitting-applications.html#master-urls}.
         */
        String master = "local[*]";

        /*
         * Initialises a Spark context with the name of the application
         *   and the (default) master settings.
         */
        SparkConf conf = new SparkConf()
                .setAppName(mensual_liquor_volume_per_capita.class.getName())
                .setMaster(master);
        JavaSparkContext context = new JavaSparkContext(conf);

        /*
         * Load the first RDD from the input location _1 (a local file, HDFS file, etc.)
         */
        JavaRDD<String> liquor_sales_dataset_all = context.textFile(inputFilePath_1);

        /*
         * Load the second RDD from the input location _2 (a local file, HDFS file, etc.)
         */
        JavaRDD<String> county_population_by_year_all = context.textFile(inputFilePath_2);
        
        /*
         * Here we filter the first line of liquor_sales_dataset_all.
         */
        JavaRDD<String> liquor_sales_dataset = liquor_sales_dataset_all.filter(
                line -> !line.split(";")[1].equals("Date")
        );
        
        /*
         * Here we filter the first line of county_population_by_year_all.
         */
        JavaRDD<String> county_population_by_year = county_population_by_year_all.filter(
                line -> !line.split(";")[1].equals("Year")
        );
        
        /*
         * Select useful columns of liquor_sales_dataset creating the map(County#Month#Year, Lts).
         */
        JavaPairRDD<String, Double> select_columns_liquor_all = liquor_sales_dataset.mapToPair(
        		line -> {try{
        			return new Tuple2<String, Double>(
        					line.split(";")[9]+"#"+line.split(";")[1].split("/")[0]+"#"+line.split(";")[1].split("/")[2], // County#Month#Year
            				Double.parseDouble(line.split(";")[23])); // Volume Sold (Liters)
        		}catch(ArrayIndexOutOfBoundsException e) {
        			return new Tuple2<String, Double>(
        					"DEFAULT", // County#Month#Year
            				0.0); // Volume Sold (Liters)
        		}}
        				
        );
        
        JavaPairRDD<String, Double> select_columns_liquor = select_columns_liquor_all.filter(
        		line -> !line._1.equals("DEFAULT"));
        
        
        /*
         * Select useful columns of county_population_by_year creating the map(County#Year, Population, Coordinates).
         */
        JavaPairRDD<String, Tuple2<Integer, String>> select_columns_population = county_population_by_year.mapToPair(
        		line -> new Tuple2<String, Tuple2<Integer, String>>(
        				line.split(";")[0]+"#"+line.split(";")[1], // County#Year
        				new Tuple2<Integer, String>(
        				Integer.parseInt(line.split(";")[2]), // Population
        				line.split(";")[3]) // Coordinates
        				)
        );
        
        /*
         * Group select_columns_liquor by year and month.
         */        
        JavaPairRDD<String, Double> yearMonthGrouped = select_columns_liquor.reduceByKey(
        		new Function2<Double, Double, Double>(){
        			@Override
	                public Double call(Double acum, Double serie) throws Exception {	                    
	                    return acum+serie;
	                }
        		});
        
        /*
         * Map yearMonthGrouped as (County#Year, Month, Lts).
         */        
        JavaPairRDD<String, Tuple2<String, Double>> county_map = yearMonthGrouped.mapToPair(
        		tup -> new Tuple2<String, Tuple2<String, Double>>(
        				tup._1.split("#")[0]+"#"+tup._1.split("#")[2],
        				new Tuple2<String, Double>(
        						tup._1.split("#")[1],
        						tup._2
        						)
        				)
        		);
        
        /*
         * Join between county_map and select_columns_population. (County#Year, ((Month, Lts), (Population, Coordinates)))
         */ 
        JavaPairRDD<String, Tuple2<Tuple2<String, Double>, Tuple2<Integer, String>>> join_county = county_map.join(select_columns_population);
        
        JavaRDD<Tuple5<String, String, String, Double, String>> liquor_per_capita = join_county.map(
        		tup -> new Tuple5<String, String, String, Double, String>(
        				tup._1.split("#")[0],
        				tup._1.split("#")[1],
        				tup._2._1._1,
        				tup._2._1._2/tup._2._2._1,
        				tup._2._2._2
        				)
        		);
        
        liquor_per_capita.saveAsTextFile(outputFilePath);

        context.close();
    }
}
