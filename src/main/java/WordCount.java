import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import java.util.Arrays;
import java.util.Iterator;
/**
 * @author: Created By lujisen
 * @company ChinaUnicom Software JiNan
 * @date: 2020-03-12 08:26
 * @version: v1.0
 * @description: com.hadoop.ljs.spark220.study
 */
public class WordCount {
    public static void main(String[] args) throws Exception{
        /*spark环境初始化*/
        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount");
        SparkSession sc = SparkSession.builder().config(sparkConf).getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(sc.sparkContext());
        /*读取本地文件*/
        JavaRDD<String> sourceRDD = jsc.textFile("/Users/chenshuai1/dev/spark-2.2.0-bin-hadoop2.7/README.md");
        /*转换多维为一维数组*/
        JavaRDD<String> words = sourceRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s)  {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });
        /*转换成（hello,1)格式*/
        JavaPairRDD<String, Integer> wordOne = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<String, Integer>(s, 1);
            }
        });
        /*根据key进行聚合*/
        JavaPairRDD<String, Integer> wordCount = wordOne.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2)  {
                return v1+v2;
            }
        });
        /*打印结果*/
        wordCount.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> result){
                System.out.println(result._1 + " -> " + result._2);
            }
        });

    }
}