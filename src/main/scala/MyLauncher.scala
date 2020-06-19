import java.util
import java.util.concurrent.CountDownLatch

import org.apache.spark.launcher.{SparkAppHandle, SparkLauncher}

object MyLauncher extends App {

  val env = new util.HashMap[String, String]()
  //这两个属性必须设置
  env.put("HADOOP_CONF_DIR", "/Users/chenshuai1/dev/hadoop-2.7.3/etc/hadoop")
  env.put("JAVA_HOME", "/Library/Java/JavaVirtualMachines/jdk1.8.0_211.jdk/Contents/Home")
  //可以不设置
  //env.put("YARN_CONF_DIR","")
  val sparkLauncher = new SparkLauncher(env)
  val countDownLatch = new CountDownLatch(1);
  //这里调用setJavaHome()方法后，JAVA_HOME is not set 错误依然存在
  val handle = new SparkLauncher(env)
    .setSparkHome("/Users/chenshuai1/dev/spark-2.4.4-bin-hadoop2.7")
    .setAppResource("/Users/chenshuai1/dev/spark-2.4.4-bin-hadoop2.7/examples/jars/spark-examples_2.11-2.4.4.jar")
    .setMainClass("org.apache.spark.examples.SparkPi")
    .setMaster("yarn")
    .setDeployMode("cluster")
    .setConf("spark.app.id", "11222")
    .setConf("spark.driver.memory", "1g")
    .setConf("spark.akka.frameSize", "200")
    .setConf("spark.executor.memory", "1g")
    .setConf("spark.executor.instances", "2")
    .setConf("spark.executor.cores", "3")
    .setConf("spark.default.parallelism", "2")
    .setConf("spark.driver.allowMultipleContexts", "true")
    .setVerbose(true).startApplication(new SparkAppHandle.Listener() {
    override def stateChanged(sparkAppHandle: SparkAppHandle): Unit = {
      if (sparkAppHandle.getState.isFinal) countDownLatch.countDown
      System.out.println("state:" + sparkAppHandle.getState.toString)
    }

    override def infoChanged(sparkAppHandle: SparkAppHandle): Unit = {
      System.out.println("Info:" + sparkAppHandle.getState.toString)
    }
  })

  System.out.println("The task is executing, please wait ....");
  //线程等待任务结束
  countDownLatch.await();
}
