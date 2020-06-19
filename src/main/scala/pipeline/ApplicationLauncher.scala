package pipeline

import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.launcher.SparkAppHandle

object ApplicationLauncher {

  def launch(hdfsFilePath:String):Unit={

    val command = new SparkLauncher()
      .setAppResource("<file-path-of -your-application-jar>")
      .setMainClass("ParseInputFile")
      .setVerbose(false)
      .addAppArgs(hdfsFilePath)
      .setMaster("local")
      .addSparkArg("--jars","<file-path>/spark-xml_2.11-0.5.0.jar") //It is required to parse xml file

    val appHandle = command.startApplication()

    appHandle.addListener(new SparkAppHandle.Listener{
      def infoChanged(sparkAppHandle : SparkAppHandle) : Unit = {
        println(sparkAppHandle.getState + "  Custom Print")
      }

      def stateChanged(sparkAppHandle : SparkAppHandle) : Unit = {
        println(sparkAppHandle.getState)
        if ("FINISHED".equals(sparkAppHandle.getState.toString)){
          sparkAppHandle.stop
        }
      }
    })

  }

}