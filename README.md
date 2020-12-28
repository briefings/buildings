## Buildings

Just starting ... a brief investigation via Census Bureau buildings data  ...

<br>

### Sources

* Buildings
  * http://www.census.gov/econ/currentdata/
  * https://www.census.gov/econ/currentdata/?programCode=RESCONST

<br>

### Logging

* [scala-logging](https://index.scala-lang.org/lightbend/scala-logging/scala-logging/3.9.2?target=_2.11)
* [ScalaLogging](https://www.playframework.com/documentation/2.6.x/ScalaLogging)
* [Log4j](https://logging.apache.org/log4j/2.x/)
  * [Scala API](https://logging.apache.org/log4j/scala/)
  * [Tutorials](https://howtodoinjava.com/log4j/)
  * [Console Appender](https://howtodoinjava.com/log4j/log4j-console-appender-example/)

Either

* https://www.playframework.com/documentation/2.6.x/ScalaLogging
  * com.typesafe.play:play_2.11: `import play.api.Logger`

or

* https://index.scala-lang.org/lightbend/scala-logging/scala-logging/3.9.2?target=_2.11
  * com.typesafe.scala-logging:scala-logging_2.11: `import com.typesafe.scalalogging.Logger`

<br>

### Environment

* `mvn clean package`
* `mvn clean install`
* `spark-submit --class com.grey.BuildingsApp --master local[*] target/buildings-...-jar-with-dependencies.jar 
https://raw.githubusercontent.com/briefings/buildings/develop/arguments.yaml`
