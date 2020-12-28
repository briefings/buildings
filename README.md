## Buildings

**Brief Investigations**: via Census Bureau buildings data

* [Sources](#sources)
* [Logging](#logging)
* [Environment](#environment)

<br>

### Sources

* Buildings
  * http://www.census.gov/econ/currentdata/
  * https://www.census.gov/econ/currentdata/?programCode=RESCONST

<br>

### Logging

* [scala-logging](https://index.scala-lang.org/lightbend/scala-logging/scala-logging/3.9.2?target=_2.11) <br>
    ```
    <groupId>com.typesafe.scala-logging</groupId>
    <artifactId>scala-logging_2.11</artifactId>
    <version>3.9.2</version>
    
    <groupId>ch.qos.logback</groupId>
    <artifactId>logback-classic</artifactId>
    <version>1.2.3</version>
    ```
            
    ```import com.typesafe.scalalogging.Logger```

* [ScalaLogging](https://www.playframework.com/documentation/2.6.x/ScalaLogging) <br>
    ```
    <groupId>com.typesafe.play</groupId>
    <artifactId>play_2.11</artifactId>
    <version>2.7.7</version>
    ```
    
    ```import play.api.Logger```
    
* [Log4j](https://logging.apache.org/log4j/2.x/)
  * [Scala API](https://logging.apache.org/log4j/scala/)
  * [Tutorials](https://howtodoinjava.com/log4j/)
  * [Console Appender](https://howtodoinjava.com/log4j/log4j-console-appender-example/)


<br>

### Environment

* `mvn clean package`
* `mvn clean install`
* `spark-submit --class com.grey.BuildingsApp --master local[*] target/buildings-...-jar-with-dependencies.jar 
https://raw.githubusercontent.com/briefings/buildings/develop/arguments.yaml`
