## Buildings

<br>

**Brief Investigations**: via Census Bureau buildings data

* [Sources](#sources)
* [Development Notes](#development-notes)
  * [Logging](#logging)
  * [Software](#software)
  * [Running Apache Spark Packages](#running-apache-spark-packages)
* [References](#references)  

<br>

### Sources

* Buildings
  * http://www.census.gov/econ/currentdata/
  * https://www.census.gov/econ/currentdata/?programCode=RESCONST

<br>

### Development Notes

#### Logging

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

#### Software

*  Java <br> 
    ```
    java -version
    
    java version "1.8.0_181"
    Java(TM) SE Runtime Environment (build 1.8.0_181-b13) <br> 
    Java HotSpot(TM) 64-Bit Server VM (build 25.181-b13, mixed mode)
    ```

* Scala <br> 
    ```bash
    scala -version
    
    Scala code runner version 2.11.12 -- Copyright 2002-2017, LAMP/EPFL
    ```

* Spark <br> 
    ```bash
    spark-submit.cmd --version # **if** Operating System **is** Windows
    
    Spark version 2.4.7
    ```

<br> 

In terms of packaging, **Maven**, instead of **Scala Build Tool** (SBT), is now being used for all projects
  
* Maven <br>
    ```bash
    mvn -version
    
    Apache Maven 3.6.3 
    
    # Packaging: either
    mvn clean package 
    
    # or 
    mvn clean install
    ```
  
* SBT <br>
    ```bash
    # Case Windows: Within CygWin
    
    # either
    sbt about
    
    This is sbt 1.2.7
    ...
    
    # or 
    sbt sbtVersion
    
    1.2.7
    ```

<br>

#### Running Apache Spark Packages 

* Either <br>
    ```sbtshell
    spark-submit 
    --class com.grey.BuildingsApp 
    --master local[*] 
    target/buildings-...-jar-with-dependencies.jar 
        https://raw.githubusercontent.com/briefings/buildings/develop/arguments.yaml
    ```

* **or** <br>

    ```bash
    spark-submit 
    --class com.grey.BuildingsApp 
    --name "buildings" 
    --master spark://10.0.0.6:7077 
    --num-executors 2 
    target/buildings-1.0.87-jar-with-dependencies.jar 
        https://raw.githubusercontent.com/briefings/buildings/develop/arguments.yaml
    ```
    
    which allows for computation analysis.  **After** <br>

    ```sbtshell
    spark-class org.apache.spark.deploy.master.Master
    << spark://...:...
    
    spark-class org.apache.spark.deploy.worker.Worker spark://...:...
    ```
    
    **visit** http://localhost:8080
    
    <img src="docs/applications.png" style="float:middle; width:35%">

<br>

### References

* [Regular Expressions](https://dataschool.com/how-to-teach-people-sql/how-regex-works-in-sql/)

* [Tool RegExr](https://regexr.com)

* [top, limit, etc.](https://www.w3schools.com/sql/sql_top.asp)<br>
    > *Not all database systems support the SELECT TOP clause. MySQL supports 
the LIMIT clause to select a limited number of records, while Oracle uses ROWNUM*