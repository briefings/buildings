
Just starting ... a brief investigation via Census Bureau buildings data  ...

* `mvn clean package`
* `mvn clean install`
* `spark-submit --class com.grey.BuildingsApp --master local[*] target/buildings-...-jar-with-dependencies.jar 
https://raw.githubusercontent.com/briefings/buildings/develop/arguments.yaml`
