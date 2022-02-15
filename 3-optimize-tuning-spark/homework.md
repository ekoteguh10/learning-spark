# Optimizing and Tuning Spark Applications, Building Datalake with Spark

**Homework**

Tuliskan perintah yang digunakan untuk submit sebuah jar ke dalam apache spark di local dengan detail: 
* jar name : spark-examples_2.12-3.2.1.jar 
* class name : org.apache.spark.examples.SparkPi 
* argument : 70 

Untuk melakukan submit sebuah jar ke dalam Apache Spark di local, kita bisa menggunakan perintah `./bin/spark-submit` diikuti dengan `options` dan `arguments` sesuai syarat di atas.

```bash
./bin/spark-submit \
    --deploy-mode cluster \
    --master yarn \
    --class org.apache.spark.examples.SparkPi \
    /spark-home/examples/jars/spark-examples_2.12-3.2.1.jar 70
```