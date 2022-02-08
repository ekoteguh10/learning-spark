# Spark Fundamentals

**Soal Essay**

1. Load sample data di Github ini ke dalam Spark DataFrame dan tampilkan datanya
  
  Link dataset:
  [https://github.com/rizqinugroho/learning-hadoop/blob/main/learning-spark/sample-books.json](https://github.com/rizqinugroho/learning-hadoop/blob/main/learning-spark/sample-books.json)

  **Langkah-langkah:**
  - Download JDK dan Spark `<Cek file .ipynb>`
  - Setting environment dari keduanya supaya bisa berjalan `<Cek file .ipynb>`
  - Download dataset
    Kita bisa menggunakan perintah `!wget --continue <link raw data>`. Data yang diunduh secara otomatis akan disimpan ke dalam `/content/sample-books.json`

    ```bash
    !wget --continue https://raw.githubusercontent.com/rizqinugroho/learning-hadoop/main/learning-spark/sample-books.json
    ```

  - Load dataset ke dalam Spark DataFrame dan tampilkan
    
    ```python
    df = spark.read.option("multiline","true").json("/content/sample-books.json")
    df.show()
    ```

  Untuk lebih detilnya, bisa dilihat pada file `Spark Fundamentals.ipynb`.

2. Tampilkan datanya dimana tahun ditulis (`year_written`) kurang dari tahun 2000
   
   Kita bisa menggunakan method `filter` pada Spark.

   ```python
   df_filtered = df.filter("year_written < 2000")
   df_filtered.show()
   ```

3. Group berdasarkan `edition` dan tampilkan total buku dari setiap editioin
   
   Kita bisa menggunakan method `groupby` pada Spark.

   ```python
   df_grouped = df.groupby('edition').count()
   df_grouped.show()
   ```
