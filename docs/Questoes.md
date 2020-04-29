
## Questões:

### 1. Qual o objetivo do comando cache em Spark?

**R.** _O objetivo do comando cache no Spark é melhorar a performance ao se trabalhar com RDD, pois os mesmos ficam armazenados em memória sendo utilizados repetidamente de maneira rápida, ao invés de realizar a leitura e gravação em disco._ 

* Referência: [DBTA](http://www.dbta.com/Editorial/Trends-and-Applications/Spark-and-the-Fine-Art-of-Caching-119305.aspx)

### 2. O mesmo código implementado em Spark é normalmente mais rápido que a implementação equivalente em MapReduce.Por quê?

**R.**_O MapReduce é normalmente mais lento por utilizar disco para I/O, ao contrário do Spark que utiliza memória, chegando a ser 100 vezes mais rápido que o MapReduce._

* Referência: [Educba](https://www.educba.com/mapreduce-vs-apache-spark/)

### 3. Qual é a função do SparkContext?
**R.** _SparkContext permite a transformação de dados originados em diversos formatos (ORC, Parquet, txt, csv, etc), criação de DataFrames, RDDs e variáveis.
É a porta de entrada para a codificação na linguagem Spark, sendo uma representação da conexão do cluster Spark._

* Referência: [Spark](https://spark.apache.org/docs/2.4.5/api/java/org/apache/spark/SparkContext.html)

### 4. Explique com suas palavras o que é Resilient Distributed Datasets (RDD).
**R.** _RDD é uma estrutura de dados somente leitura, que permite o uso das operações de MapReduce no cluster, de forma distribuida._

### 5. GroupByKey é menos eficiente que reduceByKey em grandes dataset. Por quê?
**R.** _O ReduceByKey  combina os resultados antes de preparar o dataset de saída que estão espalhados no cluster, diferente do groupby que traz dados desnecessários no tráfego de grandes datasets._

* Referência: [Data Bricks](https://databricks.gitbooks.io/databricks-spark-knowledge-base/content/best_practices/prefer_reducebykey_over_groupbykey.html)

### 6. Explique o que faz o código abaixo:
```scala
val textFile = sc.textFile("hdfs://...")
val counts = textFile.flatMap(line => line.split(" "))
                     .map(word => (word,1))
                     .reduceByKey(_+_)
counts.saveASTextFile("hdfs://...")                     
```
**R.**  _Desmembrando o código, vide comentários em cada linha abaixo:_
```scala
 val textFile = sc.textFile("hdfs://...")
```
 _Leitura de arquivo texto de um sistema distribuido hdfs._
 
```scala
 val counts = textFile.flatMap(line => line.split(" "))
 ```
 _Separação das palavras de cada linha do arquivo._
```scala
                      .map(word =>(word,1))
```                      
_Criação de pares chave-valor, com valor 1 fixo, para cada palavra obtida._
```scala
                      .reduceByKey(_+_) 
```
_Agrupamento das chaves compostas._

```scala
counts.saveASTextFile("hdfs://...")                     
```
_Salva o resultado transformado em arquivo texto no hdfs_
