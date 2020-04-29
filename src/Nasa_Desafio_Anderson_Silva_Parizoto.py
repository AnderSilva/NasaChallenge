"""
    @author: Anderson Parizoto
    https://spark.apache.org/docs/2.4.5/api/python/index.html

"""
# def main():
from pathlib import Path
import os,glob,sys
#####################################
# area de setup de input/output
#####################################

#pasta origem dos dados (finalizar com '/')
folder_input  = '/home/ander/project/nasa/data/input/'
#pasta saida dos dados (finalizar com '/')
folder_output = '/home/ander/project/nasa/data/output/'

#####################################
# validacao de origem / destino
#####################################
if not os.path.exists(folder_input):
    raise IOError("Caminho {} não encontrado".format(folder_input))

if not os.path.exists(folder_output):
    raise IOError("Caminho {} não encontrado".format(folder_output))

files=list()
files = glob.glob(folder_input + '*.gz', recursive=True)

if len(files)==0:
    raise IOError("Arquivos de origem não encontrados em {}".format(folder_input))    

#####################################
# inicio do processo
#####################################

from pyspark import SparkContext,SparkConf
from pyspark.sql import SQLContext,SparkSession
spark = SparkSession.builder.appName("Nasa_Challenge").getOrCreate() #.config("spark.executor.memory", "1g")
sc = spark.sparkContext

#leitura dos arquivos da pasta        
df = spark.read.csv(files,sep=' ') 

from pyspark.sql import functions as f

#rename e tratamento inicial das colunas
df = df.select(
        df._c0.alias("host")    
        ,f.regexp_replace(f.concat(df._c3,df._c4), '\[|\]', '').alias("time")
        ,f.split(df._c5,' ').getItem(0).alias('http')
        ,f.split(df._c5,' ').getItem(1).alias('URL')
        ,f.split(df._c5,' ').getItem(2).alias('version')
        ,df._c6.alias('status')
        ,df._c7.alias('bytes')
)

#Cria view para utilizacao de queries sql
df.createOrReplaceTempView('nasa')

#####################################
# 1 - numero de hosts únicos
#####################################
filename=folder_output+'questao_1_hosts_unicos.csv'
df1 = df.select('host').distinct()
df1.toPandas().to_csv(filename)
print("1-Hosts únicos: {}\n Arquivo Salvo em: {}".format(\
     df1.count()
    ,filename
))

#####################################
# 2 - Total de erros 404
#####################################
filename=folder_output+'questao_2_total_http404.csv'
df2 = df.where(df.status == "404")
df2.toPandas().count().to_csv(filename)
print("2-Total Erros 404: {}\nArquivo salvo em: {}".format(\
     df2.count()
    ,filename
))


#####################################
# 3 - Top 5 urls com erros 404
#####################################
filename=folder_output+'questao_3_top5_http404.csv'
df2.createOrReplaceTempView('nasa_404')
df3 = spark.sql("""
        SELECT 
            COUNT(*)           AS Erros
            ,URL
        FROM nasa_404        
        GROUP BY URL
        ORDER BY Erros DESC
        LIMIT 5
""")
df3.toPandas().to_csv(filename)
print("3-Top 5 urls com erro 404:\n {}\nArquivo salvo em: {}".format(\
     df3.toPandas()
    ,filename
))


#####################################
# 4 - Quantidade de erros 404 por dia
#####################################
filename=folder_output+'questao_4_http404_diario.csv'
df4 = spark.sql("""
        SELECT 
            COUNT(*)           AS Quantidade
            ,SUBSTR(time,8,4)   AS ano 
            ,SUBSTR(time,4,3)   AS mes
            ,SUBSTR(time,1,2)   AS dia
        FROM nasa_404
        GROUP BY 2,3,4
        ORDER BY 2,3,4
""")
df4.toPandas().to_csv(filename)
print("4-Quantidade de erros 404 por dia salvo em:\n {0}".format(\
    filename
))


#####################################
# 5 - Total de bytes retornados
#####################################
filename=folder_output+'questao_5_bytes_retornados.csv'
df5 = spark.sql("""
        SELECT
            SUM(bytes) as TotalBytes
        FROM nasa
""")
df5.toPandas().to_csv(filename)
print("5-Total Bytes {} salvo em:\n {}".format(\
    df5.TotalBytes.show()
    ,filename
))

#sc.stop()
print("Processo terminado com sucesso.\n")

# if __name__ == '__main__':
#     main()