import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum
from pyspark.sql.functions import expr
from pyspark.sql.types import *
from pyspark.sql import functions as F
import time
import datetime
from pyspark.sql.functions import *
import pandas as pd
from pyspark.sql.functions import regexp_replace


#spark session

spark = (SparkSession.builder
          .master('local[4]') #criar variável e escolhar maquina, no caso dessa maquina é uma máquina local
          .appName('instro-ptspark') #o numero entre os colchetes é o numero de nucleos da maquina
          .config('spark.ui.port', '4050') #configuração de porta
          .getOrCreate() 
          )  


#carregando dataframe

df = spark.read.csv('gs://voos-ocorrencia/ocorrencia.csv',
                    sep = ';',
                    inferSchema=True,
                    header=True,
                    encoding = 'utf-8')

#mostrando dataframe



# processamento

df=df.select('codigo_ocorrencia','ocorrencia_classificacao','ocorrencia_cidade','ocorrencia_uf','ocorrencia_pais','ocorrencia_aerodromo','ocorrencia_dia','ocorrencia_hora','total_aeronaves_envolvidas','ocorrencia_saida_pista')

df=df.withColumnRenamed('codigo_ocorrencia', 'ocorrencia').withColumnRenamed('ocorrencia_classificacao','classificacao').withColumnRenamed('ocorrencia_cidade','cidade').withColumnRenamed('ocorrencia_uf','UF').withColumnRenamed('ocorrencia_pais','pais').withColumnRenamed('ocorrencia_aerodromo','aerodromo').withColumnRenamed('ocorrencia_dia','data').withColumnRenamed('ocorrencia_hora','hora')

df=df.withColumn('data', to_date(F.col('data'),'dd/MM/yyyy' ))

df.write.format('parquet').save('gs://bucket-tratado')