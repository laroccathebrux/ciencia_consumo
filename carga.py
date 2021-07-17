from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql import functions as f
from pyspark.sql.types import IntegerType
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
import psycopg2
from sql_queries import *

def execute_sql(cur, conn, df, table, table_name):
    try:
        for row in df.rdd.collect():
            cur.execute(table, list(row))
            conn.commit()
        print("Table "+table_name+" "+str(df.count())+" lines Inserted")
    except ValueError as e:
        print(e)

def process_dimensional_table(cur, conn, df, spark):
    """
    Transform and write data into postgres
    :param cur: Cursor object
    :param filepath: Source data location
    """
    
    #Empresa
    df_empresa = df.select("CompanySize").distinct()
    df_empresa = df_empresa.na.drop()
    execute_sql(cur, conn, df_empresa, empresa_table_insert, "empresa")
    
    #Pais
    df_pais = df.select("Country").distinct()
    df_pais = df_pais.na.drop()
    df_pais = df_pais.orderBy("Country")
    execute_sql(cur, conn, df_pais, pais_table_insert, "pais")
    
    #Sistema Operacional
    df_so = df.select("OperatingSystem").distinct()
    df_so = df_so.na.drop()
    df_so = df_so.orderBy("OperatingSystem")
    execute_sql(cur, conn, df_so, sistema_operacional_table_insert, "sistema_operacional")
    
    #Ferramenta Comunic
    df_fc = df.select("CommunicationTools")
    df_fc = (
        df_fc
        .na.drop()
        .orderBy("CommunicationTools")
        .withColumn("arrCommunicationTools", f.split("CommunicationTools", ";")) 
        .withColumn("CommunicationTools", f.explode("arrCommunicationTools"))
        .drop(f.col("arrCommunicationTools"))
        .distinct()
    )
    execute_sql(cur, conn, df_fc, ferramenta_comunic_table_insert, "ferramenta_comunic")
    
    #Linguagem Programacao
    df_lp = df.select("LanguageWorkedWith")
    df_lp = (
        df_lp
        .na.drop()
        .orderBy("LanguageWorkedWith")
        .withColumn("arrLanguageWorkedWith", f.split("LanguageWorkedWith", ";"))
        .withColumn("LanguageWorkedWith", f.explode("arrLanguageWorkedWith"))
        .drop(f.col("arrLanguageWorkedWith"))
        .distinct()
    )
    execute_sql(cur, conn, df_lp, linguagem_programacao_table_insert, "linguagem_programacao")

def iterate(cur, conn, df, table):
    cur.execute(table, list(df))
    conn.commit()
        
def process_fact_table(cur, conn, df, spark):
    
    cur.execute("Select id, nome from sistema_operacional order by nome ASC")
    rst = cur.fetchall()
    df_rst_so = spark.createDataFrame(rst, ["sistema_operacional_id", "OperatingSystem_list"])
    df_rst_so.na.drop()
    
    cur.execute("Select id, nome from pais Order By nome ASC")
    rst = cur.fetchall()
    df_rst_pais = spark.createDataFrame(rst, ["pais_id", "Country_list"])
    df_rst_pais.na.drop()
    
    cur.execute("Select id, tamanho from empresa Order By id ASC")
    rst = cur.fetchall()
    df_rst_emp = spark.createDataFrame(rst, ["empresa_id", "CompanySize_list"])
    df_rst_emp.na.drop()
    
    df_rp = df.select("OpenSource", "Hobby", "ConvertedSalary", "Country", "OperatingSystem", "CompanySize")
    spec = Window.partitionBy().orderBy("OpenSource")
    df_rp = df_rp.withColumn("nome",  row_number().over(spec))
    df_rp = df_rp.withColumn("nome", f.concat(f.lit('respondente_'),f.col('nome')))
    df_rp = (
        df_rp
        .join(df_rst_so, df_rp.OperatingSystem == df_rst_so.OperatingSystem_list, "left")
        .join(df_rst_pais, df_rp.Country == df_rst_pais.Country_list, "left")
        .join(df_rst_emp, df_rp.CompanySize == df_rst_emp.CompanySize_list, "left")
        .drop(f.col("Country"))
        .drop(f.col("Country_list"))
        .drop(f.col("OperatingSystem"))
        .drop(f.col("OperatingSystem_list"))
        .drop(f.col("CompanySize"))
        .drop(f.col("CompanySize_list"))
        .drop(f.col("id"))
    )
    df_rp = df_rp.fillna({'ConvertedSalary':0})
    df_rp = (
    df_rp
        .withColumn("OpenSource", f.when(f.col("OpenSource") == "Yes", "1").otherwise("0"))
        .withColumn("Hobby", f.when(f.col("Hobby") == "Yes", "1").otherwise("0"))
        .withColumn("ConvertedSalary", df_rp.ConvertedSalary.cast(DoubleType()))
        .withColumn("ConvertedSalary", (f.col("ConvertedSalary") / 5.6))
        .withColumn("salario", f.round(f.col("ConvertedSalary"),2))
    )
    df_rp = df_rp.withColumn("contrib_open_source", df_rp.OpenSource.cast(IntegerType()))
    df_rp = df_rp.withColumn("programa_hobby", df_rp.Hobby.cast(IntegerType()))
    df_rp = df_rp.select("nome", "contrib_open_source", "programa_hobby", "salario", "sistema_operacional_id", "pais_id", "empresa_id")
    
    execute_sql(cur, conn, df_rp, respondente_table_insert, "respondente")
    
def process_other_table(cur, conn, df, spark):
    
    #resp_usa_ferramenta
    cur.execute("Select id, nome from ferramenta_comunic order by nome ASC")
    rst = cur.fetchall()
    df_rst_fc = spark.createDataFrame(rst, ["ferramenta_comunic_id", "CommunicationTools_list"])
    df_rst_fc.na.drop()
    
    cur.execute("Select id, nome from respondente order by nome ASC")
    rst = cur.fetchall()
    df_rst_rp = spark.createDataFrame(rst, ["respondente_id", "nome"])
    df_rst_rp.na.drop()
    
    df_ruf = df.select("CommunicationTools", "id")
    df_ruf = (
            df_ruf
            .orderBy("CommunicationTools")
            .withColumn("arrCommunicationTools", f.split("CommunicationTools", ";"))
            .withColumn("CommunicationTools", f.explode("arrCommunicationTools"))
            .drop(f.col("arrCommunicationTools"))
        )
    df_ruf = df_ruf.join(df_rst_fc, df_ruf.CommunicationTools == df_rst_fc.CommunicationTools_list, "left")
    df_ruf = df_ruf.join(df_rst_rp, df_ruf.id == df_rst_rp.nome, "left")
    df_ruf = (
        df_ruf
        .drop(f.col("CommunicationTools"))
        .drop(f.col("id"))
        .drop(f.col("CommunicationTools_list"))
        .drop(f.col("nome"))
        )
    
    execute_sql(cur, conn, df_ruf, resp_usa_ferramenta_table_insert, "resp_usa_ferramenta")
    
    #resp_usa_linguagem
    cur.execute("Select id, nome from linguagem_programacao order by nome ASC")
    rst = cur.fetchall()
    df_rst_lp = spark.createDataFrame(rst, ["linguagem_programacao_id", "LanguageWorkedWith_list"])
    df_rst_lp.na.drop()
    
    cur.execute("Select id, nome from respondente order by nome ASC")
    rst = cur.fetchall()
    df_rst_rp = spark.createDataFrame(rst, ["respondente_id", "nome"])
    df_rst_rp.na.drop()
    
    df_rul = df.select("LanguageWorkedWith", "id")
    df_rul = (
            df_rul
            .orderBy("LanguageWorkedWith")
            .withColumn("arrLanguageWorkedWith", f.split("LanguageWorkedWith", ";"))
            .withColumn("LanguageWorkedWith", f.explode("arrLanguageWorkedWith"))
            .drop(f.col("arrLanguageWorkedWith"))
        )
    df_rul = df_rul.join(df_rst_lp, df_rul.LanguageWorkedWith == df_rst_lp.LanguageWorkedWith_list, "left")
    df_rul = df_rul.join(df_rst_rp, df_rul.id == df_rst_rp.nome, "left")
    df_rul = (
        df_rul
        .drop(f.col("LanguageWorkedWith"))
        .drop(f.col("id"))
        .drop(f.col("LanguageWorkedWith_list"))
        .drop(f.col("nome"))
        )
    df_rul = df_rul.select("respondente_id", "linguagem_programacao_id")
    execute_sql(cur, conn, df_rul, resp_usa_linguagem_table_insert, "resp_usa_linguagem")

def process_data(cur, conn, spark, func):
    """
    Iterates over data source and applies a function to each data into the list
    :param cur: Cursor object
    :param conn: Connection
    :param spark: spark session
    :param func: Function being applied
    """
    df = (
        spark.read.csv(
            path="base_de_respostas_10k_amostra.csv",
            sep=",",
            header=True,
            quote='"'
        )
    )
    
    spec = Window.partitionBy().orderBy("OpenSource")
    df = df.withColumn("id",  row_number().over(spec))
    df = df.withColumn("id", f.concat(f.lit('respondente_'),f.col('id')))
    df.count()
    
    func(cur, conn, df, spark)


def main():
    """
    Connects to the database and processes data
    :rtype: object
    """
    conn = psycopg2.connect("host=batyr.db.elephantsql.com dbname=hzlsgeqq user=hzlsgeqq password=1WQIBn7f0j5d9J-p88FDlXXCgNzg9CnG")
    cur = conn.cursor()
    
    spark = (
        SparkSession
        .builder
        .appName("DesafioCienciaConsumo")
        .config("spark.jars", "postgresql-42.2.22.jar")
        .getOrCreate()
    )

    process_data(cur, conn, spark, func=process_dimensional_table)
    process_data(cur, conn, spark, func=process_fact_table)
    process_data(cur, conn, spark, func=process_other_table)

    conn.close()


if __name__ == "__main__":
    main()