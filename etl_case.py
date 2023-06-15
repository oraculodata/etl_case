from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, regexp_replace
from delta.tables import DeltaTable, DeltaMergeBuilder

# Iniciar uma sessão no Spark
spark = SparkSession.builder \
    .appName("ETL Task With Delta Lake") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

def preprocess_data(df):
    # Filtrar os dados com respostas válidas
    filtered_data = df.filter((col("Respondent").isNotNull()) & (col("Salary").isNotNull()) &
                              (col("YearsCoding").isNotNull()) & (col("YearsCoding").isNotNull()))

    # Remover registros duplicados com base no campo "Respondent"
    filtered_data = filtered_data.dropDuplicates(['Respondent'])

    # Tratar os valores em branco
    filtered_data = filtered_data.withColumn("Student", when(col("Student").isNull(), "NO").otherwise(col("Student")))
    filtered_data = filtered_data.withColumn("Employment", when(col("Employment").isNull(), "Full-time").otherwise(col("Employment")))

    # Remover registros com mais de 3 campos vazios
    filtered_data = filtered_data.withColumn("EmptyFieldsCount",
                                             (col("Student").isNull().cast("integer") +
                                              col("Employment").isNull().cast("integer") +
                                              col("Salary").isNull().cast("integer") +
                                              col("YearsCoding").isNull().cast("integer")))
    filtered_data = filtered_data.filter(col("EmptyFieldsCount") <= 3).drop("EmptyFieldsCount")

    # Tratar o formato dos valores de salário
    filtered_data = filtered_data.withColumn("Salary", regexp_replace(col("Salary"), "[^0-9.]", "").cast("float"))

    # Converter salários para base anual
    filtered_data = filtered_data.withColumn("Salary", when(col("SalaryType") == "Monthly", col("Salary") * 12)
                                             .when(col("SalaryType") == "Weekly", col("Salary") * 52)
                                             .otherwise(col("Salary")))

    # Corrigir a codificação dos caracteres
    filtered_data = filtered_data.withColumn("Country", regexp_replace(col("Country"), "[^\x00-\x7F]+", ""))

    return filtered_data

# Carregar os dados dos arquivos CSV
raw_data = spark.read.csv("data/RawData.csv", header=True, inferSchema=True)
raw_data_updates = spark.read.csv("data/RawDataUpdates.csv", header=True, inferSchema=True)

# Pré-processamento dos dados
filtered_data = preprocess_data(raw_data)
filtered_data_updates = preprocess_data(raw_data_updates)

# Selecionar os campos necessários para o esquema final
final_data = filtered_data.select("Respondent", "Salary", "YearsCoding", "YearsCodingProf", "Student", "Employment",
                                  "Country", "FormalEducation", "CurrencySymbol")

# Caminho para o diretório de saída do Delta Lake
delta_path = "delta_output"

# Verificar se a tabela Delta já existe
if DeltaTable.isDeltaTable(spark, delta_path):
    # Carregar a tabela Delta existente
    delta_table = DeltaTable.forPath(spark, delta_path)
else:
    # Salvar o DataFrame final em formato Delta Lake
    final_data.write.format("delta").mode("overwrite").save(delta_path)
    # Inicializar a tabela Delta
    delta_table = DeltaTable.forPath(spark, delta_path)

# Imprimir o count da tabela Delta antes do merge
print("Count antes do merge:", delta_table.toDF().count())

# Definir a condição de merge
condition = "target.Respondent = source.Respondent"

# Definir as ações de atualização e inserção
update_action = {
    "Salary": "source.Salary"
}

insert_action = {
    "Respondent": "source.Respondent",
    "Salary": "source.Salary",
    "YearsCoding": "source.YearsCoding",
    "YearsCodingProf": "source.YearsCodingProf",
    "Student": "source.Student",
    "Employment": "source.Employment",
    "Country": "source.Country",
    "FormalEducation": "source.FormalEducation",
    "CurrencySymbol": "source.CurrencySymbol"
}

# Executar a operação de merge
delta_table.alias("target") \
    .merge(filtered_data_updates.alias("source"), condition) \
    .whenMatchedUpdate(condition=condition, set=update_action) \
    .whenNotMatchedInsert(values=insert_action) \
    .execute()

# Imprimir o count da tabela Delta após o merge
print("Count após o merge:", delta_table.toDF().count())

# Imprimir o esquema da tabela Delta final
print("Esquema da tabela Delta final:")
delta_table.toDF().printSchema()

# Finalizar a sessão Spark
spark.stop()
