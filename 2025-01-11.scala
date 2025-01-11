// Databricks notebook source
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object AdvancedSparkExample {
  def main(args: Array[String]): Unit = {
    // Criar uma sessão Spark
    val spark = SparkSession
      .builder()
      .appName("ExemploAvancado")
      .master("local[*]")
      .getOrCreate()

    // Configuração para exibir resultados no console
    spark.sparkContext.setLogLevel("ERROR")

    // Criar dados simulados
    val dados = Seq(
      ("2023-01-01", "Produto A", 100, 25.0),
      ("2023-01-01", "Produto B", 200, 30.0),
      ("2023-01-02", "Produto A", 150, 25.0),
      ("2023-01-02", "Produto B", 180, 30.0),
      ("2023-01-03", "Produto A", 170, 28.0),
      ("2023-01-03", "Produto B", 190, 32.0),
      ("2023-01-04", "Produto A", 160, 27.0),
      ("2023-01-04", "Produto B", 200, 31.0)
    )

    // Definir schema para os dados
    import spark.implicits._
    val df = dados.toDF("data", "produto", "quantidade", "preco_unitario")
      .withColumn("data", to_date($"data", "yyyy-MM-dd"))

    // Exibir o DataFrame inicial
    println("Dados originais:")
    df.show()

    // Análise avançada

    // 1. Calcular o valor total de vendas por dia e produto
    val vendasTotais = df.withColumn("valor_total", $"quantidade" * $"preco_unitario")
    println("\nVendas totais por dia e produto:")
    vendasTotais.show()

    // 2. Cálculo de métricas agregadas
    val metrics = vendasTotais.groupBy("produto")
      .agg(
        sum("quantidade").alias("quantidade_total"),
        avg("preco_unitario").alias("preco_medio"),
        max("valor_total").alias("valor_maximo"),
        min("valor_total").alias("valor_minimo")
      )
    println("\nMétricas agregadas por produto:")
    metrics.show()

    // 3. Análise temporal: crescimento percentual diário
    val windowSpec = Window.partitionBy("produto").orderBy("data")
    val crescimento = vendasTotais
      .withColumn("valor_anterior", lag("valor_total", 1).over(windowSpec))
      .withColumn("crescimento_percentual",
        when($"valor_anterior".isNotNull, ($"valor_total" - $"valor_anterior") / $"valor_anterior" * 100)
          .otherwise(null))
    println("\nCrescimento percentual diário por produto:")
    crescimento.show()

    // 4. Produto com maior valor total de vendas no período
    val produtoTop = vendasTotais.groupBy("produto")
      .agg(sum("valor_total").alias("valor_total"))
      .orderBy(desc("valor_total"))
      .limit(1)
    println("\nProduto com maior valor total de vendas:")
    produtoTop.show()

    // 5. Exportar resultados para CSV
    val outputDir = "/mnt/data/vendas_totais"
    vendasTotais.write
      .option("header", "true")
      .csv(outputDir)
    println(s"\nResultados exportados para '$outputDir'.")

    // Fechar a sessão Spark
    spark.stop()
  }
}

// Executar a função main
AdvancedSparkExample.main(Array())


