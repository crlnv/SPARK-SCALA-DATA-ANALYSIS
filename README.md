Olá! \
Este script foi desenvolvido para demonstrar o uso do Apache Spark em Scala para análise de dados. Ele começa criando uma sessão Spark, que é o ponto de entrada para todas as operações no **Apache Spark**. A sessão é configurada para rodar localmente utilizando todos os núcleos da máquina (local[*]). O nível de log é configurado para mostrar apenas erros, o que permite um maior foco nas operações de análise.

**Dados Simulados** \
O script utiliza um conjunto de dados simulados, representando as vendas de produtos em diferentes datas. Cada linha contém informações sobre a data da venda, o nome do produto, a quantidade vendida e o preço unitário. Esses dados são convertidos em um dataframe do Spark, que facilita o processo de manipulação e análise dos dados.

**Cálculos e Agregações** \
Uma das primeiras operações realizadas é o cálculo do valor total de vendas para cada produto, em cada data. Este valor é obtido multiplicando a quantidade pela unidade de preço e é armazenado em uma nova coluna chamada valor_total.
Além disso, o script realiza uma série de cálculos agregados utilizando funções como sum, avg, max e min. Essas funções permitem calcular métricas como o total de quantidade vendida, o preço médio, o valor máximo e mínimo de vendas por produto.

**Análise Temporal** \
Uma das análises do script é o cálculo do crescimento percentual diário das vendas. Isso é feito utilizando funções de janela (Window), que permitem calcular o valor anterior de cada produto e comparar o crescimento entre as datas. A fórmula utilizada para calcular o crescimento percentual é (valor_total - valor_anterior) / valor_anterior * 100.

**Produto com Maior Valor de Vendas** \
Outra parte importante da análise é identificar qual produto teve o maior valor total de vendas no período analisado. Isso é feito agrupando os dados por produto e ordenando os resultados com base no valor total de vendas, retornando o produto com o maior valor.

**Exportação de Resultados** \
Após realizar as análises, o script exporta os resultados para um diretório especificado em formato CSV. Isso permite que os resultados possam ser compartilhados ou usados em outras ferramentas de análise ou visualização.

--

A abordagem baseada em Spark garante que os mesmos códigos possam ser facilmente adaptados para lidar com datasets massivos em ambientes distribuídos. Portanto, a combinação de Scala e Spark não só é funcional para análises locais, mas também é uma solução escalável para outros cenários, principalmente em Big Data.
