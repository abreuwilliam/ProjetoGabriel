# Análise da base PEMOB com Apache Spark

Projeto do Tema 1 (mobilidade urbana): exploração distribuída de indicadores da pesquisa PEMOB municipal 2026. O pipeline lê a planilha fornecida, transforma os códigos de perguntas em nomes de colunas e usa DataFrames PySpark para produzir a relação de municípios, estatísticas de indicadores numéricos por UF e uma saída longa com os valores informados.

## Requisitos

- Python 3.10 ou superior
- Java 11 ou 17 configurado em `JAVA_HOME` (necessário ao Spark)
- A planilha `pemob_municipal_2026.xlsx` na pasta do projeto

## Instalação e execução

No Windows PowerShell, na pasta do projeto:

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install pyspark openpyxl
```

O PySpark instalado pelo `pip` no Windows pode precisar do utilitário `winutils.exe` do Hadoop. O pacote Hadoop 3.5.0 usado por esta instalação foi extraído em `hadoop-3.5.0\hadoop-3.5.0`. Em cada novo terminal PowerShell, configure as variáveis e execute:

```powershell
$env:JAVA_HOME = "C:\Program Files\Java\jdk-21"
$env:HADOOP_HOME = (Resolve-Path .\hadoop-3.5.0\hadoop-3.5.0).Path
$env:PATH = "$env:HADOOP_HOME\bin;$env:PATH"
Test-Path "$env:HADOOP_HOME\bin\winutils.exe"  # deve retornar True
python .\analisar_pemob.py
```

O pacote `winutils` é uma build comunitária para Windows, não uma distribuição oficial do Apache Hadoop.

Para informar outro arquivo ou destino:

```powershell
$env:JAVA_HOME = "C:\Program Files\Java\jdk-21"
$env:HADOOP_HOME = (Resolve-Path .\hadoop-3.5.0\hadoop-3.5.0).Path
$env:PATH = "$env:HADOOP_HOME\bin;$env:PATH"
python .\analisar_pemob.py --input .\pemob_municipal_2026.xlsx --output .\resultados_pemob
```

## Saídas

O Spark cria diretórios CSV (um ou mais arquivos `part-*.csv` por diretório) dentro de `resultados_pemob/`:

- `municipios/`: código IBGE, UF e município.
- `resumo_por_uf/`: municípios por estado, médias numéricas, quantidade de respostas válidas e médias formatadas com unidade.
- `indicadores_longos/`: valor numérico original e `valor_formatado` (por exemplo `R$ 5,50` ou `12,50%`) por município e pergunta.
- `estatisticas_indicadores/`: contagem, média, mediana, mínimo, máximo e desvio padrão por estado e pergunta, com campos formatados.
- `variacao_tarifas_municipio/` e `variacao_tarifas_por_uf/`: comparação entre tarifa atual e anterior, diferença em reais e variação percentual, quando os dois indicadores existem.
- `frequencias_categoricas/`: frequência de respostas categóricas por estado e pergunta. Respostas com várias opções separadas por ponto e vírgula são separadas; o percentual é calculado sobre municípios respondentes e pode somar mais de 100% em perguntas de múltipla escolha.
- `relatorio_analises.txt`: relatório legível com metodologia, estatísticas por UF e indicador, comparação das tarifas e até cinco respostas categóricas mais frequentes por pergunta.
- `relatorio_resumido.txt`: visão geral em linguagem simples, com comparação das tarifas e alertas sobre como interpretar a amostra.

Respostas como `NR`, `NA`, `NI` e vazias não entram nas estatísticas numéricas. O código tenta reconhecer indicadores numéricos mesmo quando os valores chegam como texto e aceita vírgula decimal. `valor` e as colunas de estatística permanecem numéricos para cálculos; os campos `*_formatado` são textos para leitura humana. Quando uma célula percentual está marcada como percentual no Excel, a fração armazenada é convertida para pontos percentuais antes da exibição (por exemplo, `0,05` vira `5,00%`). A conversão de Excel para CSV acontece antes da leitura Spark porque PySpark não lê `.xlsx` diretamente. A base contém 96 municípios preenchidos na execução observada, então demonstra as APIs distribuídas do Spark, mas não comprova ganho de desempenho em escala de terabytes.

## Observações

O arquivo de origem é mantido intacto. O programa utiliza os códigos de pergunta da segunda linha como nomes estáveis; a primeira linha contém os enunciados extensos. O modo `local[*]` executa no computador disponível, sem cluster externo.
