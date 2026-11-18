"""Exploracao abrangente dos indicadores municipais PEMOB com PySpark."""
from __future__ import annotations

import argparse
import csv
import os
import re
import tempfile
import unicodedata
from pathlib import Path

from openpyxl import load_workbook
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.window import Window


MISSING = ("", "NR", "NA", "NI", "N/A", "-", "--")
ID_COLUMNS = {"c_digo_ibge", "uf", "munic_pio"}


def safe_name(value: object, position: int) -> str:
    """Converte codigos da pesquisa em nomes de coluna estaveis."""
    text = re.sub(r"[^A-Za-z0-9]+", "_", str(value or "")).strip("_").lower()
    return text or f"indicador_{position}"


def normalize_text(value: object) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    return "".join(char for char in text if not unicodedata.combining(char)).lower()


def classify_unit(question: object) -> str:
    """Infere a unidade pelo enunciado original da pergunta PEMOB."""
    original = str(question or "")
    text = normalize_text(original)
    if "%" in original or "percentual" in text or "percentagem" in text:
        return "%"
    if "quilometragem" in text or "(em km)" in text:
        return "km"
    if "(em anos)" in text or "prazo do contrato" in text:
        return "anos"
    if "numero" in text or "quantidade" in text or "frota" in text:
        return "unidades"
    if ("r$" in text or "tarifa predominante" in text or "arrecad" in text
            or "custo" in text or "bandeirada" in text
            or ("valor" in text and ("taxi" in text or "mototaxi" in text or "servico" in text))):
        return "R$"
    return "numero"


def number_as_double(column_name: str) -> F.Column:
    """Converte numero decimal, aceitando virgula decimal e sinal de percentual."""
    quoted = column_name.replace("`", "``")
    return F.expr(
        f"try_cast(replace(replace(trim(cast(`{quoted}` as string)), '%', ''), ',', '.') as double)"
    )


def format_br_number(value: F.Column) -> F.Column:
    """Formata com ponto de milhar e virgula decimal sem alterar a coluna numerica."""
    sentinel = "~"
    formatted = F.format_number(value, 2)
    return F.regexp_replace(
        F.regexp_replace(F.regexp_replace(formatted, ",", sentinel), r"\.", ","),
        sentinel,
        ".",
    )


def format_value(value: F.Column, unit: F.Column) -> F.Column:
    """Formata quando a unidade e uma coluna variavel do DataFrame."""
    number = format_br_number(value)
    return (
        F.when(unit.cast("string") == F.lit("R$"), F.concat(F.lit("R$ "), number))
        .when(unit.cast("string") == F.lit("%"), F.concat(number, F.lit("%")))
        .when(unit.cast("string") == F.lit("km"), F.concat(number, F.lit(" km")))
        .when(unit.cast("string") == F.lit("anos"), F.concat(number, F.lit(" anos")))
        .otherwise(number)
    )


def format_for_unit(value: F.Column, unit: str) -> F.Column:
    """Formata quando a unidade ja e conhecida no codigo Python."""
    number = format_br_number(value)
    if unit == "R$":
        return F.concat(F.lit("R$ "), number)
    if unit == "%":
        return F.concat(number, F.lit("%"))
    if unit == "km":
        return F.concat(number, F.lit(" km"))
    if unit == "anos":
        return F.concat(number, F.lit(" anos"))
    return number


def format_python_number(value: object) -> str:
    """Formata numero local para linhas do relatorio escrito em Python."""
    if value is None:
        return "sem dados"
    return f"{float(value):,.2f}".replace(",", "~").replace(".", ",").replace("~", ".")


def convert_excel_to_csv(source: Path, csv_path: Path) -> tuple[list[str], dict[str, str], dict[str, str]]:
    """Converte a primeira aba para CSV e associa cada codigo a sua unidade."""
    workbook = load_workbook(source, read_only=True, data_only=True)
    sheet = workbook[workbook.sheetnames[0]]
    codes = [sheet.cell(2, col).value for col in range(1, sheet.max_column + 1)]
    questions = [sheet.cell(1, col).value for col in range(1, sheet.max_column + 1)]
    headers = [safe_name(code, col) for col, code in enumerate(codes, start=1)]
    seen: dict[str, int] = {}
    unique_headers = []
    units: dict[str, str] = {}
    questions_by_code: dict[str, str] = {}
    for name, question in zip(headers, questions):
        seen[name] = seen.get(name, 0) + 1
        unique = name if seen[name] == 1 else f"{name}_{seen[name]}"
        unique_headers.append(unique)
        units[unique] = classify_unit(question)
        questions_by_code[unique] = str(question or "")

    with csv_path.open("w", newline="", encoding="utf-8-sig") as stream:
        writer = csv.writer(stream)
        writer.writerow(unique_headers)
        for row in sheet.iter_rows(min_row=3):
            if row[0].value is None and row[2].value is None:
                continue
            values = []
            for index, cell in enumerate(row):
                value = cell.value
                # O Excel guarda percentuais como fracao (ex.: 0,05) quando a celula
                # usa formato percentual. Converte para pontos percentuais (5,0).
                if (value is not None and units.get(unique_headers[index]) == "%"
                        and isinstance(value, (int, float)) and "%" in cell.number_format):
                    value *= 100
                values.append("" if value is None else value)
            writer.writerow(values)
    workbook.close()
    return unique_headers, units, questions_by_code


def find_numeric_columns(df: DataFrame, candidates: list[str]) -> list[str]:
    """Mantem colunas numericas mesmo quando ha NR/NA ou numeros gravados como texto."""
    expressions = []
    aliases: dict[str, tuple[str, str]] = {}
    for index, name in enumerate(candidates):
        raw = F.trim(F.col(name).cast("string"))
        missing = F.upper(raw).isin(*MISSING)
        valid_alias, numeric_alias = f"valid_{index}", f"numeric_{index}"
        expressions.append(F.sum(F.when(~missing & raw.isNotNull(), 1).otherwise(0)).alias(valid_alias))
        expressions.append(F.sum(F.when(number_as_double(name).isNotNull(), 1).otherwise(0)).alias(numeric_alias))
        aliases[name] = (valid_alias, numeric_alias)
    if not expressions:
        return []
    counts = df.agg(*expressions).first().asDict()
    result = []
    for name, (valid_alias, numeric_alias) in aliases.items():
        valid = counts.get(valid_alias) or 0
        numeric = counts.get(numeric_alias) or 0
        if valid and numeric and numeric / valid >= 0.8:
            result.append(name)
    return result


def write_csv(df: DataFrame, path: Path) -> None:
    df.write.mode("overwrite").option("header", True).csv(str(path))


def main() -> None:
    parser = argparse.ArgumentParser(description="Analise abrangente da base PEMOB com PySpark")
    parser.add_argument("--input", default="pemob_municipal_2026.xlsx", help="Planilha PEMOB (.xlsx)")
    parser.add_argument("--output", default="resultados_pemob", help="Pasta de saida")
    args = parser.parse_args()
    source = Path(args.input).resolve()
    output = Path(args.output).resolve()
    if not source.is_file():
        raise FileNotFoundError(f"Arquivo nao encontrado: {source}")
    output.mkdir(parents=True, exist_ok=True)

    print(f"Abrindo Spark com Java em: {os.environ.get('JAVA_HOME', '(JAVA_HOME nao definido)')}", flush=True)
    spark = (SparkSession.builder.appName("AnalisePEMOB")
             .master("local[*]")
             .config("spark.sql.shuffle.partitions", "4")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    try:
        with tempfile.TemporaryDirectory(prefix="pemob_") as temp:
            csv_file = Path(temp) / "pemob.csv"
            print("Convertendo Excel para CSV temporario...", flush=True)
            headers, units, questions_by_code = convert_excel_to_csv(source, csv_file)
            print("Lendo a base no Spark...", flush=True)
            df = (spark.read.option("header", True).option("inferSchema", False)
                  .option("encoding", "UTF-8").csv(str(csv_file)))

            if not ID_COLUMNS.issubset(set(df.columns)):
                raise ValueError(f"Colunas de identificacao esperadas nao encontradas: {df.columns[:5]}")
            df = (df.withColumnRenamed("c_digo_ibge", "codigo_ibge")
                  .withColumnRenamed("munic_pio", "municipio")
                  .withColumn("codigo_ibge", F.col("codigo_ibge").cast("long")))
            candidates = [c for c in df.columns if c not in {"codigo_ibge", "uf", "municipio"}]
            print("Identificando indicadores numericos, incluindo respostas com NR/NA...", flush=True)
            metric_cols = find_numeric_columns(df, candidates)
            categorical_cols = [c for c in candidates if c not in set(metric_cols)]

            cleaned = df
            for col in metric_cols:
                raw = F.trim(F.col(col).cast("string"))
                cleaned = cleaned.withColumn(
                    col,
                    F.when(F.upper(raw).isin(*MISSING), F.lit(None).cast("double"))
                    .otherwise(number_as_double(col)),
                )
            cleaned.cache()

            print("Gravando identificacao dos municipios...", flush=True)
            write_csv(cleaned.select("codigo_ibge", "uf", "municipio"), output / "municipios")

            fares = None
            fare_stats = None
            # Tabela larga por UF com media, cobertura e media formatada por indicador.
            print(f"Calculando resumo por UF para {len(metric_cols)} indicadores numericos...", flush=True)
            summary_exprs = [F.count(F.lit(1)).alias("municipios")]
            for col in metric_cols:
                summary_exprs.extend([
                    F.avg(F.col(col)).alias(f"media_{col}"),
                    F.count(F.col(col)).alias(f"informados_{col}"),
                    format_for_unit(F.avg(F.col(col)), units.get(col, "numero")).alias(f"media_formatada_{col}"),
                ])
            if metric_cols:
                write_csv(cleaned.groupBy("uf").agg(*summary_exprs).orderBy("uf"), output / "resumo_por_uf")
            else:
                write_csv(cleaned.groupBy("uf").agg(*summary_exprs).orderBy("uf"), output / "resumo_por_uf")

            if metric_cols:
                print("Gerando indicadores por municipio e estatisticas por UF...", flush=True)
                stack_args = ", ".join([f"'{c}', cast(`{c}` as double)" for c in metric_cols])
                long_metrics = cleaned.selectExpr(
                    "codigo_ibge", "uf", "municipio",
                    f"stack({len(metric_cols)}, {stack_args}) as (indicador, valor)",
                ).where(F.col("valor").isNotNull())
                unit_expr = F.lit("numero")
                for col in metric_cols:
                    unit_expr = F.when(F.col("indicador") == col, F.lit(units.get(col, "numero"))).otherwise(unit_expr)
                long_metrics = (long_metrics.withColumn("unidade", unit_expr)
                                .withColumn("valor_formatado", format_value(F.col("valor"), F.col("unidade"))))
                write_csv(long_metrics, output / "indicadores_longos")

                stats = (long_metrics.groupBy("uf", "indicador", "unidade")
                         .agg(F.count("valor").alias("informados"), F.avg("valor").alias("media"),
                              F.expr("percentile_approx(valor, 0.5, 10000)").alias("mediana"),
                              F.min("valor").alias("minimo"), F.max("valor").alias("maximo"),
                              F.stddev_pop("valor").alias("desvio_padrao")))
                for column in ("media", "mediana", "minimo", "maximo", "desvio_padrao"):
                    stats = stats.withColumn(f"{column}_formatado", format_value(F.col(column), F.col("unidade")))
                write_csv(stats.orderBy("uf", "indicador"), output / "estatisticas_indicadores")

                # Tarifa atual versus anterior por municipio e resumo da variacao por UF.
                if "3_1_1_a" in metric_cols and "3_1_1_b" in metric_cols:
                    print("Calculando variacao das tarifas atuais e anteriores...", flush=True)
                    fares = (cleaned.select("codigo_ibge", "uf", "municipio",
                                            F.col("3_1_1_a").alias("tarifa_atual"),
                                            F.col("3_1_1_b").alias("tarifa_anterior"))
                             .withColumn("variacao_reais", F.col("tarifa_atual") - F.col("tarifa_anterior"))
                             .withColumn("variacao_percentual", F.when(
                                 F.col("tarifa_anterior") != 0,
                                 100 * F.col("variacao_reais") / F.col("tarifa_anterior")))
                             .withColumn("tarifa_atual_formatada", format_for_unit(F.col("tarifa_atual"), "R$"))
                             .withColumn("tarifa_anterior_formatada", format_for_unit(F.col("tarifa_anterior"), "R$"))
                             .withColumn("variacao_reais_formatada", format_for_unit(F.col("variacao_reais"), "R$"))
                             .withColumn("variacao_percentual_formatada", format_for_unit(F.col("variacao_percentual"), "%")))
                    write_csv(fares, output / "variacao_tarifas_municipio")
                    fare_stats = (fares.groupBy("uf").agg(
                        F.count("variacao_reais").alias("municipios_com_ambas_tarifas"),
                        F.avg("tarifa_atual").alias("media_tarifa_atual"),
                        F.avg("tarifa_anterior").alias("media_tarifa_anterior"),
                        F.avg("variacao_reais").alias("media_variacao_reais"),
                        F.avg("variacao_percentual").alias("media_variacao_percentual"),
                    ))
                    fare_stats = (fare_stats
                                  .withColumn("media_tarifa_atual_formatada", format_for_unit(F.col("media_tarifa_atual"), "R$"))
                                  .withColumn("media_tarifa_anterior_formatada", format_for_unit(F.col("media_tarifa_anterior"), "R$"))
                                  .withColumn("media_variacao_reais_formatada", format_for_unit(F.col("media_variacao_reais"), "R$"))
                                  .withColumn("media_variacao_percentual_formatada", format_for_unit(F.col("media_variacao_percentual"), "%")))
                    write_csv(fare_stats.orderBy("uf"), output / "variacao_tarifas_por_uf")

            if categorical_cols:
                print(f"Calculando frequencias para {len(categorical_cols)} indicadores categoricos...", flush=True)
                category_stack = ", ".join([f"'{c}', cast(`{c}` as string)" for c in categorical_cols])
                answers = cleaned.selectExpr(
                    "codigo_ibge", "uf",
                    f"stack({len(categorical_cols)}, {category_stack}) as (indicador, resposta)",
                )
                answers = answers.withColumn("resposta", F.trim(F.col("resposta")))
                answers = answers.where(F.col("resposta").isNotNull() & ~F.upper(F.col("resposta")).isin(*MISSING))
                respondents = (answers.groupBy("uf", "indicador")
                               .agg(F.countDistinct("codigo_ibge").alias("municipios_que_responderam")))
                choices = (answers.withColumn("opcao", F.explode(F.split(F.col("resposta"), r"\s*;\s*")))
                           .withColumn("opcao", F.trim(F.col("opcao")))
                           .where(F.col("opcao") != "")
                           .groupBy("uf", "indicador", "opcao")
                           .agg(F.countDistinct("codigo_ibge").alias("municipios_com_opcao")))
                frequencies = (choices.join(respondents, ["uf", "indicador"], "left")
                               .withColumn("percentual_respondentes",
                                           100 * F.col("municipios_com_opcao") / F.col("municipios_que_responderam"))
                               .withColumn("percentual_formatado",
                                           format_for_unit(F.col("percentual_respondentes"), "%")))
                write_csv(frequencies.orderBy("uf", "indicador", F.desc("municipios_com_opcao")),
                          output / "frequencias_categoricas")

            total = cleaned.count()
            report_path = output / "relatorio_analises.txt"
            print("Escrevendo relatorio textual consolidado...", flush=True)
            with report_path.open("w", encoding="utf-8") as report:
                report.write("RELATORIO DE ANALISE DA BASE PEMOB\n")
                report.write("=" * 72 + "\n\n")
                report.write(f"Arquivo analisado: {source.name}\n")
                report.write(f"Municipios processados: {total}\n")
                report.write(f"Perguntas/colunas: {len(headers)}\n")
                report.write(f"Indicadores numericos: {len(metric_cols)}\n")
                report.write(f"Indicadores categoricos: {len(categorical_cols)}\n\n")
                report.write("COMO LER OS RESULTADOS\n")
                report.write("- Os CSVs mantem campos numericos para calculos e campos *_formatado para leitura.\n")
                report.write("- Valores monetarios usam R$; percentuais mantem a escala da planilha e recebem %.\n")
                report.write("- NR, NA, NI e respostas vazias nao entram nas estatisticas numericas.\n")
                report.write("- As medias descrevem apenas os municipios presentes na base e com resposta valida.\n")
                report.write("- Em perguntas de multipla escolha, percentuais de opcoes podem somar mais de 100%.\n\n")

                if metric_cols:
                    report.write("ESTATISTICAS NUMERICAS POR UF E INDICADOR\n")
                    report.write("- Media, mediana aproximada, minimo, maximo e desvio padrao.\n")
                    report.write("- Os enunciados completos estao associados aos codigos abaixo.\n\n")
                    for row in stats.orderBy("uf", "indicador").toLocalIterator():
                        question = questions_by_code.get(row["indicador"], "")
                        report.write(
                            f"{row['uf']} | {row['indicador']} ({row['unidade']}) | {question}\n"
                            f"  respostas: {row['informados']}; media: {row['media_formatado']}; "
                            f"mediana: {row['mediana_formatado']}; minimo: {row['minimo_formatado']}; "
                            f"maximo: {row['maximo_formatado']}; desvio padrao: {row['desvio_padrao_formatado']}\n"
                        )

                if metric_cols and "3_1_1_a" in metric_cols and "3_1_1_b" in metric_cols:
                    report.write("\nVARIACAO DE TARIFAS POR UF\n")
                    for row in fare_stats.orderBy("uf").toLocalIterator():
                        report.write(
                            f"{row['uf']}: tarifa atual media {row['media_tarifa_atual_formatada']}; "
                            f"anterior {row['media_tarifa_anterior_formatada']}; "
                            f"diferenca media {row['media_variacao_reais_formatada']}; "
                            f"variacao percentual media {row['media_variacao_percentual_formatada']} "
                            f"({row['municipios_com_ambas_tarifas']} municipios).\n"
                        )

                if categorical_cols:
                    report.write("\nRESPOSTAS CATEGORICAS MAIS FREQUENTES (ATE 5 POR INDICADOR)\n")
                    overall = (frequencies.groupBy("indicador", "opcao")
                               .agg(F.sum("municipios_com_opcao").alias("municipios_com_opcao"),
                                    F.sum("municipios_que_responderam").alias("municipios_respondentes"))
                               .withColumn("percentual", 100 * F.col("municipios_com_opcao") / F.col("municipios_respondentes"))
                               .withColumn("percentual_formatado", format_for_unit(F.col("percentual"), "%")))
                    ranking = F.row_number().over(
                        Window.partitionBy("indicador").orderBy(F.desc("municipios_com_opcao"), F.asc("opcao"))
                    )
                    top = overall.withColumn("posicao", ranking).where(F.col("posicao") <= 5).orderBy("indicador", "posicao")
                    for row in top.toLocalIterator():
                        question = questions_by_code.get(row["indicador"], "")
                        report.write(
                            f"{row['indicador']} | {question}\n"
                            f"  {row['opcao']}: {row['municipios_com_opcao']} municipios "
                            f"({row['percentual_formatado']} dos respondentes considerados).\n"
                        )

                report.write("\nARQUIVOS CSV DETALHADOS\n")
                report.write("Consulte resumo_por_uf, indicadores_longos, estatisticas_indicadores, "
                             "frequencias_categoricas e variacao_tarifas_* para as tabelas completas.\n")

            summary_path = output / "relatorio_resumido.txt"
            print("Escrevendo relatorio resumido em linguagem simples...", flush=True)
            with summary_path.open("w", encoding="utf-8") as report:
                report.write("PEMOB — RESUMO DA ANÁLISE\n")
                report.write("=" * 48 + "\n\n")
                report.write(
                    f"Esta análise usou as respostas de {total} municípios da planilha PEMOB 2026. "
                    f"Foram avaliadas {len(headers)} perguntas: {len(metric_cols)} com respostas numéricas "
                    f"e {len(categorical_cols)} com respostas em texto ou categorias.\n\n"
                )
                report.write("O QUE OS RESULTADOS MOSTRAM\n")
                report.write(
                    "As tabelas comparam os municípios que aparecem na planilha. Para cada pergunta numérica, "
                    "o relatório detalhado apresenta média (valor médio), mediana (valor central), menor e maior "
                    "valor e quantidade de respostas usadas. Respostas vazias, NR, NA e NI ficam fora dos cálculos.\n\n"
                )
                if fares is not None:
                    overall = fares.agg(
                        F.count("tarifa_atual").alias("n_atual"),
                        F.count("tarifa_anterior").alias("n_anterior"),
                        F.count("variacao_reais").alias("n_comparaveis"),
                        F.avg("tarifa_atual").alias("media_atual"),
                        F.avg("tarifa_anterior").alias("media_anterior"),
                        F.avg("variacao_reais").alias("media_diferenca"),
                        F.avg("variacao_percentual").alias("media_percentual"),
                        F.sum(F.when(F.col("variacao_reais") > 0, 1).otherwise(0)).alias("aumentos"),
                        F.sum(F.when(F.col("variacao_reais") < 0, 1).otherwise(0)).alias("reducoes"),
                        F.sum(F.when(F.col("variacao_reais") == 0, 1).otherwise(0)).alias("sem_mudanca"),
                    ).first()
                    report.write("TARIFAS DE ÔNIBUS\n")
                    report.write(
                        f"A tarifa atual média foi R$ {format_python_number(overall['media_atual'])}, "
                        f"e a anterior foi R$ {format_python_number(overall['media_anterior'])}.\n"
                    )
                    report.write(
                        f"Entre os {overall['n_comparaveis']} municípios com as duas tarifas informadas, "
                        f"a diferença média foi R$ {format_python_number(overall['media_diferenca'])}. "
                    )
                    if overall["media_percentual"] is not None:
                        pct = format_python_number(overall["media_percentual"])
                        report.write(f"A variação percentual média por município foi {pct}%.\n")
                    else:
                        report.write("Não houve base suficiente para calcular a variação percentual média.\n")
                    report.write(
                        f"Foram observados {overall['aumentos']} aumentos, {overall['reducoes']} reduções "
                        f"e {overall['sem_mudanca']} tarifas sem mudança.\n\n"
                    )
                    by_state = fare_stats.orderBy(F.desc("media_tarifa_atual")).collect()
                    if by_state:
                        high, low = by_state[0], by_state[-1]
                        report.write("COMPARAÇÃO ENTRE ESTADOS\n")
                        report.write(
                            f"Entre os estados incluídos, a maior tarifa atual média foi em {high['uf']}: "
                            f"{high['media_tarifa_atual_formatada']} ({high['municipios_com_ambas_tarifas']} municípios).\n"
                        )
                        report.write(
                            f"A menor tarifa atual média foi em {low['uf']}: {low['media_tarifa_atual_formatada']} "
                            f"({low['municipios_com_ambas_tarifas']} municípios).\n"
                        )
                        report.write("Essas médias descrevem somente os municípios respondentes da base.\n\n")
                report.write("COMO LER OS ARQUIVOS\n")
                report.write(
                    "Os valores com R$ são monetários; os valores com % são percentuais. "
                    "Na pasta resultados_pemob, consulte estatisticas_indicadores para os números por estado, "
                    "frequencias_categoricas para respostas textuais e variacao_tarifas_municipio para comparar tarifas cidade a cidade.\n\n"
                )
                report.write("LIMITES DA ANÁLISE\n")
                report.write(
                    "A base contém uma seleção de municípios, não necessariamente todos os municípios do Brasil. "
                    "Uma média estadual baseada em poucos municípios deve ser lida com cautela. "
                    "Os resultados descrevem os dados informados e não provam a causa de diferenças entre cidades.\n"
                )
            print(f"Municipios processados: {total}", flush=True)
            print(f"Perguntas/colunas: {len(headers)}; indicadores numericos: {len(metric_cols)}; categoricos: {len(categorical_cols)}", flush=True)
            print(f"Relatorio textual: {report_path}", flush=True)
            print(f"Relatorio resumido: {summary_path}", flush=True)
            print(f"Resultados gravados em: {output}", flush=True)
            cleaned.unpersist()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
