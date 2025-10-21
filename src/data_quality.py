"""
Modulo de Qualidade de Dados
Implementa verificacoes de qualidade de dados em tempo de execucao para o pipeline ETL
"""

import time
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
import json
import os

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from utils import setup_logger, ETLException


class QualityCheckStatus(Enum):
    """Status das verificacoes de qualidade"""
    PASS = "PASS"
    WARN = "WARN"
    FAIL = "FAIL"


@dataclass
class QualityCheckResult:
    """Resultado de uma verificacao de qualidade"""
    check_name: str
    status: QualityCheckStatus
    value: float
    threshold: float
    message: str
    timestamp: float


class DataQualityChecker:
    """
    Classe para verificacao de qualidade de dados em tempo de execucao

    Implementa verificacoes customizadas para:
    - NULL values em campos criticos
    - Valores negativos em transacoes
    - Mudancas drasticas no volume de dados
    """

    def __init__(self):
        """Inicializa o verificador de qualidade"""
        self.logger = setup_logger(__name__)
        self.results: List[QualityCheckResult] = []
        self.history_file = "data_quality_history.json"

    def check_null_percentage(self, df: DataFrame, column: str, threshold: float = 0.01) -> QualityCheckResult:
        """
        Verifica percentual de valores NULL em uma coluna

        Args:
            df: DataFrame a ser verificado
            column: Nome da coluna
            threshold: Limite percentual (0.01 = 1%)

        Returns:
            QualityCheckResult: Resultado da verificacao
        """
        total_count = df.count()

        if total_count == 0:
            return QualityCheckResult(
                check_name=f"null_check_{column}",
                status=QualityCheckStatus.PASS,
                value=0.0,
                threshold=threshold,
                message=f"DataFrame vazio - check {column} ignorado",
                timestamp=time.time()
            )

        null_count = df.filter(F.col(column).isNull()).count()
        null_percentage = null_count / total_count

        if null_percentage > threshold:
            status = QualityCheckStatus.FAIL
            message = f"NULL em {column}: {null_percentage:.2%} > {threshold:.2%} (limite)"
        else:
            status = QualityCheckStatus.PASS
            message = f"NULL em {column}: {null_percentage:.2%} <= {threshold:.2%}"

        return QualityCheckResult(
            check_name=f"null_check_{column}",
            status=status,
            value=null_percentage,
            threshold=threshold,
            message=message,
            timestamp=time.time()
        )

    def check_negative_transactions(self, df: DataFrame, column: str, threshold: float = 0.0) -> QualityCheckResult:
        """
        Verifica percentual de transacoes negativas

        Args:
            df: DataFrame a ser verificado
            column: Nome da coluna de valor
            threshold: Limite percentual (0.0 = qualquer valor negativo)

        Returns:
            QualityCheckResult: Resultado da verificacao
        """
        total_count = df.count()

        if total_count == 0:
            return QualityCheckResult(
                check_name=f"negative_check_{column}",
                status=QualityCheckStatus.PASS,
                value=0.0,
                threshold=threshold,
                message=f"DataFrame vazio - check {column} ignorado",
                timestamp=time.time()
            )

        negative_count = df.filter(F.col(column) < 0).count()
        negative_percentage = negative_count / total_count

        if negative_percentage > threshold:
            status = QualityCheckStatus.WARN
            message = f"Transacoes negativas em {column}: {negative_percentage:.2%} > {threshold:.2%}"
        else:
            status = QualityCheckStatus.PASS
            message = f"Transacoes negativas em {column}: {negative_percentage:.2%} <= {threshold:.2%}"

        return QualityCheckResult(
            check_name=f"negative_check_{column}",
            status=status,
            value=negative_percentage,
            threshold=threshold,
            message=message,
            timestamp=time.time()
        )

    def check_volume_change(self, df: DataFrame, table_name: str, tolerance: float = 0.5) -> QualityCheckResult:
        """
        Verifica mudanca drastica no volume de dados em relacao ao historico

        Args:
            df: DataFrame atual
            table_name: Nome da tabela
            tolerance: Tolerancia percentual para mudanca (0.5 = 50%)

        Returns:
            QualityCheckResult: Resultado da verificacao
        """
        current_count = df.count()

        # Carregar historico
        historical_data = self._load_historical_data()

        if table_name not in historical_data:
            # Primeiro registro - considerar como baseline
            self._save_historical_data(table_name, current_count)
            return QualityCheckResult(
                check_name=f"volume_check_{table_name}",
                status=QualityCheckStatus.PASS,
                value=0.0,
                threshold=tolerance,
                message=f"Baseline estabelecido para {table_name}: {current_count} registros",
                timestamp=time.time()
            )

        previous_count = historical_data[table_name]
        change_percentage = abs(current_count - previous_count) / previous_count

        if change_percentage > tolerance:
            status = QualityCheckStatus.WARN
            message = f"Mudanca drastica em {table_name}: {change_percentage:.2%} > {tolerance:.2%} (anterior: {previous_count}, atual: {current_count})"
        else:
            status = QualityCheckStatus.PASS
            message = f"Volume estavel em {table_name}: {change_percentage:.2%} <= {tolerance:.2%}"

        # Atualizar historico
        self._save_historical_data(table_name, current_count)

    def check_duplicate_records(self, df: DataFrame, columns: List[str], threshold: float = 0.0) -> QualityCheckResult:
        """
        Verifica duplicatas em colunas especificas

        Args:
            df: DataFrame a ser verificado
            columns: Lista de colunas para verificar duplicatas
            threshold: Limite percentual de duplicatas aceitavel

        Returns:
            QualityCheckResult: Resultado da verificacao
        """
        total_count = df.count()

        if total_count == 0:
            return QualityCheckResult(
                check_name="duplicate_check",
                status=QualityCheckStatus.PASS,
                value=0.0,
                threshold=threshold,
                message="DataFrame vazio - check de duplicatas ignorado",
                timestamp=time.time()
            )

        # Contar registros unicos baseado nas colunas especificadas
        unique_count = df.select(columns).distinct().count()
        duplicate_percentage = (total_count - unique_count) / total_count

        if duplicate_percentage > threshold:
            status = QualityCheckStatus.FAIL
            message = f"Duplicatas encontradas: {duplicate_percentage:.2%} > {threshold:.2%} (colunas: {', '.join(columns)})"
        else:
            status = QualityCheckStatus.PASS
            message = f"Sem duplicatas significativas: {duplicate_percentage:.2%} <= {threshold:.2%}"

        return QualityCheckResult(
            check_name="duplicate_check",
            status=status,
            value=duplicate_percentage,
            threshold=threshold,
            message=message,
            timestamp=time.time()
        )

    def check_extreme_values(self, df: DataFrame, column: str, min_val: float = None, max_val: float = None) -> QualityCheckResult:
        """
        Verifica valores extremos fora de limites aceitaveis

        Args:
            df: DataFrame a ser verificado
            column: Nome da coluna numerica
            min_val: Valor minimo aceitavel
            max_val: Valor maximo aceitavel

        Returns:
            QualityCheckResult: Resultado da verificacao
        """
        total_count = df.count()

        if total_count == 0:
            return QualityCheckResult(
                check_name=f"extreme_check_{column}",
                status=QualityCheckStatus.PASS,
                value=0.0,
                threshold=0.0,
                message=f"DataFrame vazio - check {column} ignorado",
                timestamp=time.time()
            )

        # Filtrar valores validos (numericos)
        numeric_df = df.filter(F.col(column).isNotNull() & F.col(column).cast("float").isNotNull())

        if numeric_df.count() == 0:
            return QualityCheckResult(
                check_name=f"extreme_check_{column}",
                status=QualityCheckStatus.WARN,
                value=1.0,
                threshold=0.0,
                message=f"Todos os valores em {column} sao invalidos",
                timestamp=time.time()
            )

        # Calcular estatisticas
        stats = numeric_df.select(
            F.min(F.col(column).cast("float")).alias("min_val"),
            F.max(F.col(column).cast("float")).alias("max_val"),
            F.avg(F.col(column).cast("float")).alias("avg_val")
        ).collect()[0]

        extreme_count = 0
        extreme_reasons = []

        if min_val is not None and stats["min_val"] < min_val:
            extreme_count += 1
            extreme_reasons.append(f"abaixo de {min_val}")

        if max_val is not None and stats["max_val"] > max_val:
            extreme_count += 1
            extreme_reasons.append(f"acima de {max_val}")

        extreme_percentage = extreme_count / total_count

        if extreme_count > 0:
            status = QualityCheckStatus.WARN
            message = f"Valores extremos em {column}: {', '.join(extreme_reasons)} (min: {stats['min_val']}, max: {stats['max_val']})"
        else:
            status = QualityCheckStatus.PASS
            message = f"Valores extremos OK em {column}: {stats['min_val']:.2f} a {stats['max_val']:.2f}"

        return QualityCheckResult(
            check_name=f"extreme_check_{column}",
            status=status,
            value=extreme_percentage,
            threshold=0.0,
            message=message,
            timestamp=time.time()
        )

    def check_string_format(self, df: DataFrame, column: str, pattern: str = None) -> QualityCheckResult:
        """
        Verifica formato de strings usando regex

        Args:
            df: DataFrame a ser verificado
            column: Nome da coluna de texto
            pattern: Padrao regex para validar formato

        Returns:
            QualityCheckResult: Resultado da verificacao
        """
        total_count = df.count()

        if total_count == 0:
            return QualityCheckResult(
                check_name=f"format_check_{column}",
                status=QualityCheckStatus.PASS,
                value=0.0,
                threshold=0.0,
                message=f"DataFrame vazio - check {column} ignorado",
                timestamp=time.time()
            )

        # Contar valores validos (não null e não vazios)
        valid_strings = df.filter(F.col(column).isNotNull() & (F.length(F.col(column)) > 0))

        if pattern:
            # Usar regex para validar formato
            valid_format = valid_strings.filter(F.col(column).rlike(pattern))
            valid_count = valid_format.count()
        else:
            valid_count = valid_strings.count()

        invalid_percentage = (total_count - valid_count) / total_count

        if invalid_percentage > 0.05:  # 5% de tolerancia
            status = QualityCheckStatus.WARN
            message = f"Formato invalido em {column}: {invalid_percentage:.2%} registros com problemas"
        else:
            status = QualityCheckStatus.PASS
            message = f"Formato OK em {column}: {invalid_percentage:.2%} registros com problemas"

        return QualityCheckResult(
            check_name=f"format_check_{column}",
            status=status,
            value=invalid_percentage,
            threshold=0.05,
            message=message,
            timestamp=time.time()
        )

    def check_data_consistency(self, df: DataFrame, column: str) -> QualityCheckResult:
        """
        Verifica consistencia de dados (tipo, formato, valores esperados)

        Args:
            df: DataFrame a ser verificado
            column: Nome da coluna

        Returns:
            QualityCheckResult: Resultado da verificacao
        """
        total_count = df.count()

        if total_count == 0:
            return QualityCheckResult(
                check_name=f"consistency_check_{column}",
                status=QualityCheckStatus.PASS,
                value=0.0,
                threshold=0.0,
                message=f"DataFrame vazio - check {column} ignorado",
                timestamp=time.time()
            )

        # Verificacoes basicas de consistencia
        null_count = df.filter(F.col(column).isNull()).count()
        empty_count = df.filter((F.col(column).isNotNull()) & (F.length(F.col(column)) == 0)).count()
        total_issues = null_count + empty_count
        consistency_score = 1.0 - (total_issues / total_count)

        if consistency_score < 0.95:  # Menos de 95% consistente
            status = QualityCheckStatus.FAIL
            message = f"Inconsistencia em {column}: {total_issues} problemas ({null_count} nulls, {empty_count} vazios)"
        else:
            status = QualityCheckStatus.PASS
            message = f"Consistencia OK em {column}: {consistency_score:.2%} dados validos"

        return QualityCheckResult(
            check_name=f"consistency_check_{column}",
            status=status,
            value=1.0 - consistency_score,
            threshold=0.05,
            message=message,
            timestamp=time.time()
        )

    def check_data_completeness(self, df: DataFrame, required_columns: List[str]) -> QualityCheckResult:
        """
        Verifica se todas as colunas obrigatorias estao presentes

        Args:
            df: DataFrame a ser verificado
            required_columns: Lista de colunas obrigatorias

        Returns:
            QualityCheckResult: Resultado da verificacao
        """
        missing_columns = [col for col in required_columns if col not in df.columns]

        if missing_columns:
            status = QualityCheckStatus.FAIL
            message = f"Colunas obrigatorias ausentes: {', '.join(missing_columns)}"
            completeness = 0.0
        else:
            status = QualityCheckStatus.PASS
            message = f"Todas as colunas obrigatorias presentes: {len(required_columns)} colunas"
            completeness = 1.0

        return QualityCheckResult(
            check_name="completeness_check",
            status=status,
            value=completeness,
            threshold=1.0,
            message=message,
            timestamp=time.time()
        )

    def run_quality_checks(self, df: DataFrame, table_name: str, required_columns: List[str] = None) -> List[QualityCheckResult]:
        """
        Executa todas as verificacoes de qualidade em um DataFrame

        Args:
            df: DataFrame a ser verificado
            table_name: Nome da tabela para contexto
            required_columns: Lista de colunas obrigatorias

        Returns:
            List[QualityCheckResult]: Lista de resultados das verificacoes
        """
        self.logger.info(f"Iniciando verificacoes de qualidade para: {table_name}")
        self.results = []

        # Verificacao de completude (colunas obrigatorias)
        if required_columns:
            self.results.append(self.check_data_completeness(df, required_columns))

        # Verificacoes especificas por tabela
        if table_name == "cartao":
            # Check NULL em num_cartao (> 1%)
            self.results.append(self.check_null_percentage(df, "num_cartao", threshold=0.01))
            # Check duplicatas em num_cartao
            self.results.append(self.check_duplicate_records(df, ["num_cartao"], threshold=0.0))
            # Check formato do numero do cartao
            self.results.append(self.check_string_format(df, "num_cartao", pattern=r"^\d{16}$"))

        elif table_name == "movimento":
            # Check transacoes negativas (> 0%)
            self.results.append(self.check_negative_transactions(df, "vlr_transacao", threshold=0.0))
            # Check valores extremos em vlr_transacao
            self.results.append(self.check_extreme_values(df, "vlr_transacao", min_val=0.01, max_val=100000.0))
            # Check mudanca drastica no volume
            self.results.append(self.check_volume_change(df, table_name, tolerance=0.5))
            # Check duplicatas em transacoes (id + cartao)
            self.results.append(self.check_duplicate_records(df, ["id", "id_cartao"], threshold=0.0))

        elif table_name == "associado":
            # Check consistencia em dados pessoais
            self.results.append(self.check_data_consistency(df, "email"))
            self.results.append(self.check_data_consistency(df, "nome"))

        elif table_name == "conta":
            # Check consistencia de tipos de conta
            self.results.append(self.check_data_consistency(df, "tipo"))

        # Log dos resultados
        for result in self.results:
            if result.status == QualityCheckStatus.FAIL:
                self.logger.error(f"ERRO: {result.message}")
            elif result.status == QualityCheckStatus.WARN:
                self.logger.warning(f"AVISO: {result.message}")
            else:
                self.logger.info(f"OK: {result.message}")

        return self.results

    def get_failed_checks(self) -> List[QualityCheckResult]:
        """Retorna apenas as verificacoes que falharam"""
        return [r for r in self.results if r.status == QualityCheckStatus.FAIL]

    def get_warning_checks(self) -> List[QualityCheckResult]:
        """Retorna apenas as verificacoes com avisos"""
        return [r for r in self.results if r.status == QualityCheckStatus.WARN]

    def should_reject_pipeline(self) -> bool:
        """Determina se o pipeline deve ser rejeitado baseado nos resultados"""
        return len(self.get_failed_checks()) > 0

    def _load_historical_data(self) -> Dict[str, int]:
        """Carrega dados historicos de volume"""
        try:
            if os.path.exists(self.history_file):
                with open(self.history_file, 'r') as f:
                    return json.load(f)
        except Exception as e:
            self.logger.warning(f"Erro ao carregar historico: {e}")

        return {}

    def _save_historical_data(self, table_name: str, count: int):
        """Salva dados historicos de volume"""
        try:
            historical_data = self._load_historical_data()
            historical_data[table_name] = count

            with open(self.history_file, 'w') as f:
                json.dump(historical_data, f, indent=2)
        except Exception as e:
            self.logger.warning(f"Erro ao salvar historico: {e}")

    def generate_detailed_metrics(self, df: DataFrame, table_name: str) -> Dict:
        """
        Gera métricas detalhadas de qualidade de dados

        Args:
            df: DataFrame analisado
            table_name: Nome da tabela

        Returns:
            Dict: Métricas detalhadas em formato JSON
        """
        total_count = df.count()

        if total_count == 0:
            return {
                "table_name": table_name,
                "total_records": 0,
                "timestamp": time.time(),
                "summary": {
                    "status": "EMPTY",
                    "message": "DataFrame vazio"
                },
                "columns": {},
                "quality_checks": []
            }

        metrics = {
            "table_name": table_name,
            "total_records": total_count,
            "timestamp": time.time(),
            "summary": {
                "status": "COMPLETED",
                "total_checks": len(self.results),
                "passed_checks": len([r for r in self.results if r.status == QualityCheckStatus.PASS]),
                "warning_checks": len([r for r in self.results if r.status == QualityCheckStatus.WARN]),
                "failed_checks": len([r for r in self.results if r.status == QualityCheckStatus.FAIL])
            },
            "columns": {},
            "quality_checks": []
        }

        # Métricas por coluna
        for column in df.columns:
            col_metrics = self._analyze_column_metrics(df, column)
            metrics["columns"][column] = col_metrics

        # Detalhes das verificações realizadas
        for result in self.results:
            check_detail = {
                "check_name": result.check_name,
                "status": result.status.value,
                "value": result.value,
                "threshold": result.threshold,
                "message": result.message,
                "timestamp": result.timestamp
            }
            metrics["quality_checks"].append(check_detail)

        return metrics

    def _analyze_column_metrics(self, df: DataFrame, column: str) -> Dict:
        """
        Analisa métricas detalhadas de uma coluna específica

        Args:
            df: DataFrame
            column: Nome da coluna

        Returns:
            Dict: Métricas da coluna
        """
        total_count = df.count()

        # Métricas básicas
        null_count = df.filter(F.col(column).isNull()).count()
        not_null_count = total_count - null_count
        null_percentage = null_count / total_count if total_count > 0 else 0

        # Tentar converter para numérico para estatísticas adicionais
        try:
            numeric_df = df.filter(F.col(column).cast("float").isNotNull())
            if numeric_df.count() > 0:
                stats = numeric_df.select(
                    F.min(F.col(column).cast("float")).alias("min_val"),
                    F.max(F.col(column).cast("float")).alias("max_val"),
                    F.avg(F.col(column).cast("float")).alias("avg_val"),
                    F.stddev(F.col(column).cast("float")).alias("std_val")
                ).collect()[0]

                # Histograma básico (buckets de valores)
                histogram = self._create_histogram(numeric_df, column)
            else:
                stats = {"min_val": None, "max_val": None, "avg_val": None, "std_val": None}
                histogram = {}
        except:
            stats = {"min_val": None, "max_val": None, "avg_val": None, "std_val": None}
            histogram = {}

        # Verificar se é string para análise de formato
        if df.schema[column].dataType.simpleString() in ['string', 'varchar']:
            empty_strings = df.filter((F.col(column).isNotNull()) & (F.length(F.col(column)) == 0)).count()
            avg_length = df.filter(F.col(column).isNotNull()).select(F.avg(F.length(F.col(column)))).collect()[0][0]
        else:
            empty_strings = 0
            avg_length = None

        return {
            "data_type": df.schema[column].dataType.simpleString(),
            "total_count": total_count,
            "null_count": null_count,
            "not_null_count": not_null_count,
            "null_percentage": null_percentage,
            "empty_strings": empty_strings,
            "completeness_score": not_null_count / total_count if total_count > 0 else 0,
            "numeric_stats": {
                "min": float(stats["min_val"]) if stats["min_val"] is not None else None,
                "max": float(stats["max_val"]) if stats["max_val"] is not None else None,
                "avg": float(stats["avg_val"]) if stats["avg_val"] is not None else None,
                "std": float(stats["std_val"]) if stats["std_val"] is not None else None
            },
            "string_stats": {
                "avg_length": float(avg_length) if avg_length is not None else None
            },
            "histogram": histogram
        }

    def _create_histogram(self, df: DataFrame, column: str, buckets: int = 10) -> Dict:
        """
        Cria histograma básico de distribuição de valores

        Args:
            df: DataFrame numérico
            column: Nome da coluna
            buckets: Número de buckets

        Returns:
            Dict: Histograma de distribuição
        """
        try:
            # Calcular estatísticas básicas
            stats = df.select(
                F.min(F.col(column).cast("float")).alias("min_val"),
                F.max(F.col(column).cast("float")).alias("max_val")
            ).collect()[0]

            if stats["min_val"] is None or stats["max_val"] is None:
                return {}

            min_val, max_val = float(stats["min_val"]), float(stats["max_val"])

            if min_val == max_val:
                return {"single_value": min_val, "count": df.count()}

            # Criar buckets
            bucket_size = (max_val - min_val) / buckets

            histogram_data = {}
            for i in range(buckets):
                bucket_min = min_val + (i * bucket_size)
                bucket_max = min_val + ((i + 1) * bucket_size)

                if i == buckets - 1:  # Último bucket inclui o valor máximo
                    bucket_max = max_val

                bucket_label = f"{bucket_min:.2f}-{bucket_max:.2f}"

                count = df.filter(
                    (F.col(column).cast("float") >= bucket_min) &
                    (F.col(column).cast("float") < bucket_max)
                ).count()

                if count > 0:
                    histogram_data[bucket_label] = count

            return histogram_data

        except Exception as e:
            self.logger.warning(f"Erro ao criar histograma para {column}: {e}")
            return {}

    def generate_quality_report(self) -> str:
        """
        Gera relatório de qualidade de dados (formato texto legado)

        Returns:
            str: Relatório em formato texto
        """
        if not self.results:
            return "Nenhuma verificacao de qualidade executada"

        report_lines = ["# Relatorio de Qualidade de Dados", ""]

        # Resumo geral
        total_checks = len(self.results)
        passed = len([r for r in self.results if r.status == QualityCheckStatus.PASS])
        warned = len([r for r in self.results if r.status == QualityCheckStatus.WARN])
        failed = len([r for r in self.results if r.status == QualityCheckStatus.FAIL])

        report_lines.append("## Resumo Geral")
        report_lines.append(f"- OK: {passed}/{total_checks}")
        report_lines.append(f"- AVISO: {warned}/{total_checks}")
        report_lines.append(f"- ERRO: {failed}/{total_checks}")
        report_lines.append("")

        # Detalhes das verificacoes
        report_lines.append("## Detalhes das Verificacoes")
        report_lines.append("")

        for result in self.results:
            status_icon = {"PASS": "OK", "WARN": "AVISO", "FAIL": "ERRO"}[result.status.value]
            report_lines.append(f"### {status_icon} {result.check_name}")
            report_lines.append(f"- Status: {result.status.value}")
            report_lines.append(f"- Valor: {result.value}")
            report_lines.append(f"- Limite: {result.threshold}")
            report_lines.append(f"- Mensagem: {result.message}")
            report_lines.append("")

        return "\n".join(report_lines)

    def save_detailed_report(self, df: DataFrame, table_name: str, output_path: str = None) -> str:
        """
        Salva relatório detalhado de qualidade em formato JSON

        Args:
            df: DataFrame analisado
            table_name: Nome da tabela
            output_path: Caminho para salvar o arquivo (opcional)

        Returns:
            str: Caminho do arquivo salvo ou métricas em JSON
        """
        if output_path is None:
            timestamp = int(time.time())
            output_path = f"data_quality_report_{table_name}_{timestamp}.json"

        # Gerar métricas detalhadas
        detailed_metrics = self.generate_detailed_metrics(df, table_name)

        # Salvar em arquivo
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(detailed_metrics, f, indent=2, ensure_ascii=False, default=str)

            self.logger.info(f"Relatorio detalhado salvo em: {output_path}")
            return output_path

        except Exception as e:
            self.logger.error(f"Erro ao salvar relatorio: {e}")
            # Retornar métricas como string JSON se não conseguir salvar
            return json.dumps(detailed_metrics, indent=2, ensure_ascii=False, default=str)
