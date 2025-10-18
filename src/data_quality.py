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

        return QualityCheckResult(
            check_name=f"volume_check_{table_name}",
            status=status,
            value=change_percentage,
            threshold=tolerance,
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

        elif table_name == "movimento":
            # Check transacoes negativas (> 0%)
            self.results.append(self.check_negative_transactions(df, "vlr_transacao", threshold=0.0))

            # Check mudanca drastica no volume
            self.results.append(self.check_volume_change(df, table_name, tolerance=0.5))

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

    def generate_quality_report(self) -> str:
        """Gera relatorio de qualidade de dados"""
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
