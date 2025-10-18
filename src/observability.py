"""
Módulo de Observabilidade e Métricas
Implementa sistema de métricas e monitoramento para o pipeline ETL
"""

import time
import json
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from datetime import datetime
import threading
import os

from utils import setup_logger


@dataclass
class MetricPoint:
    """Ponto de métrica com timestamp"""
    name: str
    value: float
    timestamp: float
    labels: Dict[str, str] = field(default_factory=dict)
    metric_type: str = "gauge"  # gauge, counter, histogram

    def to_dict(self) -> Dict[str, Any]:
        """Converte para dicionário"""
        return {
            "name": self.name,
            "value": self.value,
            "timestamp": self.timestamp,
            "labels": self.labels,
            "type": self.metric_type
        }

    def to_prometheus_format(self) -> str:
        """Converte para formato Prometheus"""
        labels_str = ",".join([f'{k}="{v}"' for k, v in self.labels.items()])
        if labels_str:
            return f"{self.name}{{{labels_str}}} {self.value}"
        return f"{self.name} {self.value}"


@dataclass
class TimerResult:
    """Resultado de medição de tempo"""
    name: str
    duration_seconds: float
    start_time: float
    end_time: float
    metadata: Dict[str, Any] = field(default_factory=dict)


class MetricsCollector:
    """
    Coletor de métricas para o pipeline ETL

    Responsável por:
    - Coletar métricas de tempo e contadores
    - Armazenar métricas em memória
    - Exportar métricas em diferentes formatos
    - Integrar com sistemas externos (Prometheus)
    """

    def __init__(self):
        """Inicializa o coletor de métricas"""
        self.logger = setup_logger(__name__)
        self.metrics: List[MetricPoint] = []
        self.timers: List[TimerResult] = []
        self._lock = threading.Lock()
        self.prometheus_gateway_url = os.getenv("PROMETHEUS_GATEWAY_URL")
        self.prometheus_job_name = os.getenv("PROMETHEUS_JOB_NAME", "sicooperative-etl")

    def record_metric(self, name: str, value: float, labels: Dict[str, str] = None,
                     metric_type: str = "gauge") -> None:
        """
        Registra uma métrica

        Args:
            name: Nome da métrica
            value: Valor da métrica
            labels: Labels/tags da métrica
            metric_type: Tipo da métrica (gauge, counter, histogram)
        """
        labels = labels or {}

        metric = MetricPoint(
            name=name,
            value=value,
            timestamp=time.time(),
            labels=labels,
            metric_type=metric_type
        )

        with self._lock:
            self.metrics.append(metric)

        self.logger.debug(f"Métrica registrada: {name}={value} {labels}")

    def record_counter(self, name: str, value: float = 1.0, labels: Dict[str, str] = None) -> None:
        """Registra um contador"""
        self.record_metric(name, value, labels, "counter")

    def record_gauge(self, name: str, value: float, labels: Dict[str, str] = None) -> None:
        """Registra um gauge"""
        self.record_metric(name, value, labels, "gauge")

    def start_timer(self, name: str, metadata: Dict[str, Any] = None) -> str:
        """
        Inicia medição de tempo

        Args:
            name: Nome do timer
            metadata: Metadados adicionais

        Returns:
            str: ID único do timer
        """
        timer_id = f"{name}_{int(time.time() * 1000)}"
        metadata = metadata or {}

        timer_result = TimerResult(
            name=name,
            duration_seconds=0.0,
            start_time=time.time(),
            end_time=0.0,
            metadata=metadata
        )

        with self._lock:
            self.timers.append(timer_result)

        self.logger.debug(f"Timer iniciado: {name} (ID: {timer_id})")
        return timer_id

    def stop_timer(self, timer_id: str) -> TimerResult:
        """
        Para medição de tempo

        Args:
            timer_id: ID do timer

        Returns:
            TimerResult: Resultado da medição
        """
        current_time = time.time()

        with self._lock:
            # Encontrar timer ativo
            timer_result = None
            for timer in self.timers:
                if timer.name in timer_id and timer.end_time == 0.0:
                    timer_result = timer
                    break

        if not timer_result:
            self.logger.warning(f"Timer não encontrado: {timer_id}")
            return None

        # Calcular duração
        timer_result.end_time = current_time
        timer_result.duration_seconds = current_time - timer_result.start_time

        # Registrar métricas
        labels = {"timer_id": timer_id, **timer_result.metadata}
        self.record_gauge(f"etl_duration_seconds", timer_result.duration_seconds, labels)
        self.record_counter(f"etl_timer_total", 1.0, labels)

        self.logger.info(f"Timer finalizado: {timer_result.name} = {timer_result.duration_seconds:.3f}s")
        return timer_result

    def record_pipeline_stage(self, stage: str, duration_seconds: float,
                            records_input: int = 0, records_output: int = 0) -> None:
        """
        Registra métricas de uma etapa do pipeline

        Args:
            stage: Nome da etapa (extract, transform, load)
            duration_seconds: Tempo de execução
            records_input: Registros de entrada
            records_output: Registros de saída
        """
        labels = {"stage": stage}

        # Métricas de tempo
        self.record_gauge(f"etl_stage_duration_seconds", duration_seconds, labels)
        self.record_counter(f"etl_stage_total", 1.0, labels)

        # Métricas de registros
        if records_input > 0:
            self.record_gauge(f"etl_records_input", records_input, labels)

        if records_output > 0:
            self.record_gauge(f"etl_records_output", records_output, labels)

        # Taxa de processamento
        if duration_seconds > 0:
            throughput = records_output / duration_seconds if records_output > 0 else 0
            self.record_gauge(f"etl_throughput_records_per_second", throughput, labels)

        self.logger.info(f"Etapa {stage}: {duration_seconds:.3f}s, {records_output} registros")

    def record_error(self, error_type: str, count: float = 1.0) -> None:
        """Registra ocorrência de erro"""
        self.record_counter(f"etl_errors_total", count, {"error_type": error_type})

    def record_data_quality_check(self, check_name: str, status: str, duration_seconds: float) -> None:
        """Registra métrica de verificação de qualidade"""
        labels = {"check_name": check_name, "status": status}
        self.record_gauge(f"etl_quality_check_duration_seconds", duration_seconds, labels)
        self.record_counter(f"etl_quality_checks_total", 1.0, labels)

    def get_metrics_summary(self) -> Dict[str, Any]:
        """Retorna resumo das métricas coletadas"""
        with self._lock:
            total_metrics = len(self.metrics)
            total_timers = len([t for t in self.timers if t.end_time > 0])

            # Estatísticas básicas
            durations = [t.duration_seconds for t in self.timers if t.end_time > 0]
            total_duration = sum(durations) if durations else 0

            return {
                "total_metrics": total_metrics,
                "total_timers": total_timers,
                "total_duration_seconds": total_duration,
                "average_duration_seconds": total_duration / total_timers if total_timers > 0 else 0,
                "collection_timestamp": datetime.now().isoformat()
            }

    def export_to_json(self) -> str:
        """Exporta métricas para formato JSON"""
        summary = self.get_metrics_summary()

        data = {
            "summary": summary,
            "metrics": [m.to_dict() for m in self.metrics],
            "timers": [
                {
                    "name": t.name,
                    "duration_seconds": t.duration_seconds,
                    "start_time": t.start_time,
                    "end_time": t.end_time,
                    "metadata": t.metadata
                }
                for t in self.timers if t.end_time > 0
            ]
        }

        return json.dumps(data, indent=2, default=str)

    def export_to_prometheus(self) -> str:
        """Exporta métricas para formato Prometheus"""
        lines = []

        with self._lock:
            for metric in self.metrics:
                lines.append(metric.to_prometheus_format())

        return "\n".join(lines) + "\n"

    def push_to_prometheus_gateway(self) -> bool:
        """
        Envia métricas para Prometheus Pushgateway

        Returns:
            bool: True se sucesso, False caso contrário
        """
        if not self.prometheus_gateway_url:
            self.logger.debug("URL do Prometheus Gateway não configurada")
            return False

        try:
            import requests

            prometheus_data = self.export_to_prometheus()

            response = requests.post(
                f"{self.prometheus_gateway_url}/metrics/job/{self.prometheus_job_name}",
                data=prometheus_data,
                headers={"Content-Type": "text/plain"}
            )

            if response.status_code == 202:
                self.logger.info(f"Métricas enviadas para Prometheus Gateway: {len(self.metrics)} métricas")
                return True
            else:
                self.logger.error(f"Erro ao enviar métricas: HTTP {response.status_code}")
                return False

        except Exception as e:
            self.logger.error(f"Erro ao conectar com Prometheus Gateway: {e}")
            return False

    def clear_metrics(self) -> None:
        """Limpa métricas coletadas"""
        with self._lock:
            self.metrics.clear()
            self.timers.clear()

        self.logger.info("Métricas limpas")


class ObservabilityManager:
    """
    Gerenciador de observabilidade para o pipeline ETL

    Coordena coleta e exportação de métricas
    """

    def __init__(self):
        """Inicializa o gerenciador de observabilidade"""
        self.logger = setup_logger(__name__)
        self.collector = MetricsCollector()
        self.enabled = os.getenv("OBSERVABILITY_ENABLED", "true").lower() == "true"

    def is_enabled(self) -> bool:
        """Verifica se observabilidade está habilitada"""
        return self.enabled

    def get_collector(self) -> MetricsCollector:
        """Retorna o coletor de métricas"""
        return self.collector

    def log_metrics_summary(self) -> None:
        """Loga resumo das métricas coletadas"""
        if not self.enabled:
            return

        summary = self.collector.get_metrics_summary()

        self.logger.info("=" * 60)
        self.logger.info("📊 RESUMO DE MÉTRICAS DO PIPELINE")
        self.logger.info("=" * 60)
        self.logger.info(f"⏱️ Duração total: {summary['total_duration_seconds']:.3f}s")
        self.logger.info(f"📈 Média por etapa: {summary['average_duration_seconds']:.3f}s")
        self.logger.info(f"🔢 Total de métricas: {summary['total_metrics']}")
        self.logger.info(f"⏳ Timers executados: {summary['total_timers']}")
        self.logger.info(f"📅 Timestamp: {summary['collection_timestamp']}")
        self.logger.info("=" * 60)

        # Log detalhado das métricas
        if self.collector.metrics:
            self.logger.debug("Métricas detalhadas:")
            for metric in self.collector.metrics[-10:]:  # Últimas 10 métricas
                self.logger.debug(f"  {metric.name}: {metric.value}")

    def export_metrics(self, format_type: str = "json") -> str:
        """
        Exporta métricas em formato especificado

        Args:
            format_type: Formato de exportação (json, prometheus)

        Returns:
            str: Métricas formatadas
        """
        if format_type == "prometheus":
            return self.collector.export_to_prometheus()
        else:
            return self.collector.export_to_json()

    def push_to_gateway(self) -> bool:
        """Envia métricas para gateway se configurado"""
        return self.collector.push_to_prometheus_gateway()


# Instância global do gerenciador de observabilidade
observability_manager = ObservabilityManager()
