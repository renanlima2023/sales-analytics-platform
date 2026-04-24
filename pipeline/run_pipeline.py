# =============================================================================
# Sales Analytics Platform
# Script : run_pipeline.py
# Autor  : Renan Lima
# Desc   : Orquestrador do pipeline completo.
#          Executa as 3 camadas em sequência com controle de erro,
#          tempo por etapa e resumo final de execução.
#
# Uso    : python pipeline/run_pipeline.py
#          python pipeline/run_pipeline.py --etapa raw
#          python pipeline/run_pipeline.py --etapa trusted
#          python pipeline/run_pipeline.py --etapa refined
# =============================================================================

import sys
import logging
import argparse
import traceback
from pathlib import Path
from datetime import datetime

# Garante que o Python encontra os módulos do pipeline
ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR / "pipeline"))

# Importa os orquestradores de cada camada
from extract_01 import run_extraction   # noqa: E402
from trusted_02 import run_trusted      # noqa: E402
from refined_03 import run_refined      # noqa: E402

# -----------------------------------------------------------------------------
# LOGGING
# -----------------------------------------------------------------------------

LOG_DIR  = ROOT_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
log_file = LOG_DIR / "pipeline.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(log_file, encoding="utf-8"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# ETAPAS DO PIPELINE
# -----------------------------------------------------------------------------

ETAPAS = {
    "raw"     : ("Camada RAW     — Extração SQL Server",   run_extraction),
    "trusted" : ("Camada TRUSTED — Transformações",        run_trusted),
    "refined" : ("Camada REFINED — Star Schema SQLite",    run_refined),
}

# -----------------------------------------------------------------------------
# EXECUTOR DE ETAPA
# -----------------------------------------------------------------------------

def executar_etapa(nome: str, descricao: str, fn) -> tuple[bool, float]:
    """
    Executa uma etapa do pipeline com controle de tempo e erro.

    Retorna
    -------
    (sucesso: bool, elapsed: float)
    """
    logger.info(f"▶  Iniciando | {descricao}")
    start = datetime.now()

    try:
        fn()
        elapsed = (datetime.now() - start).total_seconds()
        logger.info(f"✔  Concluído | {descricao} | {elapsed:.2f}s")
        return True, elapsed

    except Exception as e:
        elapsed = (datetime.now() - start).total_seconds()
        logger.error(f"✘  Falhou    | {descricao} | {elapsed:.2f}s")
        logger.error(f"   Erro      : {e}")
        logger.debug(traceback.format_exc())
        return False, elapsed


# -----------------------------------------------------------------------------
# PIPELINE COMPLETO
# -----------------------------------------------------------------------------

def run_full_pipeline() -> None:
    """Executa as 3 camadas em sequência. Para se qualquer etapa falhar."""

    logger.info("=" * 65)
    logger.info("SALES ANALYTICS PLATFORM — PIPELINE COMPLETO")
    logger.info(f"Início : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 65)

    pipeline_start = datetime.now()
    resultados     = {}

    for nome, (descricao, fn) in ETAPAS.items():
        sucesso, elapsed = executar_etapa(nome, descricao, fn)
        resultados[nome] = {"sucesso": sucesso, "tempo": elapsed}

        if not sucesso:
            logger.error(f"Pipeline interrompido na etapa '{nome}'.")
            logger.error("Corrija o erro acima e execute novamente.")
            _print_resumo(resultados, pipeline_start, abortado=True)
            sys.exit(1)

    _print_resumo(resultados, pipeline_start, abortado=False)


# -----------------------------------------------------------------------------
# PIPELINE PARCIAL (--etapa)
# -----------------------------------------------------------------------------

def run_single_etapa(nome: str) -> None:
    """Executa apenas uma etapa específica do pipeline."""

    if nome not in ETAPAS:
        logger.error(f"Etapa '{nome}' não existe. Use: {list(ETAPAS.keys())}")
        sys.exit(1)

    descricao, fn = ETAPAS[nome]

    logger.info("=" * 65)
    logger.info(f"SALES ANALYTICS PLATFORM — ETAPA: {nome.upper()}")
    logger.info("=" * 65)

    sucesso, elapsed = executar_etapa(nome, descricao, fn)

    if not sucesso:
        sys.exit(1)


# -----------------------------------------------------------------------------
# RESUMO FINAL
# -----------------------------------------------------------------------------

def _print_resumo(
    resultados    : dict,
    pipeline_start: datetime,
    abortado      : bool
) -> None:
    """Imprime o resumo de execução no log e no terminal."""

    total_elapsed = (datetime.now() - pipeline_start).total_seconds()
    status_geral  = "ABORTADO" if abortado else "SUCESSO COMPLETO"

    logger.info("=" * 65)
    logger.info("RESUMO DE EXECUÇÃO")
    logger.info("-" * 65)

    for nome, resultado in resultados.items():
        descricao, _ = ETAPAS[nome]
        icone  = "✔" if resultado["sucesso"] else "✘"
        status = "OK" if resultado["sucesso"] else "FALHOU"
        logger.info(f"  {icone}  {descricao:<42} {resultado['tempo']:>6.2f}s  {status}")

    logger.info("-" * 65)
    logger.info(f"  Tempo total  : {total_elapsed:.2f}s")
    logger.info(f"  Status final : {status_geral}")
    logger.info("=" * 65)


# -----------------------------------------------------------------------------
# PONTO DE ENTRADA
# -----------------------------------------------------------------------------

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Sales Analytics Platform — Orquestrador do pipeline",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        "--etapa",
        type=str,
        choices=list(ETAPAS.keys()),
        default=None,
        help=(
            "Executa apenas uma etapa específica:\n"
            "  raw     → Extração do SQL Server\n"
            "  trusted → Transformações analíticas\n"
            "  refined → Modelagem dimensional (Star Schema)\n"
            "  (omitir → executa pipeline completo)"
        )
    )

    args = parser.parse_args()

    if args.etapa:
        run_single_etapa(args.etapa)
    else:
        run_full_pipeline()