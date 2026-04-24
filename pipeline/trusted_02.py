# =============================================================================
# Sales Analytics Platform
# Camada : TRUSTED — Transformações Analíticas
# Script : 02_trusted.py
# Autor  : Renan Lima
# Desc   : Lê os .parquet da camada Raw, aplica limpeza, tipagem,
#          enriquecimento e cria colunas de negócio. Salva os dados
#          tratados como .parquet na camada Trusted.
# =============================================================================

import logging
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime

# -----------------------------------------------------------------------------
# 1. CONFIGURAÇÃO — caminhos e logging
# -----------------------------------------------------------------------------

ROOT_DIR     = Path(__file__).resolve().parent.parent
RAW_DIR      = ROOT_DIR / "data" / "raw"
TRUSTED_DIR  = ROOT_DIR / "data" / "trusted"
LOG_DIR      = ROOT_DIR / "logs"

TRUSTED_DIR.mkdir(parents=True, exist_ok=True)

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
# 2. FUNÇÕES UTILITÁRIAS
# -----------------------------------------------------------------------------

def read_parquet(table_name: str) -> pd.DataFrame:
    """Lê um .parquet da camada Raw e loga o resultado."""
    path = RAW_DIR / f"{table_name}.parquet"
    df = pd.read_parquet(path, engine="pyarrow")
    logger.info(f"Lido da Raw | tabela={table_name} | linhas={len(df):,}")
    return df


def save_parquet(df: pd.DataFrame, table_name: str) -> None:
    """Salva DataFrame como .parquet na camada Trusted."""
    path = TRUSTED_DIR / f"{table_name}.parquet"
    df.to_parquet(path, index=False, engine="pyarrow")
    logger.info(
        f"Salvo na Trusted | tabela={table_name} | "
        f"linhas={len(df):,} | colunas={len(df.columns)}"
    )


def log_nulls(df: pd.DataFrame, table_name: str) -> None:
    """Loga colunas com nulos — útil para documentar qualidade dos dados."""
    nulls = df.isnull().sum()
    nulls = nulls[nulls > 0]
    if not nulls.empty:
        logger.warning(f"Nulos encontrados | tabela={table_name}")
        for col, count in nulls.items():
            pct = (count / len(df)) * 100
            logger.warning(f"  └─ {col}: {count:,} nulos ({pct:.1f}%)")
    else:
        logger.info(f"Sem nulos | tabela={table_name}")


# -----------------------------------------------------------------------------
# 3. TRANSFORMAÇÕES POR TABELA
# -----------------------------------------------------------------------------

def transform_fact_internet_sales(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma a fato de vendas Internet (B2C).

    Transformações aplicadas:
    - Converte DateKey (int 20101229) para datetime
    - Trata nulos em colunas financeiras
    - Cria colunas de negócio: lucro, margem, ticket_medio
    - Adiciona coluna de canal para unificação posterior
    """
    logger.info("Transformando fact_internet_sales...")

    # Cópia para não modificar o dado original em memória
    df = df.copy()

    # --- Tipagem de datas ---
    # OrderDateKey vem como int (ex: 20101229) — converte para datetime
    df["OrderDate"] = pd.to_datetime(
        df["OrderDateKey"].astype(str), format="%Y%m%d", errors="coerce"
    )
    df["ShipDate"] = pd.to_datetime(df["ShipDate"], errors="coerce")

    # --- Trata nulos em colunas financeiras ---
    # No canal Internet as métricas são NOT NULL, mas aplicamos fillna
    # como segurança para futuras cargas incrementais
    financial_cols = [
        "OrderQuantity", "UnitPrice", "DiscountAmount",
        "ProductStandardCost", "TotalProductCost", "SalesAmount",
        "TaxAmt", "Freight"
    ]
    df[financial_cols] = df[financial_cols].fillna(0)

    # --- Colunas de negócio ---
    # Lucro bruto: receita menos custo do produto
    df["Lucro"] = df["SalesAmount"] - df["TotalProductCost"]

    # Margem percentual: quanto do real vendido sobra após o custo
    # DIVIDE segura — evita divisão por zero
    df["Margem_Pct"] = np.where(
        df["SalesAmount"] > 0,
        (df["Lucro"] / df["SalesAmount"]) * 100,
        0
    )

    # Ticket médio por linha de pedido
    df["Ticket_Medio"] = np.where(
        df["OrderQuantity"] > 0,
        df["SalesAmount"] / df["OrderQuantity"],
        0
    )

    # Receita líquida (descontando frete e impostos)
    df["Receita_Liquida"] = df["SalesAmount"] - df["TaxAmt"] - df["Freight"]

    # Canal — identifica a origem para análise multicanal
    df["Canal"] = "Internet"

    # Extrai componentes de data úteis para análises de sazonalidade
    df["Ano"]       = df["OrderDate"].dt.year
    df["Mes"]       = df["OrderDate"].dt.month
    df["Trimestre"] = df["OrderDate"].dt.quarter

    # Remove colunas que não serão usadas na camada Refined
    df = df.drop(columns=["DueDateKey", "ShipDateKey"], errors="ignore")

    log_nulls(df, "fact_internet_sales_trusted")
    return df


def transform_fact_reseller_sales(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma a fato de vendas Reseller (B2B).

    Diferença importante em relação ao Internet Sales:
    as métricas financeiras são NULLABLE no Reseller —
    por isso o tratamento de nulos é mais cuidadoso.
    """
    logger.info("Transformando fact_reseller_sales...")

    df = df.copy()

    # --- Tipagem de datas ---
    df["OrderDate"] = pd.to_datetime(
        df["OrderDateKey"].astype(str), format="%Y%m%d", errors="coerce"
    )
    df["ShipDate"] = pd.to_datetime(df["ShipDate"], errors="coerce")

    # --- Trata nulos — aqui é mais crítico pois o schema permite nulos ---
    financial_cols = [
        "OrderQuantity", "UnitPrice", "DiscountAmount",
        "ProductStandardCost", "TotalProductCost", "SalesAmount",
        "TaxAmt", "Freight"
    ]
    null_count_before = df[financial_cols].isnull().sum().sum()
    df[financial_cols] = df[financial_cols].fillna(0)

    if null_count_before > 0:
        logger.warning(
            f"fact_reseller_sales: {null_count_before:,} nulos tratados "
            f"em colunas financeiras (preenchidos com 0)"
        )

    # --- Colunas de negócio (idênticas ao Internet para permitir união) ---
    df["Lucro"] = df["SalesAmount"] - df["TotalProductCost"]

    df["Margem_Pct"] = np.where(
        df["SalesAmount"] > 0,
        (df["Lucro"] / df["SalesAmount"]) * 100,
        0
    )

    df["Ticket_Medio"] = np.where(
        df["OrderQuantity"] > 0,
        df["SalesAmount"] / df["OrderQuantity"],
        0
    )

    df["Receita_Liquida"] = df["SalesAmount"] - df["TaxAmt"] - df["Freight"]

    # Canal B2B
    df["Canal"] = "Reseller"

    df["Ano"]       = df["OrderDate"].dt.year
    df["Mes"]       = df["OrderDate"].dt.month
    df["Trimestre"] = df["OrderDate"].dt.quarter

    df = df.drop(columns=["DueDateKey", "ShipDateKey"], errors="ignore")

    log_nulls(df, "fact_reseller_sales_trusted")
    return df


def build_fact_vendas_unified(
    df_internet: pd.DataFrame,
    df_reseller: pd.DataFrame
) -> pd.DataFrame:
    """
    Unifica Internet Sales e Reseller Sales em uma única fato de vendas.

    Por que unificar?
    - Permite análise consolidada de receita total da empresa
    - Permite comparativo direto B2C vs B2B pela coluna Canal
    - Simplifica o modelo no Power BI (uma tabela fato central)

    A chave CustomerKey existe só no Internet — no Reseller é ResellerKey.
    Criamos CustomerKey_Unified para manter compatibilidade no Star Schema.
    """
    logger.info("Unificando fatos Internet + Reseller...")

    # Colunas comuns entre os dois canais
    common_cols = [
        "ProductKey", "OrderDateKey", "SalesTerritoryKey",
        "SalesOrderNumber", "SalesOrderLineNumber",
        "OrderQuantity", "UnitPrice", "DiscountAmount",
        "ProductStandardCost", "TotalProductCost", "SalesAmount",
        "TaxAmt", "Freight", "OrderDate", "ShipDate",
        "Lucro", "Margem_Pct", "Ticket_Medio", "Receita_Liquida",
        "Canal", "Ano", "Mes", "Trimestre"
    ]

    # Internet: usa CustomerKey como chave de cliente
    df_int = df_internet[common_cols + ["CustomerKey"]].copy()
    df_int = df_int.rename(columns={"CustomerKey": "ClienteKey"})

    # Reseller: usa ResellerKey como chave de cliente (canal B2B)
    df_res = df_reseller[common_cols + ["ResellerKey"]].copy()
    df_res = df_res.rename(columns={"ResellerKey": "ClienteKey"})

    # União vertical
    df_unified = pd.concat([df_int, df_res], ignore_index=True)

    # Gera chave surrogate para a fato unificada
    df_unified.insert(0, "VendaKey", range(1, len(df_unified) + 1))

    logger.info(
        f"Fato unificada | Internet={len(df_internet):,} | "
        f"Reseller={len(df_reseller):,} | Total={len(df_unified):,}"
    )

    return df_unified


def transform_dim_product(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma a dimensão de produtos.
    Trata nulos em hierarquia (produtos sem subcategoria/categoria).
    Padroniza strings.
    """
    logger.info("Transformando dim_product...")

    df = df.copy()

    # Produtos sem subcategoria (ex: componentes avulsos) recebem label explícito
    df["SubcategoryName"] = df["SubcategoryName"].fillna("Sem Subcategoria")
    df["CategoryName"]    = df["CategoryName"].fillna("Sem Categoria")

    # Padroniza campos opcionais
    df["Color"]       = df["Color"].fillna("N/A")
    df["Size"]        = df["Size"].fillna("N/A")
    df["ProductLine"] = df["ProductLine"].fillna("N/A")
    df["ModelName"]   = df["ModelName"].fillna(df["ProductName"])
    df["Status"]      = df["Status"].fillna("Current")

    # Margem potencial do produto (ListPrice vs StandardCost)
    # Útil para análise de precificação
    df["Margem_Potencial_Pct"] = np.where(
        df["ListPrice"] > 0,
        ((df["ListPrice"] - df["StandardCost"]) / df["ListPrice"]) * 100,
        0
    )

    log_nulls(df, "dim_product_trusted")
    return df


def transform_dim_customer(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma a dimensão de clientes B2C.
    Cria nome completo e faixas de renda para segmentação.
    """
    logger.info("Transformando dim_customer...")

    df = df.copy()

    # Nome completo — útil para exibição no dashboard
    df["NomeCompleto"] = df["FirstName"].str.strip() + " " + df["LastName"].str.strip()

    # Faixa de renda — segmentação analítica
    # Baseado nos valores do AdventureWorks (10k a 170k anuais)
    bins   = [0, 30000, 60000, 100000, float("inf")]
    labels = ["Baixa", "Média", "Alta", "Premium"]
    df["FaixaRenda"] = pd.cut(
        df["YearlyIncome"], bins=bins, labels=labels, right=False
    )

    # Trata nulos em localização
    df["City"]    = df["City"].fillna("Desconhecida")
    df["State"]   = df["State"].fillna("Desconhecido")
    df["Country"] = df["Country"].fillna("Desconhecido")

    # Converte data
    df["BirthDate"]         = pd.to_datetime(df["BirthDate"], errors="coerce")
    df["DateFirstPurchase"] = pd.to_datetime(df["DateFirstPurchase"], errors="coerce")

    log_nulls(df, "dim_customer_trusted")
    return df


def transform_dim_reseller(df: pd.DataFrame) -> pd.DataFrame:
    """Transforma a dimensão de revendedores B2B."""
    logger.info("Transformando dim_reseller...")

    df = df.copy()

    df["City"]    = df["City"].fillna("Desconhecida")
    df["State"]   = df["State"].fillna("Desconhecido")
    df["Country"] = df["Country"].fillna("Desconhecido")
    df["BusinessType"] = df["BusinessType"].fillna("Outros")

    log_nulls(df, "dim_reseller_trusted")
    return df


def transform_dim_territory(df: pd.DataFrame) -> pd.DataFrame:
    """Transforma a dimensão de territórios."""
    logger.info("Transformando dim_territory...")

    df = df.copy()
    df.columns = [c.strip() for c in df.columns]

    log_nulls(df, "dim_territory_trusted")
    return df


def transform_dim_date(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma a dimensão de tempo.
    Adiciona colunas úteis para análise de sazonalidade.
    """
    logger.info("Transformando dim_date...")

    df = df.copy()

    df["FullDate"] = pd.to_datetime(df["FullDate"], errors="coerce")

    # Label de período — útil para eixos de gráficos no Power BI
    df["AnoMes"]      = df["Year"].astype(str) + "-" + df["MonthNumber"].astype(str).str.zfill(2)
    df["AnoTrimestre"] = df["Year"].astype(str) + " Q" + df["Quarter"].astype(str)

    # Flag de fim de ano — identifica Q4 para análise de sazonalidade
    df["Flag_Q4"] = (df["Quarter"] == 4).astype(int)

    log_nulls(df, "dim_date_trusted")
    return df


def transform_dim_promotion(df: pd.DataFrame) -> pd.DataFrame:
    """Transforma a dimensão de promoções."""
    logger.info("Transformando dim_promotion...")

    df = df.copy()

    df["StartDate"] = pd.to_datetime(df["StartDate"], errors="coerce")
    df["EndDate"]   = pd.to_datetime(df["EndDate"],   errors="coerce")
    df["MinQty"]    = df["MinQty"].fillna(0).astype(int)
    df["MaxQty"]    = df["MaxQty"].fillna(0).astype(int)

    log_nulls(df, "dim_promotion_trusted")
    return df


# -----------------------------------------------------------------------------
# 4. ORQUESTRADOR DA CAMADA TRUSTED
# -----------------------------------------------------------------------------

def run_trusted() -> dict:
    """
    Executa todas as transformações da camada Trusted em sequência.
    Retorna dicionário com os DataFrames transformados.
    """
    logger.info("=" * 65)
    logger.info("INÍCIO DO PIPELINE — CAMADA TRUSTED")
    logger.info("=" * 65)

    pipeline_start = datetime.now()
    trusted_data   = {}

    # --- Fatos ---
    df_internet = transform_fact_internet_sales(read_parquet("fact_internet_sales"))
    df_reseller = transform_fact_reseller_sales(read_parquet("fact_reseller_sales"))

    # Fato unificada — diferencial multicanal do projeto
    df_fato = build_fact_vendas_unified(df_internet, df_reseller)
    save_parquet(df_fato, "fato_vendas")
    trusted_data["fato_vendas"] = df_fato

    # --- Dimensões ---
    transformations = {
        "dim_product"   : transform_dim_product,
        "dim_customer"  : transform_dim_customer,
        "dim_reseller"  : transform_dim_reseller,
        "dim_territory" : transform_dim_territory,
        "dim_date"      : transform_dim_date,
        "dim_promotion" : transform_dim_promotion,
    }

    for table_name, transform_fn in transformations.items():
        df_raw = read_parquet(table_name)
        df_trusted = transform_fn(df_raw)
        save_parquet(df_trusted, table_name)
        trusted_data[table_name] = df_trusted

    # --- Resumo ---
    total_elapsed = (datetime.now() - pipeline_start).total_seconds()

    logger.info("=" * 65)
    logger.info("RESUMO — CAMADA TRUSTED")
    logger.info(f"Tabelas transformadas : {len(trusted_data)}")
    logger.info(f"Tempo total           : {total_elapsed:.2f}s")
    logger.info("Status                : SUCESSO COMPLETO")
    logger.info("=" * 65)

    return trusted_data


# -----------------------------------------------------------------------------
# 5. PONTO DE ENTRADA
# -----------------------------------------------------------------------------

if __name__ == "__main__":
    trusted_data = run_trusted()

    # Preview analítico — valida as colunas de negócio criadas
    print("\n📊 Preview da fato_vendas unificada:\n")
    df = trusted_data["fato_vendas"]

    print(f"  Total de registros  : {len(df):,}")
    print(f"  Receita total       : R$ {df['SalesAmount'].sum():,.2f}")
    print(f"  Lucro total         : R$ {df['Lucro'].sum():,.2f}")
    print(f"  Margem média        : {df['Margem_Pct'].mean():.1f}%")
    print(f"  Ticket médio        : R$ {df['Ticket_Medio'].mean():,.2f}")
    print(f"\n  Por canal:")
    print(df.groupby("Canal")[["SalesAmount", "Lucro", "Margem_Pct"]].agg({
        "SalesAmount" : "sum",
        "Lucro"       : "sum",
        "Margem_Pct"  : "mean"
    }).round(2).to_string())