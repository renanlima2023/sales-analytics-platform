# =============================================================================
# Sales Analytics Platform
# Camada : RAW — Extração do SQL Server
# Script : 01_extract.py
# Autor  : Renan Lima
# Desc   : Extrai tabelas do AdventureWorksDW2019 via pyodbc,
#          salva como .parquet na camada raw e registra log de execução.
# =============================================================================

import os
import logging
import pyodbc
import pandas as pd
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

# -----------------------------------------------------------------------------
# 1. CONFIGURAÇÃO — caminhos e variáveis de ambiente
# -----------------------------------------------------------------------------

# Carrega variáveis do .env (DB_SERVER, DB_NAME, DB_DRIVER)
load_dotenv()

# Diretórios do projeto — Path resolve independente de OS
ROOT_DIR    = Path(__file__).resolve().parent.parent
RAW_DIR     = ROOT_DIR / "data" / "raw"
LOG_DIR     = ROOT_DIR / "logs"

# Garante que as pastas existam (segurança caso alguém clone o repo)
RAW_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

# -----------------------------------------------------------------------------
# 2. LOGGING — registra cada execução com timestamp
# -----------------------------------------------------------------------------
# Por que logar? Em ambiente real, pipeline roda agendado.
# O log é a única evidência do que aconteceu, quando e com qual resultado.

log_file = LOG_DIR / "pipeline.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(log_file, encoding="utf-8"),  # salva no arquivo
        logging.StreamHandler()                            # exibe no terminal
    ]
)

logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# 3. CONEXÃO COM SQL SERVER
# -----------------------------------------------------------------------------

def get_connection() -> pyodbc.Connection:
    """
    Cria e retorna conexão com SQL Server via Autenticação Windows.
    Lê configurações do .env — nunca hardcode credenciais no código.
    """
    server = os.getenv("DB_SERVER", "localhost")
    database = os.getenv("DB_NAME", "AdventureWorksDW2019")
    driver = os.getenv("DB_DRIVER", "ODBC Driver 17 for SQL Server")

    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"Trusted_Connection=yes;"        # Autenticação Windows
        f"TrustServerCertificate=yes;"    # Evita erro de certificado local
    )

    try:
        conn = pyodbc.connect(conn_str, timeout=30)
        logger.info(f"Conexão estabelecida | servidor={server} | banco={database}")
        return conn
    except pyodbc.Error as e:
        logger.error(f"Falha na conexão com SQL Server: {e}")
        raise

# -----------------------------------------------------------------------------
# 4. QUERIES DE EXTRAÇÃO
# -----------------------------------------------------------------------------
# Cada query é nomeada — o nome vira o nome do arquivo .parquet na camada raw.
# Selecionamos apenas as colunas necessárias para o modelo analítico.
# Joins já feitos aqui para enriquecer produto com categoria e subcategoria.

QUERIES = {

    # -------------------------------------------------------------------------
    # FATO: Vendas canal Internet (B2C)
    # -------------------------------------------------------------------------
    "fact_internet_sales": """
        SELECT
            f.ProductKey,
            f.OrderDateKey,
            f.CustomerKey,
            f.SalesTerritoryKey,
            f.SalesOrderNumber,
            f.SalesOrderLineNumber,
            f.OrderQuantity,
            f.UnitPrice,
            f.DiscountAmount,
            f.ProductStandardCost,
            f.TotalProductCost,
            f.SalesAmount,
            f.TaxAmt,
            f.Freight,
            f.OrderDate,
            f.ShipDate
        FROM dbo.FactInternetSales f
    """,

    # -------------------------------------------------------------------------
    # FATO: Vendas canal Reseller (B2B)
    # -------------------------------------------------------------------------
    "fact_reseller_sales": """
        SELECT
            f.ProductKey,
            f.OrderDateKey,
            f.ResellerKey,
            f.EmployeeKey,
            f.SalesTerritoryKey,
            f.SalesOrderNumber,
            f.SalesOrderLineNumber,
            f.OrderQuantity,
            f.UnitPrice,
            f.DiscountAmount,
            f.ProductStandardCost,
            f.TotalProductCost,
            f.SalesAmount,
            f.TaxAmt,
            f.Freight,
            f.OrderDate,
            f.ShipDate
        FROM dbo.FactResellerSales f
    """,

    # -------------------------------------------------------------------------
    # DIMENSÃO: Produto (com hierarquia de categoria já resolvida no JOIN)
    # Por que fazer o JOIN aqui na extração?
    # Porque na camada Trusted vamos trabalhar com um DataFrame flat —
    # mais simples para transformações. O JOIN no SQL é mais eficiente
    # do que fazer merge de 3 DataFrames no pandas.
    # -------------------------------------------------------------------------
    "dim_product": """
        SELECT
            p.ProductKey,
            p.ProductAlternateKey,
            p.EnglishProductName          AS ProductName,
            p.StandardCost,
            p.ListPrice,
            p.DaysToManufacture,
            p.Color,
            p.Size,
            p.Weight,
            p.ProductLine,
            p.ModelName,
            p.EnglishDescription          AS ProductDescription,
            p.Status,
            ps.ProductSubcategoryKey,
            ps.EnglishProductSubcategoryName  AS SubcategoryName,
            pc.ProductCategoryKey,
            pc.EnglishProductCategoryName     AS CategoryName
        FROM dbo.DimProduct p
        LEFT JOIN dbo.DimProductSubcategory ps
            ON p.ProductSubcategoryKey = ps.ProductSubcategoryKey
        LEFT JOIN dbo.DimProductCategory pc
            ON ps.ProductCategoryKey = pc.ProductCategoryKey
    """,

    # -------------------------------------------------------------------------
    # DIMENSÃO: Cliente (com localização já resolvida no JOIN)
    # -------------------------------------------------------------------------
    "dim_customer": """
        SELECT
            c.CustomerKey,
            c.CustomerAlternateKey,
            c.FirstName,
            c.LastName,
            c.BirthDate,
            c.Gender,
            c.YearlyIncome,
            c.TotalChildren,
            c.NumberChildrenAtHome,
            c.EnglishEducation         AS Education,
            c.EnglishOccupation        AS Occupation,
            c.HouseOwnerFlag,
            c.NumberCarsOwned,
            c.DateFirstPurchase,
            c.CommuteDistance,
            g.City,
            g.StateProvinceName        AS State,
            g.EnglishCountryRegionName AS Country,
            g.PostalCode,
            g.SalesTerritoryKey
        FROM dbo.DimCustomer c
        LEFT JOIN dbo.DimGeography g
            ON c.GeographyKey = g.GeographyKey
    """,

    # -------------------------------------------------------------------------
    # DIMENSÃO: Revendedor (canal B2B)
    # -------------------------------------------------------------------------
    "dim_reseller": """
        SELECT
            r.ResellerKey,
            r.ResellerAlternateKey,
            r.ResellerName,
            r.BusinessType,
            r.NumberEmployees,
            r.OrderFrequency,
            r.ProductLine,
            r.AddressLine1,
            g.City,
            g.StateProvinceName        AS State,
            g.EnglishCountryRegionName AS Country,
            g.PostalCode
        FROM dbo.DimReseller r
        LEFT JOIN dbo.DimGeography g
            ON r.GeographyKey = g.GeographyKey
    """,

    # -------------------------------------------------------------------------
    # DIMENSÃO: Território de vendas
    # -------------------------------------------------------------------------
    "dim_territory": """
        SELECT
            SalesTerritoryKey,
            SalesTerritoryAlternateKey,
            SalesTerritoryRegion   AS Region,
            SalesTerritoryCountry  AS Country,
            SalesTerritoryGroup    AS TerritoryGroup
        FROM dbo.DimSalesTerritory
    """,

    # -------------------------------------------------------------------------
    # DIMENSÃO: Data (filtrada para o período com dados de venda)
    # -------------------------------------------------------------------------
    "dim_date": """
        SELECT
            DateKey,
            FullDateAlternateKey       AS FullDate,
            DayNumberOfWeek,
            EnglishDayNameOfWeek       AS DayName,
            DayNumberOfMonth,
            DayNumberOfYear,
            WeekNumberOfYear           AS WeekNumber,
            EnglishMonthName           AS MonthName,
            MonthNumberOfYear          AS MonthNumber,
            CalendarQuarter            AS Quarter,
            CalendarYear               AS Year,
            CalendarSemester           AS Semester,
            FiscalQuarter,
            FiscalYear,
            FiscalSemester
        FROM dbo.DimDate
        WHERE CalendarYear BETWEEN 2010 AND 2014
    """,

    # -------------------------------------------------------------------------
    # DIMENSÃO: Promoções (útil para análise futura de desconto)
    # -------------------------------------------------------------------------
    "dim_promotion": """
        SELECT
            PromotionKey,
            PromotionAlternateKey,
            EnglishPromotionName       AS PromotionName,
            EnglishPromotionType       AS PromotionType,
            EnglishPromotionCategory   AS PromotionCategory,
            DiscountPct,
            StartDate,
            EndDate,
            MinQty,
            MaxQty
        FROM dbo.DimPromotion
    """
}

# -----------------------------------------------------------------------------
# 5. FUNÇÃO PRINCIPAL DE EXTRAÇÃO
# -----------------------------------------------------------------------------

def extract_table(
    conn: pyodbc.Connection,
    table_name: str,
    query: str
) -> pd.DataFrame:
    """
    Executa query no SQL Server, retorna DataFrame e salva como .parquet.

    Parâmetros
    ----------
    conn       : conexão ativa com o SQL Server
    table_name : nome usado para o arquivo .parquet (ex: 'fact_internet_sales')
    query      : SQL a ser executado

    Retorna
    -------
    DataFrame com os dados extraídos
    """
    output_path = RAW_DIR / f"{table_name}.parquet"

    logger.info(f"Iniciando extração | tabela={table_name}")
    start_time = datetime.now()

    try:
        df = pd.read_sql(query, conn)
        elapsed = (datetime.now() - start_time).total_seconds()

        # Salva como parquet — mais rápido e compacto que CSV
        # engine='pyarrow' garante compatibilidade com pandas e spark
        df.to_parquet(output_path, index=False, engine="pyarrow")

        logger.info(
            f"Extração concluída | tabela={table_name} | "
            f"linhas={len(df):,} | colunas={len(df.columns)} | "
            f"arquivo={output_path.name} | tempo={elapsed:.2f}s"
        )
        return df

    except Exception as e:
        logger.error(f"Falha na extração | tabela={table_name} | erro={e}")
        raise


def run_extraction() -> dict:
    """
    Orquestra a extração de todas as tabelas definidas em QUERIES.
    Retorna dicionário com os DataFrames extraídos para uso imediato.
    """
    logger.info("=" * 65)
    logger.info("INÍCIO DO PIPELINE — CAMADA RAW")
    logger.info(f"Tabelas a extrair : {len(QUERIES)}")
    logger.info(f"Destino           : {RAW_DIR}")
    logger.info("=" * 65)

    pipeline_start = datetime.now()
    extracted = {}
    failed = []

    conn = get_connection()

    try:
        for table_name, query in QUERIES.items():
            try:
                df = extract_table(conn, table_name, query)
                extracted[table_name] = df
            except Exception:
                # Registra falha mas continua as demais tabelas
                failed.append(table_name)
                logger.warning(f"Tabela ignorada por erro | tabela={table_name}")
    finally:
        conn.close()
        logger.info("Conexão encerrada")

    # Resumo final da execução
    total_elapsed = (datetime.now() - pipeline_start).total_seconds()
    total_rows = sum(len(df) for df in extracted.values())

    logger.info("=" * 65)
    logger.info("RESUMO — CAMADA RAW")
    logger.info(f"Tabelas extraídas : {len(extracted)}/{len(QUERIES)}")
    logger.info(f"Total de linhas   : {total_rows:,}")
    logger.info(f"Tempo total       : {total_elapsed:.2f}s")

    if failed:
        logger.warning(f"Tabelas com falha  : {failed}")
    else:
        logger.info("Status            : SUCESSO COMPLETO")

    logger.info("=" * 65)

    return extracted


# -----------------------------------------------------------------------------
# 6. PONTO DE ENTRADA
# -----------------------------------------------------------------------------

if __name__ == "__main__":
    extracted_data = run_extraction()

    # Preview rápido no terminal após extração — útil para validação visual
    print("\n📦 Preview dos dados extraídos:\n")
    for name, df in extracted_data.items():
        print(f"  {name:<30} {len(df):>7,} linhas  |  {list(df.columns[:4])} ...")