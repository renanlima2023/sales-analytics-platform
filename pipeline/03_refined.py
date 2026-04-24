# =============================================================================
# Sales Analytics Platform
# Camada : REFINED — Modelagem Dimensional
# Script : 03_refined.py
# Autor  : Seu Nome
# Desc   : Lê os .parquet da camada Trusted, constrói o Star Schema
#          e popula o banco SQLite analítico. Esse banco é a fonte
#          direta do Power BI.
# =============================================================================

import logging
import sqlite3
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime

# -----------------------------------------------------------------------------
# 1. CONFIGURAÇÃO
# -----------------------------------------------------------------------------

ROOT_DIR     = Path(__file__).resolve().parent.parent
TRUSTED_DIR  = ROOT_DIR / "data" / "trusted"
REFINED_DIR  = ROOT_DIR / "data" / "refined"
LOG_DIR      = ROOT_DIR / "logs"

REFINED_DIR.mkdir(parents=True, exist_ok=True)

DB_PATH = REFINED_DIR / "sales_analytics.db"

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

def read_trusted(table_name: str) -> pd.DataFrame:
    """Lê um .parquet da camada Trusted."""
    path = TRUSTED_DIR / f"{table_name}.parquet"
    df = pd.read_parquet(path, engine="pyarrow")
    logger.info(f"Lido da Trusted | tabela={table_name} | linhas={len(df):,}")
    return df


def save_to_db(
    df: pd.DataFrame,
    table_name: str,
    conn: sqlite3.Connection,
    if_exists: str = "replace"
) -> None:
    """
    Salva DataFrame no banco SQLite.
    if_exists='replace' recria a tabela a cada execução —
    garante idempotência (pode rodar N vezes com segurança).
    """
    # SQLite não suporta todos os tipos do pandas — converte categoricals
    df = df.copy()
    for col in df.select_dtypes(include="category").columns:
        df[col] = df[col].astype(str)

    # Converte datetime para string ISO — SQLite armazena como TEXT
    for col in df.select_dtypes(include=["datetime64"]).columns:
        df[col] = df[col].dt.strftime("%Y-%m-%d").where(df[col].notna(), None)

    df.to_sql(table_name, conn, if_exists=if_exists, index=False)
    logger.info(f"Salvo no banco | tabela={table_name} | linhas={len(df):,}")


# -----------------------------------------------------------------------------
# 3. CONSTRUÇÃO DAS DIMENSÕES
# -----------------------------------------------------------------------------

def build_dim_tempo(df_date: pd.DataFrame) -> pd.DataFrame:
    """
    Constrói dim_tempo a partir da dim_date tratada.
    Renomeia para português — padrão do projeto.
    """
    logger.info("Construindo dim_tempo...")

    dim = df_date[[
        "DateKey", "FullDate", "DayNumberOfMonth", "DayName",
        "WeekNumber", "MonthName", "MonthNumber", "Quarter",
        "Semester", "Year", "AnoMes", "AnoTrimestre", "Flag_Q4"
    ]].copy()

    dim = dim.rename(columns={
        "DateKey"          : "TempoKey",
        "FullDate"         : "Data",
        "DayNumberOfMonth" : "DiaMes",
        "DayName"          : "NomeDia",
        "WeekNumber"       : "Semana",
        "MonthName"        : "NomeMes",
        "MonthNumber"      : "Mes",
        "Quarter"          : "Trimestre",
        "Semester"         : "Semestre",
        "Year"             : "Ano",
    })

    logger.info(f"dim_tempo | {len(dim):,} registros | {dim['Ano'].min()}–{dim['Ano'].max()}")
    return dim


def build_dim_produto(df_product: pd.DataFrame) -> pd.DataFrame:
    """
    Constrói dim_produto com hierarquia de categoria completa.
    Inclui margem potencial calculada na camada Trusted.
    """
    logger.info("Construindo dim_produto...")

    dim = df_product[[
        "ProductKey", "ProductName", "ModelName", "ProductLine",
        "Color", "Size", "StandardCost", "ListPrice",
        "SubcategoryName", "CategoryName", "Status",
        "Margem_Potencial_Pct"
    ]].copy()

    dim = dim.rename(columns={
        "ProductKey"          : "ProdutoKey",
        "ProductName"         : "NomeProduto",
        "ModelName"           : "Modelo",
        "ProductLine"         : "LinhaProduto",
        "Color"               : "Cor",
        "Size"                : "Tamanho",
        "StandardCost"        : "CustoPadrao",
        "ListPrice"           : "PrecoLista",
        "SubcategoryName"     : "Subcategoria",
        "CategoryName"        : "Categoria",
        "Status"              : "StatusProduto",
        "Margem_Potencial_Pct": "MargemPotencial_Pct",
    })

    logger.info(f"dim_produto | {len(dim):,} produtos | {dim['Categoria'].nunique()} categorias")
    return dim


def build_dim_cliente(df_customer: pd.DataFrame) -> pd.DataFrame:
    """
    Constrói dim_cliente para o canal B2C (Internet Sales).
    Inclui segmentação por faixa de renda.
    """
    logger.info("Construindo dim_cliente...")

    dim = df_customer[[
        "CustomerKey", "NomeCompleto", "Gender", "BirthDate",
        "Education", "Occupation", "YearlyIncome", "FaixaRenda",
        "NumberCarsOwned", "HouseOwnerFlag",
        "City", "State", "Country", "PostalCode",
        "SalesTerritoryKey", "DateFirstPurchase"
    ]].copy()

    dim = dim.rename(columns={
        "CustomerKey"      : "ClienteKey",
        "NomeCompleto"     : "NomeCliente",
        "Gender"           : "Genero",
        "BirthDate"        : "DataNascimento",
        "Education"        : "Escolaridade",
        "Occupation"       : "Ocupacao",
        "YearlyIncome"     : "RendaAnual",
        "FaixaRenda"       : "FaixaRenda",
        "NumberCarsOwned"  : "QtdCarros",
        "HouseOwnerFlag"   : "ProprietarioImovel",
        "City"             : "Cidade",
        "State"            : "Estado",
        "Country"          : "Pais",
        "PostalCode"       : "CEP",
        "SalesTerritoryKey": "TerritorioKey",
        "DateFirstPurchase": "DataPrimeiraCompra",
    })

    logger.info(f"dim_cliente | {len(dim):,} clientes | {dim['Pais'].nunique()} países")
    return dim


def build_dim_revendedor(df_reseller: pd.DataFrame) -> pd.DataFrame:
    """
    Constrói dim_revendedor para o canal B2B (Reseller Sales).
    """
    logger.info("Construindo dim_revendedor...")

    dim = df_reseller[[
        "ResellerKey", "ResellerName", "BusinessType",
        "NumberEmployees", "ProductLine",
        "City", "State", "Country", "PostalCode"
    ]].copy()

    dim = dim.rename(columns={
        "ResellerKey"     : "RevendedorKey",
        "ResellerName"    : "NomeRevendedor",
        "BusinessType"    : "TipoNegocio",
        "NumberEmployees" : "QtdFuncionarios",
        "ProductLine"     : "LinhaProduto",
        "City"            : "Cidade",
        "State"           : "Estado",
        "Country"         : "Pais",
        "PostalCode"      : "CEP",
    })

    logger.info(f"dim_revendedor | {len(dim):,} revendedores")
    return dim


def build_dim_territorio(df_territory: pd.DataFrame) -> pd.DataFrame:
    """
    Constrói dim_territorio — dimensão compartilhada entre B2C e B2B.
    """
    logger.info("Construindo dim_territorio...")

    dim = df_territory[[
        "SalesTerritoryKey", "Region", "Country", "TerritoryGroup"
    ]].copy()

    dim = dim.rename(columns={
        "SalesTerritoryKey" : "TerritorioKey",
        "Region"            : "Regiao",
        "Country"           : "Pais",
        "TerritoryGroup"    : "GrupoTerritorio",
    })

    logger.info(f"dim_territorio | {len(dim):,} territórios")
    return dim


def build_dim_canal() -> pd.DataFrame:
    """
    Cria dim_canal — dimensão derivada, não existe no AdventureWorks.
    Representa o modelo de venda: B2C (Internet) ou B2B (Reseller).
    Esse é o diferencial multicanal do projeto.
    """
    logger.info("Construindo dim_canal (derivada)...")

    dim = pd.DataFrame({
        "CanalKey"      : [1, 2],
        "Canal"         : ["Internet", "Reseller"],
        "ModeloVenda"   : ["B2C", "B2B"],
        "Descricao"     : [
            "Venda direta ao consumidor via canal digital",
            "Venda por revendedor autorizado (atacado/distribuição)"
        ],
        "Margem_Perfil" : ["Alta margem / Menor volume", "Baixa margem / Alto volume"]
    })

    logger.info(f"dim_canal | {len(dim)} canais")
    return dim


# -----------------------------------------------------------------------------
# 4. CONSTRUÇÃO DA FATO VENDAS
# -----------------------------------------------------------------------------

def build_fato_vendas(
    df_fato     : pd.DataFrame,
    dim_canal   : pd.DataFrame,
) -> pd.DataFrame:
    """
    Constrói fato_vendas final com todas as chaves do Star Schema.

    Adiciona CanalKey via join com dim_canal para garantir
    integridade referencial no modelo dimensional.
    """
    logger.info("Construindo fato_vendas...")

    # Join para resolver CanalKey
    df = df_fato.merge(
        dim_canal[["CanalKey", "Canal"]],
        on="Canal",
        how="left"
    )

    # Seleciona e renomeia colunas para o modelo final
    fato = df[[
        "VendaKey",
        "OrderDateKey",       # FK → dim_tempo
        "ProductKey",         # FK → dim_produto
        "ClienteKey",         # FK → dim_cliente (B2C) ou dim_revendedor (B2B)
        "SalesTerritoryKey",  # FK → dim_territorio
        "CanalKey",           # FK → dim_canal
        "SalesOrderNumber",
        "SalesOrderLineNumber",
        "OrderQuantity",
        "UnitPrice",
        "DiscountAmount",
        "ProductStandardCost",
        "TotalProductCost",
        "SalesAmount",
        "TaxAmt",
        "Freight",
        "Lucro",
        "Margem_Pct",
        "Ticket_Medio",
        "Receita_Liquida",
        "Canal",
        "Ano",
        "Mes",
        "Trimestre",
        "OrderDate",
        "ShipDate",
    ]].copy()

    fato = fato.rename(columns={
        "VendaKey"           : "VendaKey",
        "OrderDateKey"       : "TempoKey",
        "ProductKey"         : "ProdutoKey",
        "ClienteKey"         : "ClienteKey",
        "SalesTerritoryKey"  : "TerritorioKey",
        "SalesOrderNumber"   : "NumeroPedido",
        "SalesOrderLineNumber": "LinhaPedido",
        "OrderQuantity"      : "Quantidade",
        "UnitPrice"          : "PrecoUnitario",
        "DiscountAmount"     : "Desconto",
        "ProductStandardCost": "CustoPadrao",
        "TotalProductCost"   : "CustoTotal",
        "SalesAmount"        : "Receita",
        "TaxAmt"             : "Imposto",
        "Freight"            : "Frete",
        "Lucro"              : "Lucro",
        "Margem_Pct"         : "Margem_Pct",
        "Ticket_Medio"       : "TicketMedio",
        "Receita_Liquida"    : "ReceitaLiquida",
        "Canal"              : "Canal",
        "OrderDate"          : "DataPedido",
        "ShipDate"           : "DataEnvio",
    })

    logger.info(
        f"fato_vendas | {len(fato):,} registros | "
        f"Receita=R${fato['Receita'].sum():,.0f} | "
        f"Lucro=R${fato['Lucro'].sum():,.0f}"
    )

    return fato


# -----------------------------------------------------------------------------
# 5. CURVA ABC — DIFERENCIAL PREMIUM
# -----------------------------------------------------------------------------

def build_curva_abc(
    fato        : pd.DataFrame,
    dim_produto : pd.DataFrame
) -> pd.DataFrame:
    """
    Calcula a Curva ABC de produtos por receita acumulada.

    Metodologia:
    - Classe A: produtos que acumulam até 80% da receita
    - Classe B: de 80% a 95%
    - Classe C: acima de 95% (cauda longa)

    Por que fazer aqui e não no DAX?
    A classificação ABC exige ordenação e cálculo de percentual
    acumulado — operações que o Python/pandas faz com muito mais
    clareza e performance do que DAX. O resultado entra no banco
    como uma tabela pronta, e o Power BI só exibe.
    """
    logger.info("Calculando Curva ABC de produtos...")

    # Agrega receita por produto
    abc = (
        fato.groupby("ProdutoKey")["Receita"]
        .sum()
        .reset_index()
        .sort_values("Receita", ascending=False)
    )

    # Percentual acumulado
    abc["Receita_Pct"]     = abc["Receita"] / abc["Receita"].sum() * 100
    abc["Receita_Pct_Acum"] = abc["Receita_Pct"].cumsum()

    # Classificação
    def classificar(pct_acum):
        if pct_acum <= 80:
            return "A"
        elif pct_acum <= 95:
            return "B"
        else:
            return "C"

    abc["Classe_ABC"] = abc["Receita_Pct_Acum"].apply(classificar)
    abc["Rank"]       = range(1, len(abc) + 1)

    # Enriquece com nome e categoria do produto
    abc = abc.merge(
        dim_produto[["ProdutoKey", "NomeProduto", "Categoria", "Subcategoria"]],
        on="ProdutoKey",
        how="left"
    )

    # Resumo para o log
    resumo = abc.groupby("Classe_ABC").agg(
        Produtos=("ProdutoKey", "count"),
        Receita=("Receita", "sum"),
        Pct_Receita=("Receita_Pct", "sum")
    ).round(2)

    logger.info(f"Curva ABC calculada | {len(abc)} produtos classificados")
    for classe, row in resumo.iterrows():
        logger.info(
            f"  Classe {classe}: {row['Produtos']} produtos | "
            f"R${row['Receita']:,.0f} | {row['Pct_Receita']:.1f}% da receita"
        )

    return abc


# -----------------------------------------------------------------------------
# 6. SAZONALIDADE — DIFERENCIAL PREMIUM
# -----------------------------------------------------------------------------

def build_sazonalidade(fato: pd.DataFrame) -> pd.DataFrame:
    """
    Agrega receita e volume por ano/trimestre e por canal.
    Essa tabela alimenta diretamente os visuais de sazonalidade
    e o comparativo de crescimento no Power BI.
    """
    logger.info("Calculando tabela de sazonalidade...")

    saz = (
        fato.groupby(["Ano", "Trimestre", "Canal"])
        .agg(
            Receita        = ("Receita", "sum"),
            Lucro          = ("Lucro", "sum"),
            QtdPedidos     = ("VendaKey", "count"),
            TicketMedio    = ("TicketMedio", "mean"),
            Margem_Pct     = ("Margem_Pct", "mean"),
        )
        .reset_index()
        .round(2)
    )

    # Label para eixo do gráfico
    saz["Periodo"] = saz["Ano"].astype(str) + " Q" + saz["Trimestre"].astype(str)

    logger.info(f"Sazonalidade | {len(saz):,} períodos x canais")
    return saz


# -----------------------------------------------------------------------------
# 7. ORQUESTRADOR DA CAMADA REFINED
# -----------------------------------------------------------------------------

def run_refined() -> None:
    """
    Executa a construção completa do Star Schema no SQLite.
    Ordem de carga: dimensões primeiro, fato por último.
    Isso é boa prática de DW — garante integridade referencial.
    """
    logger.info("=" * 65)
    logger.info("INÍCIO DO PIPELINE — CAMADA REFINED")
    logger.info(f"Banco de dados      : {DB_PATH}")
    logger.info("=" * 65)

    pipeline_start = datetime.now()

    # --- Lê dados da camada Trusted ---
    df_fato_trusted = read_trusted("fato_vendas")
    df_product      = read_trusted("dim_product")
    df_customer     = read_trusted("dim_customer")
    df_reseller     = read_trusted("dim_reseller")
    df_territory    = read_trusted("dim_territory")
    df_date         = read_trusted("dim_date")

    # --- Constrói dimensões ---
    dim_tempo       = build_dim_tempo(df_date)
    dim_produto     = build_dim_produto(df_product)
    dim_cliente     = build_dim_cliente(df_customer)
    dim_revendedor  = build_dim_revendedor(df_reseller)
    dim_territorio  = build_dim_territorio(df_territory)
    dim_canal       = build_dim_canal()

    # --- Constrói fato ---
    fato_vendas     = build_fato_vendas(df_fato_trusted, dim_canal)

    # --- Diferenciais analíticos ---
    curva_abc       = build_curva_abc(fato_vendas, dim_produto)
    sazonalidade    = build_sazonalidade(fato_vendas)

    # --- Salva no banco SQLite ---
    # IMPORTANTE: dimensões primeiro, fato por último
    logger.info("Iniciando carga no banco SQLite...")

    conn = sqlite3.connect(DB_PATH)

    try:
        # Dimensões
        save_to_db(dim_tempo,      "dim_tempo",      conn)
        save_to_db(dim_produto,    "dim_produto",     conn)
        save_to_db(dim_cliente,    "dim_cliente",     conn)
        save_to_db(dim_revendedor, "dim_revendedor",  conn)
        save_to_db(dim_territorio, "dim_territorio",  conn)
        save_to_db(dim_canal,      "dim_canal",       conn)

        # Fato central
        save_to_db(fato_vendas,    "fato_vendas",     conn)

        # Tabelas analíticas (diferenciais premium)
        save_to_db(curva_abc,      "analytics_curva_abc",   conn)
        save_to_db(sazonalidade,   "analytics_sazonalidade", conn)

        conn.commit()

    finally:
        conn.close()

    # --- Resumo final ---
    total_elapsed = (datetime.now() - pipeline_start).total_seconds()

    logger.info("=" * 65)
    logger.info("RESUMO — CAMADA REFINED")
    logger.info(f"Tabelas no banco    : 9 (6 dimensões + 1 fato + 2 analíticas)")
    logger.info(f"Fato vendas         : {len(fato_vendas):,} registros")
    logger.info(f"Receita total       : R$ {fato_vendas['Receita'].sum():,.2f}")
    logger.info(f"Banco gerado        : {DB_PATH.name}")
    logger.info(f"Tempo total         : {total_elapsed:.2f}s")
    logger.info("Status              : SUCESSO COMPLETO")
    logger.info("=" * 65)


# -----------------------------------------------------------------------------
# 8. PONTO DE ENTRADA
# -----------------------------------------------------------------------------

if __name__ == "__main__":
    run_refined()

    # Validação final — lê direto do banco e confirma integridade
    print("\n✅ Validação do banco SQLite:\n")
    conn = sqlite3.connect(DB_PATH)

    tabelas = pd.read_sql(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name",
        conn
    )

    for tabela in tabelas["name"]:
        count = pd.read_sql(f"SELECT COUNT(*) AS n FROM {tabela}", conn)["n"][0]
        print(f"  {tabela:<35} {count:>8,} registros")

    # Preview da Curva ABC — insight imediato
    print("\n📊 Curva ABC — resumo por classe:\n")
    abc_resumo = pd.read_sql("""
        SELECT
            Classe_ABC,
            COUNT(*)        AS Produtos,
            SUM(Receita)    AS Receita_Total,
            AVG(Receita_Pct_Acum) AS Pct_Acumulado
        FROM analytics_curva_abc
        GROUP BY Classe_ABC
        ORDER BY Classe_ABC
    """, conn)

    for _, row in abc_resumo.iterrows():
        print(
            f"  Classe {row['Classe_ABC']} | "
            f"{int(row['Produtos'])} produtos | "
            f"R$ {row['Receita_Total']:,.0f}"
        )

    conn.close()