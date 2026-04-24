# Sales Analytics Platform

> Pipeline de dados end-to-end com inteligência de vendas multicanal — AdventureWorksDW2019 · Python · SQL Server · Power BI

![Status](https://img.shields.io/badge/status-em%20desenvolvimento-yellow)
![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python&logoColor=white)
![SQL Server](https://img.shields.io/badge/SQL%20Server-2019-red?logo=microsoftsqlserver&logoColor=white)
![Power BI](https://img.shields.io/badge/Power%20BI-Dashboard-F2C811?logo=powerbi&logoColor=black)
![License](https://img.shields.io/badge/license-MIT-green)

---

## Problema de negócio

A empresa enfrenta dificuldades para monitorar a performance de vendas em tempo hábil, identificar oportunidades de crescimento por canal e agir sobre quedas de receita — devido à ausência de uma solução centralizada e automatizada de dados.

Este projeto entrega uma plataforma de analytics construída sobre dados reais de vendas (AdventureWorksDW2019), com pipeline Python estruturado, modelagem dimensional própria e dashboard executivo no Power BI — cobrindo os canais B2C (Internet Sales) e B2B (Reseller Sales).

---

## Arquitetura

```
SQL Server (AdventureWorksDW2019)
        │
        ▼  pyodbc + sqlalchemy
┌───────────────┐
│   RAW Layer   │  Extração bruta · Parquet · Log de execução
└───────┬───────┘
        │
        ▼
┌───────────────┐
│ TRUSTED Layer │  Limpeza · Tipagem · Joins analíticos · Colunas derivadas
└───────┬───────┘
        │
        ▼
┌───────────────┐
│ REFINED Layer │  Star Schema · SQLite · Fato + Dimensões
└───────┬───────┘
        │
        ▼
┌───────────────┐
│    Power BI   │  4 páginas · DAX · Storytelling · Publicado no PBI Service
└───────────────┘
```

---

## Modelagem dimensional

**Star Schema** construído a partir das tabelas-fonte do AdventureWorksDW2019:

```
                    ┌─────────────┐
                    │  dim_tempo  │
                    └──────┬──────┘
                           │
┌─────────────┐   ┌────────┴────────┐   ┌──────────────┐
│ dim_produto │───│   fato_vendas   │───│  dim_cliente │
└─────────────┘   └────────┬────────┘   └──────────────┘
                           │
              ┌────────────┴────────────┐
              │                         │
       ┌──────┴──────┐         ┌────────┴──────┐
       │  dim_regiao │         │   dim_canal   │
       └─────────────┘         └───────────────┘
```

| Tabela | Origem | Descrição |
|---|---|---|
| `fato_vendas` | FactInternetSales + FactResellerSales | Métricas de vendas unificadas por canal |
| `dim_produto` | DimProduct + SubCategory + Category | Hierarquia completa de produtos |
| `dim_cliente` | DimCustomer + DimGeography | Perfil e localização do cliente B2C |
| `dim_regiao` | DimSalesTerritory + DimGeography | Território de vendas B2B e B2C |
| `dim_tempo` | DimDate | Hierarquia temporal (ano, trimestre, mês) |
| `dim_canal` | Derivada | Internet vs Reseller |

---

## Pipeline de dados

| Script | Camada | Responsabilidade |
|---|---|---|
| `pipeline/01_extract.py` | Raw | Extração via SQL Server, salva `.parquet`, gera log |
| `pipeline/02_trusted.py` | Trusted | Limpeza, tipagem, joins, colunas derivadas |
| `pipeline/03_refined.py` | Refined | Cria Star Schema no SQLite |
| `pipeline/run_pipeline.py` | Orquestração | Executa as 3 etapas em sequência |

Para executar o pipeline completo:

```bash
python pipeline/run_pipeline.py
```

---

## KPIs e métricas

### Executivos
| KPI | Descrição |
|---|---|
| Receita Total | Soma de `SalesAmount` — visão consolidada |
| Lucro Total | Receita − Custo |
| Margem % | Lucro / Receita |
| Crescimento MoM | Variação percentual mês a mês |

### Analíticos
| KPI | Descrição |
|---|---|
| Receita por Canal | Internet vs Reseller — comparativo de margem |
| Curva ABC | Classificação de produtos por contribuição de receita |
| Ticket Médio | Receita / Quantidade de pedidos |
| Sazonalidade | Padrão de receita por trimestre ao longo dos anos |

---

## Diferenciais analíticos

### Curva ABC de produtos
Classificação automática por contribuição acumulada de receita:
- **Classe A** — produtos que representam 80% da receita
- **Classe B** — próximos 15%
- **Classe C** — cauda longa (últimos 5%)

### Análise multicanal
Comparativo entre canal Internet (B2C) e Reseller (B2B):
- Margem por canal
- Sazonalidade por canal
- Ticket médio por canal

### Sazonalidade
Identificação de padrões trimestrais com base em 4 anos de dados (2010–2014):
- Q4 consistentemente maior
- Crescimento explosivo de 2013 documentado e explicado

---

## Dashboard — Power BI

| Página | Conteúdo |
|---|---|
| Executive Overview | KPIs principais, receita ao longo do tempo, Meta vs Realizado |
| Produtos | Curva ABC, ranking, margem por categoria |
| Regional | Mapa de receita por território, comparativo por região |
| Insights | Storytelling — conclusões em linguagem de negócio |

🔗 **[Acessar dashboard no Power BI Service](#)** ← link será atualizado após publicação

---

## Tecnologias

| Tecnologia | Uso |
|---|---|
| Python 3.11 | Pipeline de dados (extração, transformação, carga) |
| SQL Server 2019 | Fonte de dados — AdventureWorksDW2019 |
| pandas | Manipulação e transformação de dados |
| pyodbc / sqlalchemy | Conexão com SQL Server |
| SQLite | Banco analítico — Star Schema final |
| Power BI Desktop | Modelagem DAX e visualização |
| Power BI Service | Publicação e compartilhamento |
| Git / GitHub | Versionamento e portfólio |

---

## Como executar

### Pré-requisitos
- Python 3.9+
- SQL Server com AdventureWorksDW2019 restaurado
- ODBC Driver 17 for SQL Server instalado
- Power BI Desktop (para o dashboard)

### Instalação

```bash
# Clone o repositório
git clone https://github.com/renanlima2023/sales-analytics-platform.git
cd sales-analytics-platform

# Crie e ative o ambiente virtual
python -m venv venv
venv\Scripts\activate  # Windows

# Instale as dependências
pip install -r requirements.txt
```

### Configuração

Crie o arquivo `.env` na raiz do projeto:

```env
DB_SERVER=localhost
DB_NAME=AdventureWorksDW2019
DB_DRIVER=ODBC Driver 17 for SQL Server
```

### Execução

```bash
# Executa o pipeline completo
python pipeline/run_pipeline.py

# Ou etapa por etapa
python pipeline/01_extract.py
python pipeline/02_trusted.py
python pipeline/03_refined.py
```

---

## Estrutura do projeto

```
sales-analytics-platform/
├── .env                        # Credenciais locais (não versionado)
├── .gitignore
├── requirements.txt
├── README.md
├── data/
│   ├── raw/                    # Parquets extraídos do SQL Server
│   ├── trusted/                # Dados tratados e enriquecidos
│   └── refined/                # Banco SQLite com Star Schema
├── pipeline/
│   ├── 01_extract.py           # Extração do SQL Server
│   ├── 02_trusted.py           # Transformações analíticas
│   ├── 03_refined.py           # Carga no banco analítico
│   └── run_pipeline.py         # Orquestrador
├── sql/
│   └── queries/                # Queries de exploração e validação
├── notebooks/
│   └── eda.ipynb               # Análise exploratória inicial
├── powerbi/
│   └── sales_platform.pbix     # Dashboard local (não versionado)
├── logs/
│   └── pipeline.log            # Gerado automaticamente
└── docs/
    └── architecture.png        # Diagrama de arquitetura
```

---

## Principais insights encontrados

- **Receita total:** R$ 109,8M | Margem média: 30,3% | Ticket médio: R$ 464
- **Canal Internet (B2C):** R$ 29,4M — margem de 53%
- **Canal Reseller (B2B):** R$ 80,4M — margem de apenas 7,4%
- **Curva ABC:** 87 produtos (25% do catálogo) geram 80% da receita
- **Concentração:** Top 10 produtos são todos Bikes
- **Crescimento:** Receita cresceu 130% entre 2011 e 2013

---

## Status do projeto

- [x] Setup do ambiente e repositório
- [x] Reconhecimento e mapeamento do banco de dados
- [x] Pipeline — camada Raw (extração) — 8 tabelas · 142.897 linhas · 3,58s
- [x] Pipeline — camada Trusted (transformação) — fato unificada 121.253 linhas
- [x] Pipeline — camada Refined (modelagem) — Star Schema · 9 tabelas · 0,61s
- [ ] Dashboard Power BI — 4 páginas
- [ ] Publicação no Power BI Service
- [ ] Documentação final

---

## Autor

**Renan Lima**
Analista de Dados | 16 anos em TI · Transição para BI & Analytics

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-0A66C2?logo=linkedin)](https://linkedin.com/in/renan-slima)
[![GitHub](https://img.shields.io/badge/GitHub-Portfolio-181717?logo=github)](https://github.com/renanlima2023)

---

*Projeto desenvolvido para portfólio profissional com dados do AdventureWorksDW2019 (Microsoft).*