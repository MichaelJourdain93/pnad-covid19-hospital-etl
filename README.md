<details name="lang-toggle">
<summary><b>🇺🇸 English</b></summary>

# 📊 PNAD-COVID-19: ETL and Analysis with PySpark for Hospital Preparedness

![Python](https://img.shields.io/badge/Python-3.x-3776AB?logo=python&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-Big%20Data-E25A1C?logo=apachespark&logoColor=white)
![GeoPandas](https://img.shields.io/badge/GeoPandas-Geospatial-2E8B57)
![Jupyter](https://img.shields.io/badge/Jupyter-Notebook-F37626?logo=jupyter&logoColor=white)
![Airflow](https://img.shields.io/badge/Airflow-Orchestration%20(planned)-017CEE?logo=apacheairflow&logoColor=white)
![Status](https://img.shields.io/badge/Status-Completed-brightgreen)

##
![Author](https://img.shields.io/badge/Author-Michael%20Jourdain%20Gbedjinou-lightgrey?style=for-the-badge)
## 🎯 Business problem

Understand how the Brazilian population behaved during the COVID-19 pandemic and **which indicators would be relevant for hospital planning**, in case of a new outbreak.

The project simulates the role of a data team that receives a real hospital management problem and needs to turn raw microdata from the **PNAD-COVID-19 (IBGE)** household survey into actionable indicators, covering three dimensions: clinical symptoms, population behavior, and socioeconomic characteristics.

**Predefined project constraints:**
- Use at most 20 questions from the original survey
- 3-month data window (Sep/Oct/Nov 2020)
- Coverage of: clinical symptoms, population behavior, and economic characteristics


## 🏗️ Architecture / Data flow

```mermaid
flowchart LR
    subgraph Sources["Data sources (IBGE)"]
        A1[PNAD-COVID Sep/2020]
        A2[PNAD-COVID Oct/2020]
        A3[PNAD-COVID Nov/2020]
        A4[Variable dictionary]
        A5[State shapefiles]
    end

    subgraph ETL["ETL - PySpark"]
        B1[Read the 3 datasets\n~387k / ~380k / ~381k rows]
        B2[Select 20 questions\n145+ → 22 columns]
        B3[Semantic decoding\nUDFs: symptoms, comorbidities,\nage group, state, income]
        B4[Column standardization\nfact table creation]
        B5[Final consolidation\n~1.15M records]
    end

    subgraph Analysis["Analysis & Visualization"]
        C1[EDA - 3 sections:\nsociodemographic, clinical, economic]
        C2[Choropleth map by state\nGeoPandas + Shapefile]
        C3[Final report\nfindings + proposed actions]
    end

    subgraph Orch["Orchestration (planned)"]
        D1[Airflow DAG\nspark-submit ETL → validation → publication]
    end

    A1 & A2 & A3 --> B1 --> B2 --> B3 --> B4 --> B5
    A4 -.guides.-> B3
    B5 --> C1
    B5 --> C2
    A5 --> C2
    C1 & C2 --> C3
    B1 -.future.-> D1
```

## ⚙️ Execution phases

### 1. Ingestion — Data extraction
Reading the three raw PNAD-COVID-19 files (September, October, and November 2020) via **PySpark (`SparkSession`)**, totaling **387,298 + 380,461 + 381,438 records**, each with 145+ columns — most with a very high proportion of null values. This extraction gave an overview of the available content before modeling.

### 2. Transformation — Data manipulation
- **Creation of a fact table**, renaming columns according to the official PNAD dictionary.
- **Selection of 20 questions** (out of 145+ available), with explicit criteria of analytical relevance + low null ratio, to reduce bias in decision-making.
- **Semantic decoding via UDFs**: state, age group, sex, symptoms (fever, cough, shortness of breath, loss of smell/taste), comorbidities (hypertension, respiratory disease, heart disease, cancer), health insurance, COVID test, and socioeconomic variables (contact restriction, emergency aid, unemployment insurance, loan request).
- **Consolidation of the 3 months** into a single base of **~1,149,197 records**.

### 3. Orchestration
Today the pipeline runs manually, cell by cell, in a notebook. As the next step of the project, the ETL logic is being extracted into `.py` modules under `src/`, orchestrated by an **Airflow DAG** (`dags/pnad_covid_pipeline.py` — see the stub included in this repository) that simulates the `spark-submit ETL → schema validation → publication of the curated base` flow.

### 4. Visualization
Charts organized into **3 analytical sections** (sociodemographic, clinical symptoms, economic) using pandas/matplotlib/seaborn, plus a **choropleth map by state** (GeoPandas + IBGE shapefiles) to visualize the geographic distribution of indicators. Findings were consolidated into a final report with recommendations for hospital management.

## 🛠️ Tech stack and rationale

| Layer | Technology | Why |
|---|---|---|
| Distributed processing | **PySpark** (`pyspark.sql`, DataFrame API, UDFs) | Volume of +1M rows and need for transformation at scale — same logic used in production pipelines |
| Manipulation/EDA | **pandas, numpy** | Exploratory analysis after consolidation via Spark |
| Visualization | **matplotlib, seaborn** | Charts for the 3 analytical sections (sociodemographic, clinical, economic) |
| Geospatial | **GeoPandas + Shapefiles (IBGE)** | Choropleth map by state |
| Environment | **Jupyter Notebook** | Iterative exploration documented side by side with the code |
| Orchestration (planned) | **Apache Airflow** | Simulate the transition from an exploratory notebook to a schedulable/productizable pipeline |

## 💻 Running locally

```bash
git clone https://github.com/MichaelJourdain93/pnad-covid19-hospital-etl.git
cd pnad-covid19-hospital-etl

python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate

pip install -r requirements.txt

jupyter notebook notebooks/01_etl_pnad_covid.ipynb
```

> The raw PNAD-COVID-19 microdata is not versioned in Git. Download it directly from the [IBGE FTP](https://ftp.ibge.gov.br/Trabalho_e_Rendimento/Pesquisa_Nacional_por_Amostra_de_Domicilios_continua/Trabalho_e_garantia_de_renda_durante_a_pandemia_de_Covid-19/Microdados/) and place it in `data/raw/`.

## 📈 Results / Insights

- **Socioeconomic profile as a risk factor:** more than **60% of those hospitalized for COVID-19** belonged to the lowest income brackets — indicating that economic vulnerability is a relevant predictor of clinical severity.
- **Symptoms as an early signal:** fever, cough, and shortness of breath were the most prevalent symptoms, reported by **~10% of respondents**, reinforcing their value as key screening indicators.
- **Labor market shock:** unemployment reached **~14%** during the most critical months of the pandemic, concentrated among informal workers and the trade and services sectors.
- **Inequality cycle:** the analysis pointed to a circular effect — economic vulnerability reduces access to healthcare, which worsens clinical symptoms in poorer populations; Black and mixed-race groups faced greater difficulty accessing adequate health services.
- **Recommended actions for the hospital**, derived directly from the data:
  1. **Automated clinical triage** for fever, cough, and breathing difficulty, optimizing resource allocation.
  2. **Focus on vulnerable populations** (elderly, comorbidities, low income) via home care and telemedicine, reducing avoidable hospitalizations.
  3. **Partnerships for economic and social mitigation** (government/NGOs) — food baskets, medication assistance, and educational actions in underserved communities.


## 📁 Project structure

```
pnad-covid19-hospital-etl/
├── README.md
├── requirements.txt
├── .gitignore
├── notebooks/
│   └── PNAD_covid_19.ipynb
├── dags/
│   └── pnad_covid_pipeline.py         # Airflow orchestration stub
├── data/
│   └── raw/
│       └── SOURCES.md                 # official IBGE link to download the 3 raw datasets
├── reports/
│   └── mapa_calor_estados/
│       └── SOURCES.md                 # link to the shapefile used in the heat map
└── docs/
    ├── Fase3_Tech_Challenge_Grupo62.pdf
    ├── tech_challenge_briefing.txt    # original challenge statement
    ├── datasets_import_google_colab.png
    └── dicionario_pnad_covid/
        ├── Dicionario_PNAD_COVID_092020_20220621.xls
        ├── Dicionario_PNAD_COVID_102020_20220621.xls
        ├── Dicionario_PNAD_COVID_112020_20220621.xls
        └── de_para_variaveis_selecionadas.txt
```

</details>

<details open name="lang-toggle">
<summary><b>🇧🇷 Português</b></summary>

# 📊 PNAD-COVID-19: ETL e Análise com PySpark para Preparação Hospitalar

![Python](https://img.shields.io/badge/Python-3.x-3776AB?logo=python&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-Big%20Data-E25A1C?logo=apachespark&logoColor=white)
![GeoPandas](https://img.shields.io/badge/GeoPandas-Geoespacial-2E8B57)
![Jupyter](https://img.shields.io/badge/Jupyter-Notebook-F37626?logo=jupyter&logoColor=white)
![Airflow](https://img.shields.io/badge/Airflow-Orquestração%20(planejada)-017CEE?logo=apacheairflow&logoColor=white)
![Status](https://img.shields.io/badge/Status-Concluído-brightgreen)

## 
![Autor](https://img.shields.io/badge/Autor-Michael%20Jourdain%20Gbedjinou-lightgrey?style=for-the-badge)
## 🎯 Problema de negócio

Entender como foi o comportamento da população brasileira durante a pandemia de COVID-19 e **quais indicadores seriam relevantes para o planejamento hospitalar**, caso ocorra um novo surto da doença.

O projeto simula o papel de um time de dados que recebe um problema real de gestão hospitalar e precisa transformar microdados brutos da pesquisa domiciliar **PNAD-COVID-19 (IBGE)** em indicadores acionáveis, cobrindo três dimensões: sintomas clínicos, comportamento da população e características socioeconômicas.

**Pré-requisitos definidos para o projeto:**
- Uso de no máximo 20 perguntas da pesquisa original
- Janela de 3 meses de dados (set/out/nov de 2020)
- Cobertura de: sintomas clínicos, comportamento populacional e características econômicas


## 🏗️ Arquitetura / Fluxo de dados

```mermaid
flowchart LR
    subgraph Fontes["Fontes de dados (IBGE)"]
        A1[PNAD-COVID Set/2020]
        A2[PNAD-COVID Out/2020]
        A3[PNAD-COVID Nov/2020]
        A4[Dicionário de variáveis]
        A5[Shapefiles dos estados]
    end

    subgraph ETL["ETL - PySpark"]
        B1[Leitura dos 3 datasets\n~387k / ~380k / ~381k linhas]
        B2[Seleção de 20 perguntas\n145+ → 22 colunas]
        B3[Decodificação semântica\nUDFs: sintomas, comorbidades,\nfaixa etária, UF, renda]
        B4[Padronização de nomes\ncriação de tabela fato]
        B5[Consolidação final\n~1,15M registros]
    end

    subgraph Analise["Análise & Visualização"]
        C1[EDA - 3 seções:\nsociodemográfica, clínica, econômica]
        C2[Mapa de calor por estado\nGeoPandas + Shapefile]
        C3[Relatório final\nachados + ações propostas]
    end

    subgraph Orq["Orquestração (planejada)"]
        D1[DAG Airflow\nspark-submit ETL → validação → publicação]
    end

    A1 & A2 & A3 --> B1 --> B2 --> B3 --> B4 --> B5
    A4 -.orienta.-> B3
    B5 --> C1
    B5 --> C2
    A5 --> C2
    C1 & C2 --> C3
    B1 -.futuro.-> D1
```

## ⚙️ Fases de execução

### 1. Ingestão — Extração da base de dados
Leitura dos três arquivos brutos da PNAD-COVID-19 (setembro, outubro e novembro de 2020) via **PySpark (`SparkSession`)**, totalizando **387.298 + 380.461 + 381.438 registros**, cada um com 145+ colunas — a maioria com altíssima proporção de valores nulos. Essa extração deu visão geral do conteúdo disponível antes da modelagem.

### 2. Transformação — Manipulação dos dados
- **Criação de uma tabela fato**, renomeando colunas conforme o dicionário oficial da PNAD.
- **Seleção de 20 perguntas** (das 145+ disponíveis), com critério explícito de relevância analítica + baixa proporção de nulos, para reduzir viés na tomada de decisão.
- **Decodificação semântica via UDFs**: UF, faixa etária, sexo, sintomas (febre, tosse, falta de ar, perda de olfato/paladar), comorbidades (hipertensão, doença respiratória, cardíaca, câncer), plano de saúde, teste de COVID, e variáveis socioeconômicas (restrição de contato, auxílio emergencial, seguro-desemprego, solicitação de empréstimo).
- **Consolidação dos 3 meses** em uma base única de **~1.149.197 registros**.

### 3. Orquestração
Hoje o pipeline roda manualmente, célula-a-célula, em notebook. Como próximo passo do projeto, a lógica de ETL está sendo extraída para módulos `.py` em `src/`, orquestrados por uma **DAG do Airflow** (`dags/pnad_covid_pipeline.py` — ver stub incluído neste repositório) que simula o fluxo `spark-submit ETL → validação de schema → publicação da base tratada`.

### 4. Visualização
Gráficos organizados em **3 seções analíticas** (sociodemográfica, sintomas clínicos, econômica) usando pandas/matplotlib/seaborn, além de um **mapa de calor por estado** (GeoPandas + shapefiles do IBGE) para visualizar a distribuição geográfica dos indicadores. Os achados foram consolidados em relatório final com recomendações para gestão hospitalar.

## 🛠️ Stack técnica e por quê

| Camada | Tecnologia | Por quê |
|---|---|---|
| Processamento distribuído | **PySpark** (`pyspark.sql`, DataFrame API, UDFs) | Volume de +1M linhas e necessidade de transformação em escala — mesma lógica usada em pipelines de produção |
| Manipulação/EDA | **pandas, numpy** | Análises exploratórias após a consolidação via Spark |
| Visualização | **matplotlib, seaborn** | Gráficos das 3 seções analíticas (sociodemográfica, clínica, econômica) |
| Geoespacial | **GeoPandas + Shapefiles (IBGE)** | Mapa de calor por estado |
| Ambiente | **Jupyter Notebook** | Exploração iterativa documentada lado a lado com o código |
| Orquestração (planejada) | **Apache Airflow** | Simular a transição de notebook exploratório para pipeline agendável/produtizável |

## 💻 Como rodar localmente

```bash
git clone https://github.com/MichaelJourdain93/pnad-covid19-hospital-etl.git
cd pnad-covid19-hospital-etl

python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate

pip install -r requirements.txt

jupyter notebook notebooks/01_etl_pnad_covid.ipynb
```

> Os microdados brutos da PNAD-COVID-19 não são versionados no Git. Baixe-os direto do [FTP do IBGE](https://ftp.ibge.gov.br/Trabalho_e_Rendimento/Pesquisa_Nacional_por_Amostra_de_Domicilios_continua/Trabalho_e_garantia_de_renda_durante_a_pandemia_de_Covid-19/Microdados/) e coloque em `data/raw/`.

## 📈 Resultados / Insights

- **Perfil socioeconômico como fator de risco:** mais de **60% dos internados por COVID-19** pertenciam às faixas de renda mais baixas — indicando que vulnerabilidade econômica é um preditor relevante de gravidade clínica.
- **Sintomas como sinal precoce:** febre, tosse e dificuldade respiratória foram os sintomas mais prevalentes, reportados por **~10% dos entrevistados**, reforçando seu valor como indicadores-chave de triagem.
- **Choque no mercado de trabalho:** o desemprego chegou a **~14%** nos meses mais críticos da pandemia, concentrado em trabalhadores informais e nos setores de comércio e serviços.
- **Ciclo de desigualdade:** a análise indicou um efeito circular — vulnerabilidade econômica reduz o acesso a cuidados de saúde, o que agrava os sintomas clínicos nas populações mais pobres; grupos negros e pardos enfrentaram maiores dificuldades de acesso a serviços de saúde adequados.
- **Ações recomendadas ao hospital**, derivadas diretamente dos dados:
  1. **Triagem clínica automatizada** para febre, tosse e dificuldade respiratória, otimizando alocação de recursos.
  2. **Foco em populações vulneráveis** (idosos, comorbidades, baixa renda) via atendimento domiciliar e telemedicina, reduzindo hospitalizações evitáveis.
  3. **Parcerias para mitigação econômica e social** (governo/ONGs) — cestas básicas, auxílio a medicamentos e ações educativas em comunidades carentes.

## 📁 Estrutura do projeto

```
PNAD_COVID_TECH_CHALLENGE/
├── README.md
├── requirements.txt
├── .gitignore
├── notebooks/
│   └── PNAD_covid_19.ipynb
├── dags/
│   └── pnad_covid_pipeline.py         # stub de orquestração Airflow
├── data/
│   └── raw/
│       └── SOURCES.md                 # link oficial do IBGE para baixar os 3 datasets brutos
├── reports/
│   └── mapa_calor_estados/
│       └── SOURCES.md                 # link do shapefile usado no mapa de calor
└── docs/
    ├── Fase3_Tech_Challenge_Grupo62.pdf
    ├── tech_challenge_briefing.txt    # enunciado original do desafio
    ├── datasets_import_google_colab.png
    └── dicionario_pnad_covid/
        ├── Dicionario_PNAD_COVID_092020_20220621.xls
        ├── Dicionario_PNAD_COVID_102020_20220621.xls
        ├── Dicionario_PNAD_COVID_112020_20220621.xls
        └── de_para_variaveis_selecionadas.txt
```

</details>
