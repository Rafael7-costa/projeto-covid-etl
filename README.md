# 🦠 ETL Automatizado — Covid-19 Open Data Repository

![Python](https://img.shields.io/badge/Python-3.10+-blue?logo=python&logoColor=white)
![SQLite](https://img.shields.io/badge/SQLite-Database-lightblue?logo=sqlite&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-Data%20Analysis-150458?logo=pandas&logoColor=white)
![Rundeck](https://img.shields.io/badge/Rundeck-Automation-red)
![Status](https://img.shields.io/badge/Status-Concluído-green)

---

## 🧩 O Problema de Negócio

Durante a pandemia de Covid-19, o Google disponibilizou um repositório público com dados detalhados sobre casos, óbitos, vacinação e indicadores de saúde de centenas de países e regiões do mundo.

O problema: esses dados estão distribuídos em **5 arquivos separados**, cada um com informações distintas e sem nenhuma integração entre eles. Para um analista de dados extrair qualquer insight útil, seria necessário cruzar manualmente essas fontes toda vez — um processo lento, manual e sujeito a erros.

> **Como transformar dados dispersos e brutos em uma base analítica consolidada, confiável e atualizada automaticamente?**

---

## 💡 A Solução Desenvolvida

Foi desenvolvido um **pipeline ETL completo e automatizado** em Python que resolve esse problema em 4 etapas:

**1. Extração** — O pipeline conecta diretamente à fonte do Google e baixa os 5 arquivos CSV automaticamente, verificando se já existem localmente para evitar downloads desnecessários.

**2. Transformação** — Os dados brutos passam por um processo de limpeza: remoção de linhas vazias, padronização de valores nulos e organização em uma pasta separada de dados tratados.

**3. Carga** — Cada arquivo tratado é carregado como uma tabela individual em um banco de dados SQLite, criando uma estrutura organizada e consultável.

**4. Tabela Analítica** — As 5 tabelas são unificadas em uma única tabela denormalizada chamada `analitico_covid`, cruzando todas as informações pelo campo `location_key`. O resultado é uma base pronta para análise, sem necessidade de JOINs.

Todo o processo é **automatizado via Rundeck** (agendamento em produção) e **Schedule** (agendamento via Python), e cada execução é registrada em um arquivo de **log** para auditoria e monitoramento.

---

## 📊 O Resultado

Ao final do pipeline, a tabela `analitico_covid` consolida em uma única estrutura:

| Dimensão | O que contém |
|---|---|
| 📍 Localização | País, estado e chave de localidade |
| 🦠 Epidemiologia | Casos confirmados, óbitos e recuperados por dia |
| 💉 Vacinação | Pessoas vacinadas e doses aplicadas por dia |
| 👥 Demografia | População total e densidade populacional |
| 🏥 Saúde | Expectativa de vida e leitos hospitalares |

Com essa base consolidada, análises que antes exigiam cruzamento manual de múltiplos arquivos passam a ser feitas com uma única consulta SQL — **reduzindo o tempo de análise e eliminando retrabalho**.

---

## 🛠️ Tecnologias Utilizadas

| Tecnologia | Para que foi usada |
|---|---|
| **Python 3.10+** | Linguagem principal do pipeline |
| **Pandas** | Limpeza e transformação dos dados |
| **Requests** | Download dos arquivos CSV via HTTP |
| **SQLite3** | Banco de dados local (nativo do Python) |
| **Logging** | Registro de execuções em arquivo de log |
| **Schedule** | Agendamento automático do pipeline |
| **Rundeck** | Orquestração e monitoramento em produção |

---

## 🚀 Como Executar

**1. Clone o repositório**
```bash
git clone https://github.com/rafael/projeto-covid-etl.git
cd projeto-covid-etl
```

**2. Crie e ative o ambiente virtual**
```bash
# Windows
python -m venv venv
venv\Scripts\activate

# Linux / Mac
source venv/bin/activate
```

**3. Instale as dependências**
```bash
pip install -r requirements.txt
```

**4. Execute o pipeline**
```bash
python etl_covid_completo.py
```

> ⚠️ O download completo ocupa aproximadamente **700 MB**. Para testar com uma amostra, descomente a linha `nrows=1_000` na função `transform()` do script.

---

## 📁 Estrutura do Projeto

```
projeto-covid-etl/
├── etl_covid_completo.py   # Script principal do pipeline
├── requirements.txt        # Dependências do projeto
├── venv/                   # Ambiente virtual Python
├── covid_data.db           # Banco de dados gerado automaticamente
├── data/                   # Arquivos CSV baixados e tratados
├── logs/                   # Logs de execução
├── DOCUMENTACAO.md         # Documentação técnica detalhada
└── README.md               # Este arquivo
```

📄 Para detalhes técnicos completos sobre arquitetura e decisões de desenvolvimento, consulte o [DOCUMENTACAO.md](DOCUMENTACAO.md).

---

## 👨‍💻 Autor

**Rafael Costa**
Analista de Dados | Formação Profissional em Análise de Dados — Comunidade DS

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Rafael-blue?logo=linkedin)](https://linkedin.com/in/rafael)
[![GitHub](https://img.shields.io/badge/GitHub-rafael-black?logo=github)](https://github.com/rafael)
