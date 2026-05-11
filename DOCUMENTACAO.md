# 📄 Documentação Técnica — ETL Covid-19

> Documento de referência técnica do projeto. Descreve as decisões de arquitetura,
> o raciocínio por trás de cada escolha e o passo a passo detalhado de como o pipeline foi construído.

---

## 1. Visão Geral da Arquitetura

O pipeline segue o padrão clássico **ETL (Extract, Transform, Load)**, dividido em funções independentes e orquestradas por uma função principal.

```
┌─────────────────────────────────────────────────────────────────┐
│                      execute_pipeline()                          │
│                                                                  │
│   extract()  →  transform()  →  load()  →  create_analytical()  │
│       ↓               ↓            ↓              ↓             │
│   data/CSV     data/tratados/   SQLite      analitico_covid      │
└─────────────────────────────────────────────────────────────────┘
```

Cada etapa é **independente** — pode ser testada e reexecutada separadamente sem afetar as demais.

---

## 2. Decisões de Arquitetura

### 2.1 Por que Python puro e não uma ferramenta de ETL dedicada?

O projeto foi desenvolvido em Python puro (com pandas e requests) por três motivos:

- **Controle total** sobre cada etapa do processo, sem abstrações que escondem o que está acontecendo
- **Portabilidade** — roda em qualquer máquina com Python instalado, sem dependência de serviços externos
- **Didático** — cada linha de código é explícita e comentada, facilitando manutenção e aprendizado

### 2.2 Por que SQLite e não PostgreSQL ou MySQL?

O SQLite foi escolhido intencionalmente porque:

- É **nativo do Python** — zero instalação, zero configuração de servidor
- Gera um **único arquivo** `.db` que pode ser versionado, compartilhado e aberto em ferramentas como DBeaver
- Para o volume de dados deste projeto (~700 MB de CSVs), o SQLite performa perfeitamente
- Em produção com volumes maiores, a troca para PostgreSQL exigiria apenas mudar a string de conexão

### 2.3 Por que uma tabela analítica denormalizada?

Em vez de deixar os dados em 5 tabelas separadas e fazer JOINs a cada consulta, optou-se por criar uma tabela consolidada `analitico_covid` porque:

- **Performance** — consultas analíticas em uma tabela plana são muito mais rápidas
- **Simplicidade** — o analista não precisa conhecer os relacionamentos entre as tabelas para extrair insights
- **Reprodutibilidade** — qualquer pessoa com acesso ao banco consegue consultar os dados sem documentação adicional

---

## 3. Detalhamento das Etapas

### 3.1 Extract — Extração

**O que faz:** Baixa os 5 arquivos CSV do repositório do Google via requisição HTTP.

**Decisão técnica — verificação de existência:**
```python
if not os.path.exists(caminho_destino):
    # só baixa se não existir
```
Isso evita baixar ~700 MB toda vez que o pipeline roda. Em reexecuções, o download é pulado automaticamente. Para forçar um novo download (dados atualizados), basta apagar os arquivos da pasta `data/`.

**Decisão técnica — download em streaming:**
```python
response = requests.get(url, stream=True)
shutil.copyfileobj(response.raw, f)
```
O `stream=True` baixa o arquivo em partes pequenas em vez de carregar tudo na memória de uma vez. Essencial para o `epidemiology.csv` de 520 MB.

---

### 3.2 Transform — Transformação

**O que faz:** Lê cada CSV bruto, aplica limpezas e salva na pasta `data/tratados/`.

**Decisão técnica — separação de pastas (bruto vs tratado):**
Os arquivos brutos são mantidos intactos em `data/` e os tratados ficam em `data/tratados/`. Isso permite:
- Reprocessar os dados sem precisar baixar novamente
- Auditar diferenças entre o dado original e o tratado
- Reverter transformações se necessário

**Transformações aplicadas:**

| Transformação | Código | Motivo |
|---|---|---|
| Padronizar nulos | `df.replace({'\\N': None})` | O Google representa nulos como string `\N` — o pandas não reconhece isso como nulo nativo |
| Remover linhas vazias | `df.dropna(how='all')` | Remove linhas onde todos os campos são nulos, que não agregam valor analítico |

**Decisão técnica — `low_memory=False`:**
```python
df = pd.read_csv(caminho_arquivo, low_memory=False)
```
Sem esse parâmetro, o pandas "chuta" o tipo de dado de cada coluna lendo apenas as primeiras linhas. Em arquivos grandes com tipos mistos, isso gera warnings e pode causar erros silenciosos na carga.

---

### 3.3 Load — Carga

**O que faz:** Lê cada CSV tratado e carrega como tabela no banco SQLite.

**Decisão técnica — `if_exists='replace'`:**
```python
df.to_sql(nome_tabela, conexao, index=False, if_exists='replace')
```
A opção `replace` recria a tabela do zero a cada execução, garantindo que o banco sempre reflita os dados mais recentes. Para um pipeline incremental (que só insere registros novos), usaria-se `append` — mas requereria lógica adicional de controle de duplicatas.

**Decisão técnica — derivar nome da tabela do nome do arquivo:**
```python
nome_tabela = os.path.splitext(arquivo)[0]
nome_tabela = nome_tabela.replace(".", "_").replace("-", "_")
```
Isso torna o pipeline genérico — se novos arquivos forem adicionados à lista `ARQUIVOS`, eles são carregados automaticamente sem alterar o código de carga.

---

### 3.4 Create Analytical Tables — Tabela Analítica

**O que faz:** Cria a tabela `analitico_covid` unindo as 5 tabelas pelo campo `location_key`.

**Por que `LEFT JOIN` e não `INNER JOIN`?**

O `LEFT JOIN` foi escolhido para manter **todos os registros de epidemiologia**, mesmo que não tenham correspondência nas outras tabelas. Um `INNER JOIN` descartaria silenciosamente registros sem correspondência — o que poderia fazer dados de regiões menos documentadas desaparecerem da análise.

**Por que o JOIN com vaccinations usa duas condições?**
```sql
LEFT JOIN vaccinations v
    ON  v.location_key = e.location_key
    AND v.date         = e.date
```
A tabela de vacinação tem dados por **localidade E data** (igual à epidemiologia). Sem o `AND v.date = e.date`, cada linha de epidemiologia seria cruzada com todas as linhas de vacinação da mesma localidade — multiplicando os registros incorretamente (produto cartesiano).

**Por que demographics e health usam apenas `location_key`?**
```sql
LEFT JOIN demographics d ON d.location_key = e.location_key
LEFT JOIN health h       ON h.location_key = e.location_key
```
Essas tabelas têm dados **estáticos por localidade** — a população e a expectativa de vida de um país não mudam por dia. Por isso, o JOIN é apenas por localidade, sem filtro de data.

---

## 4. Sistema de Logging

O logging foi configurado para registrar simultaneamente no **console** (visível durante execução) e em **arquivo** (para auditoria posterior).

```python
# Handler do console — já configurado pelo basicConfig
logging.basicConfig(level=logging.DEBUG, format=log_format)

# Handler do arquivo — adicionado manualmente
file_handler = logging.FileHandler("logs/etl.log")
logging.getLogger().addHandler(file_handler)
```

**Níveis de log utilizados:**

| Nível | Onde é usado | Exemplo |
|---|---|---|
| `DEBUG` | Detalhes internos de cada etapa | `Transformando epidemiology.csv...` |
| `INFO` | Eventos importantes do pipeline | `epidemiology.csv baixado com sucesso!` |
| `ERROR` | Falhas não críticas | `Falha ao baixar vaccinations.csv. HTTP 404` |

---

## 5. Automação

### 5.1 Schedule

O Schedule mantém o processo rodando e verifica a fila de tarefas a cada 60 segundos:

```python
schedule.every().day.at("06:00").do(execute_pipeline)

while True:
    schedule.run_pending()
    time.sleep(60)
```

**Por que está comentado no script?**
No JupyterLab, o `while True` bloquearia o kernel indefinidamente. O bloco está documentado para uso em ambiente de produção (servidor ou Rundeck).

### 5.2 Rundeck

O Rundeck foi estruturado com **4 Jobs independentes**, um para cada etapa do pipeline:

| Job | Step | Função chamada |
|---|---|---|
| `job_extract` | `step_extract` | `extract()` |
| `job_transform` | `step_transform` | `transform()` |
| `job_load` | `step_load` | `load()` |
| `job_analytics` | `step_analytics` | `create_analytical_tables()` |

**Por que separar em Jobs individuais?**
Jobs separados permitem:
- Reprocessar apenas uma etapa sem rodar o pipeline inteiro
- Monitorar falhas por etapa com precisão
- Configurar alertas e notificações específicos por etapa

---

## 6. Relacionamento entre os Datasets

```
index.csv
    └── location_key (PK)
            │
            ├── epidemiology.csv  (location_key + date)  ← tabela base
            ├── vaccinations.csv  (location_key + date)
            ├── demographics.csv  (location_key)
            └── health.csv        (location_key)
```

O campo `location_key` é a **chave primária de localidade** compartilhada por todos os datasets. Exemplos de valores: `BR` (Brasil), `BR-SP` (São Paulo), `US` (Estados Unidos).

---

## 7. Tabela Analítica — Dicionário de Dados

**Nome:** `analitico_covid`
**Tipo:** Tabela denormalizada
**Atualização:** Recriada a cada execução do pipeline

| Coluna | Tipo | Origem | Descrição |
|---|---|---|---|
| `date` | TEXT | epidemiology | Data do registro no formato YYYY-MM-DD |
| `location_key` | TEXT | epidemiology | Chave de localidade (ex: BR, BR-SP) |
| `country_name` | TEXT | index | Nome do país em inglês |
| `subregion1_name` | TEXT | index | Nome do estado ou província |
| `new_confirmed` | INTEGER | epidemiology | Novos casos confirmados no dia |
| `new_deceased` | INTEGER | epidemiology | Novos óbitos registrados no dia |
| `new_recovered` | INTEGER | epidemiology | Novos recuperados registrados no dia |
| `new_persons_vaccinated` | INTEGER | vaccinations | Novas pessoas que receberam ao menos 1 dose |
| `new_vaccine_doses_administered` | INTEGER | vaccinations | Total de doses aplicadas no dia |
| `population` | INTEGER | demographics | População total estimada da localidade |
| `population_density` | FLOAT | demographics | Habitantes por km² |
| `life_expectancy` | FLOAT | health | Expectativa de vida em anos |
| `hospital_beds_per_1000` | FLOAT | health | Leitos hospitalares por 1.000 habitantes |

---

## 8. Exemplo de Consulta

```python
import sqlite3
import pandas as pd

con = sqlite3.connect("covid_data.db")

# Casos e vacinação no Brasil — últimos 30 dias
df = pd.read_sql("""
    SELECT
        date,
        country_name,
        SUM(new_confirmed)          AS total_casos,
        SUM(new_deceased)           AS total_obitos,
        SUM(new_persons_vaccinated) AS total_vacinados
    FROM analitico_covid
    WHERE location_key LIKE 'BR%'
    GROUP BY date, country_name
    ORDER BY date DESC
    LIMIT 30
""", con)

print(df)
con.close()
```

---

## 9. Requisitos do Ambiente

```
pandas==2.2.2
requests==2.32.3
schedule==1.2.2
jupyterlab==4.1.5
```

Para recriar o ambiente:
```bash
python -m venv venv
venv\Scripts\activate        # Windows
pip install -r requirements.txt
```

---

*Desenvolvido por **Rafael Costa** | Ciclo 06 — Formação Profissional em Análise de Dados | Comunidade DS*
