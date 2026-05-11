# =============================================================================
# PROJETO DO ALUNO — Ciclo 06
# ETL Automatizado: Covid-19 Open Data Repository (Google)
# =============================================================================
# Fonte de dados : https://health.google.com/covid-19/opendata/raw-data
# Banco de dados : SQLite (covid_data.db)
# Automação      : Logging + Schedule + Rundeck
#
# COMO USAR NO JUPYTERLAB:
#   Execute cada seção separadamente para acompanhar o resultado de cada etapa.
#   As seções estão marcadas com blocos de comentários em MAIÚSCULO.
# =============================================================================


# =============================================================================
# SEÇÃO 1 — IMPORTS E DEPENDÊNCIAS
# =============================================================================
# Todas as bibliotecas usadas no projeto.
# Para instalar: pip install requests pandas schedule
# =============================================================================

import os           # Manipulação de caminhos e diretórios
import shutil       # Cópia de arquivos em streaming (download eficiente)
import sqlite3      # Banco de dados SQLite — nativo do Python, sem instalação
import logging      # Registro de logs em arquivo e console
import requests     # Requisições HTTP para baixar os arquivos CSV
import pandas as pd # Manipulação e transformação dos dados
import schedule     # Agendamento de tarefas (ex: rodar todo dia às 06h)
import time         # Usado junto com schedule para manter o loop ativo

print("✅ Imports realizados com sucesso!")


# =============================================================================
# SEÇÃO 2 — CONFIGURAÇÕES GLOBAIS
# =============================================================================
# Centralize aqui todos os parâmetros do projeto.
# Assim, se precisar mudar algo (ex: caminho do banco), muda em um lugar só.
# =============================================================================

# URL base onde os arquivos CSV do Google estão hospedados
BASE_URL = "https://storage.googleapis.com/covid19-open-data/v3/"

# Nome do arquivo do banco de dados SQLite que será criado/atualizado
BANCO_DADOS = "covid_data.db"

# Pasta onde os arquivos brutos serão salvos após o download
DESTINO_DIR = "data"

# Lista de arquivos CSV que serão baixados da fonte
# Cada um representa uma "tabela" do repositório do Google
ARQUIVOS = [
    "epidemiology.csv",   # Casos confirmados, óbitos e recuperados por dia
    "demographics.csv",   # Dados populacionais por localidade
    "index.csv",          # Índice de localidades (nomes de países, estados)
    "vaccinations.csv",   # Doses de vacinas aplicadas por dia
    "health.csv",         # Indicadores de saúde (leitos, expectativa de vida)
]

print("✅ Configurações definidas!")
print(f"   Base URL  : {BASE_URL}")
print(f"   Banco     : {BANCO_DADOS}")
print(f"   Arquivos  : {ARQUIVOS}")


# =============================================================================
# SEÇÃO 3 — CONFIGURAÇÃO DO LOGGING
# =============================================================================
# O Logging registra tudo que acontece durante a execução do pipeline.
# Gera saída tanto no console (para acompanhar em tempo real) quanto
# em arquivo .log (para auditar depois ou no Rundeck).
# =============================================================================

# Cria a pasta de logs se ela ainda não existir
os.makedirs("logs", exist_ok=True)

# Formato de cada linha do log: DATA/HORA - NÍVEL - MENSAGEM
log_format = "%(asctime)s - %(levelname)s - %(message)s"

# Configura o logging básico (saída no console)
logging.basicConfig(level=logging.DEBUG, format=log_format)

# Cria um "handler" (manipulador) para gravar o log em arquivo
file_handler = logging.FileHandler("logs/etl.log")
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(logging.Formatter(log_format))

# Adiciona o handler de arquivo ao logger raiz
logging.getLogger().addHandler(file_handler)

print("✅ Logging configurado! Logs serão gravados em: logs/etl.log")


# =============================================================================
# SEÇÃO 4 — ETAPA E (EXTRACT) — EXTRAÇÃO DOS DADOS
# =============================================================================
# Faz o download dos arquivos CSV diretamente da URL do Google.
# Verifica se o arquivo já foi baixado antes de baixar novamente
# (evita downloads desnecessários em reexecuções do pipeline).
# =============================================================================

def extract(
    base_url: str = BASE_URL,
    arquivos: list = ARQUIVOS,
    destino: str = DESTINO_DIR,
) -> None:
    """
    Baixa os arquivos CSV da fonte de dados para o diretório local.

    Parâmetros:
        base_url  : URL raiz onde os arquivos estão hospedados
        arquivos  : lista com os nomes dos arquivos a baixar
        destino   : pasta local onde os arquivos serão salvos
    """

    # Cria o diretório de destino se não existir
    os.makedirs(destino, exist_ok=True)

    for arquivo in arquivos:

        # Monta a URL completa do arquivo (base + nome)
        url = base_url + arquivo

        # Monta o caminho local onde o arquivo será salvo
        caminho_destino = os.path.join(destino, arquivo)

        # Só baixa se o arquivo ainda não existir localmente
        if not os.path.exists(caminho_destino):
            logging.info(f"Baixando {arquivo}...")

            # stream=True permite baixar em partes (eficiente para arquivos grandes)
            response = requests.get(url, stream=True)

            if response.status_code == 200:
                # Copia o conteúdo da resposta HTTP direto para o arquivo local
                with open(caminho_destino, "wb") as f:
                    shutil.copyfileobj(response.raw, f)
                logging.info(f"{arquivo} baixado com sucesso!")

            else:
                # Registra erro mas não interrompe o loop — tenta os demais arquivos
                logging.error(f"Falha ao baixar {arquivo}. Status HTTP: {response.status_code}")

            # Libera a memória da resposta HTTP
            del response

        else:
            # Arquivo já existe — pula o download
            logging.info(f"{arquivo} já existe localmente. Pulando download.")

    logging.info("Extração concluída.")


# -----------------------------------------------------------------------------
# Execute a extração:
# -----------------------------------------------------------------------------
extract()


# =============================================================================
# SEÇÃO 5 — ETAPA T (TRANSFORM) — TRANSFORMAÇÃO DOS DADOS
# =============================================================================
# Lê cada arquivo CSV baixado, aplica limpezas e padronizações,
# e salva os arquivos tratados em uma subpasta separada (data/tratados/).
# Isso mantém os dados brutos intactos e os tratados organizados.
# =============================================================================

def transform(
    arquivos: list = ARQUIVOS,
    dir_dados: str = DESTINO_DIR,
) -> None:
    """
    Trata os arquivos CSV brutos e salva os dados limpos em data/tratados/.

    Transformações aplicadas:
        - Remove linhas completamente vazias
        - Padroniza valores nulos representados como string 'N'

    Parâmetros:
        arquivos  : lista com os nomes dos arquivos a tratar
        dir_dados : pasta onde os arquivos brutos estão salvos
    """

    # Subpasta onde os arquivos tratados serão salvos
    dir_tratados = os.path.join(dir_dados, "tratados")
    os.makedirs(dir_tratados, exist_ok=True)

    for arquivo in arquivos:

        # Caminho completo do arquivo bruto
        caminho_arquivo = os.path.join(dir_dados, arquivo)

        # Verifica se o arquivo existe e é um CSV
        if os.path.isfile(caminho_arquivo) and arquivo.endswith(".csv"):
            logging.debug(f"Transformando {arquivo}...")

            # Lê o CSV como DataFrame do pandas
            # low_memory=False evita warnings em arquivos com tipos de coluna mistos
            # nrows=1000 limita a leitura a 1000 linhas (útil para testes rápidos)
            df = pd.read_csv(
                caminho_arquivo,
                low_memory=False,
                # nrows=1_000  # ← descomente esta linha para testar mais rápido
            )

            # Substitui a string "N" (valor nulo no padrão do Google) por None (NaN do pandas)
            df.replace({"\\N": None}, inplace=True)

            # Remove linhas onde TODOS os campos são nulos (linhas completamente vazias)
            df.dropna(how="all", inplace=True)

            # Salva o DataFrame tratado como CSV na pasta de tratados
            caminho_destino = os.path.join(dir_tratados, arquivo)
            df.to_csv(caminho_destino, index=False)

            logging.debug(f"{arquivo} tratado e salvo em {caminho_destino}")

    logging.info("Transformação concluída.")


# -----------------------------------------------------------------------------
# Execute a transformação:
# -----------------------------------------------------------------------------
transform()


# =============================================================================
# SEÇÃO 6 — ETAPA L (LOAD) — CARGA NO BANCO DE DADOS
# =============================================================================
# Lê cada CSV tratado da pasta data/tratados/ e carrega no banco SQLite.
# Cada arquivo vira uma tabela separada no banco.
# O nome da tabela é derivado do nome do arquivo (sem extensão).
# =============================================================================

def load(conexao) -> None:
    """
    Carrega os arquivos CSV tratados no banco de dados SQLite.

    Cada arquivo CSV se torna uma tabela no banco.
    if_exists='replace' recria a tabela a cada execução (dados sempre atualizados).

    Parâmetros:
        conexao : objeto de conexão SQLite aberto (sqlite3.connect)
    """

    # Pasta com os arquivos tratados
    dir_tratados = os.path.join(DESTINO_DIR, "tratados")

    # Itera sobre todos os arquivos dentro da pasta tratados/
    for arquivo in os.listdir(dir_tratados):

        caminho_arquivo = os.path.join(dir_tratados, arquivo)

        # Processa apenas arquivos CSV
        if os.path.isfile(caminho_arquivo) and arquivo.endswith(".csv"):

            # Lê o CSV tratado como DataFrame
            df = pd.read_csv(caminho_arquivo, low_memory=False)

            # Deriva o nome da tabela a partir do nome do arquivo
            # Exemplo: "epidemiology.csv" → "epidemiology"
            nome_tabela = os.path.splitext(arquivo)[0]

            # Substitui caracteres inválidos para nomes de tabela SQL
            nome_tabela = nome_tabela.replace(".", "_").replace("-", "_")

            # Carrega o DataFrame no banco SQLite como uma tabela
            # if_exists='replace' → recria a tabela se já existir
            df.to_sql(nome_tabela, conexao, index=False, if_exists="replace")

            logging.info(f"{arquivo} → tabela [{nome_tabela}] carregada no banco.")

    logging.info("Carga concluída.")


# =============================================================================
# SEÇÃO 7 — CRIAÇÃO DA TABELA ANALÍTICA
# =============================================================================
# Após carregar todas as tabelas brutas no banco, criamos uma tabela
# analítica denormalizada — um único SELECT que já une todas as informações
# relevantes em uma só tabela.
# Isso facilita consultas e análises futuras sem precisar fazer JOINs.
# =============================================================================

def create_analytical_tables(conexao) -> None:
    """
    Cria a tabela analítica analitico_covid no banco SQLite.

    Une os dados de epidemiologia, vacinação, demografia e saúde
    pelo campo location_key (chave de localidade compartilhada por todos).

    Parâmetros:
        conexao : objeto de conexão SQLite aberto (sqlite3.connect)
    """

    # Query SQL que cria e popula a tabela analítica em uma única operação
    sql_analitico_covid = """
    CREATE TABLE IF NOT EXISTS analitico_covid AS

    SELECT
        -- Dimensão: tempo e localidade
        e.date,                               -- Data do registro
        e.location_key,                       -- Chave de localidade (campo de JOIN)
        i.country_name,                       -- Nome do país
        i.subregion1_name,                    -- Estado ou Província

        -- Métricas de epidemiologia (origem: tabela epidemiology)
        e.new_confirmed,                      -- Novos casos confirmados no dia
        e.new_deceased,                       -- Novos óbitos no dia
        e.new_recovered,                      -- Novos recuperados no dia

        -- Métricas de vacinação (origem: tabela vaccinations)
        v.new_persons_vaccinated,             -- Novas pessoas vacinadas no dia
        v.new_vaccine_doses_administered,     -- Total de doses aplicadas no dia

        -- Dados populacionais (origem: tabela demographics)
        d.population,                         -- População total da localidade
        d.population_density,                 -- Densidade populacional (hab/km²)

        -- Indicadores de saúde (origem: tabela health)
        h.life_expectancy,                    -- Expectativa de vida (anos)
        h.hospital_beds_per_1000              -- Leitos hospitalares por 1000 hab.

    -- Tabela base: epidemiologia (tem data + location_key — ideal para o FROM)
    FROM epidemiology e

    -- Traz nome do país e estado a partir do índice de localidades
    LEFT JOIN 'index' i
        ON i.location_key = e.location_key

    -- JOIN com vacinação por localidade E data (evita multiplicar linhas)
    LEFT JOIN vaccinations v
        ON  v.location_key = e.location_key
        AND v.date         = e.date

    -- JOIN com demografia só por localidade (dados não variam por dia)
    LEFT JOIN demographics d
        ON d.location_key = e.location_key

    -- JOIN com saúde só por localidade (dados não variam por dia)
    LEFT JOIN health h
        ON h.location_key = e.location_key
    """

    # Executa as queries em sequência: primeiro remove a versão antiga, depois recria
    queries = [
        "DROP TABLE IF EXISTS analitico_covid",  # Remove tabela antiga (se existir)
        sql_analitico_covid,                      # Cria e popula a tabela analítica
    ]

    logging.info("Criando tabela analítica...")

    for query in queries:
        conexao.execute(query)

    # Confirma e salva as mudanças no arquivo .db
    conexao.commit()

    logging.info("Tabela analitico_covid criada com sucesso!")


# =============================================================================
# SEÇÃO 8 — ORQUESTRAÇÃO DO PIPELINE COMPLETO
# =============================================================================
# Função principal que chama todas as etapas na ordem correta:
# Extract → Transform → Load → Create Analytical Tables
# Esta é a função que será agendada pelo Schedule ou chamada pelo Rundeck.
# =============================================================================

def execute_pipeline() -> None:
    """
    Orquestra o pipeline ETL completo na ordem correta.

    Ordem de execução:
        1. extract()                   → Download dos CSVs
        2. transform()                 → Limpeza e padronização
        3. load()                      → Carga no banco SQLite
        4. create_analytical_tables()  → Criação da tabela analítica
    """

    logging.info("=" * 60)
    logging.info("INÍCIO DO PIPELINE ETL — Covid-19 Open Data")
    logging.info("=" * 60)

    # Etapa 1: Download dos arquivos brutos
    logging.info(">>> ETAPA 1/4: EXTRACT")
    extract()

    # Etapa 2: Limpeza e padronização dos dados
    logging.info(">>> ETAPA 2/4: TRANSFORM")
    transform()

    # Etapas 3 e 4 compartilham a mesma conexão com o banco
    logging.info(">>> ETAPA 3/4: LOAD")
    conexao = sqlite3.connect(BANCO_DADOS)  # Abre (ou cria) o arquivo .db
    load(conexao=conexao)

    logging.info(">>> ETAPA 4/4: CREATE ANALYTICAL TABLES")
    create_analytical_tables(conexao=conexao)

    # IMPORTANTE: sempre feche a conexão ao terminar para evitar locks no banco
    conexao.close()

    logging.info("=" * 60)
    logging.info("FIM DO PIPELINE ETL — Concluído com sucesso!")
    logging.info("=" * 60)


# =============================================================================
# SEÇÃO 9 — AUTOMAÇÃO COM RUNDECK
# =============================================================================
# O Rundeck permite agendar e monitorar os jobs em produção via interface web.
# Aqui documentamos a estrutura de Jobs e Steps que seriam configurados nele.
# Em produção, cada step no Rundeck chamaria uma das funções acima.
# =============================================================================

# Dicionário que mapeia o nome de cada Job à sua função Python correspondente
RUNDECK_JOBS = {
    "job_extract":   extract,    # Job responsável pela extração dos dados
    "job_transform": transform,  # Job responsável pela transformação
    "job_load":      None,       # Job de carga (requer conexão aberta — ver execute_pipeline)
    "job_analytics": None,       # Job de analítica (requer conexão aberta)
}

# Descrição dos steps que compõem o pipeline no Rundeck
RUNDECK_STEPS = [
    {"step": 1, "nome": "step_extract",   "descricao": "Download dos arquivos CSV da fonte"},
    {"step": 2, "nome": "step_transform", "descricao": "Limpeza e padronização dos dados brutos"},
    {"step": 3, "nome": "step_load",      "descricao": "Carga dos dados tratados no banco SQLite"},
    {"step": 4, "nome": "step_analytics", "descricao": "Criação da tabela analítica denormalizada"},
]


def listar_steps_rundeck() -> None:
    """Exibe os steps disponíveis no pipeline (equivalente ao que o Rundeck executa)."""
    print("\n📋 Steps do Pipeline (Rundeck):")
    print("-" * 60)
    for step in RUNDECK_STEPS:
        print(f"  Step {step['step']}: {step['nome']:20} → {step['descricao']}")
    print("-" * 60)


# Exibe os steps disponíveis
listar_steps_rundeck()


# =============================================================================
# SEÇÃO 10 — AGENDAMENTO COM SCHEDULE
# =============================================================================
# O Schedule permite rodar o pipeline automaticamente em um horário fixo.
# Útil para manter os dados sempre atualizados sem intervenção manual.
#
# ATENÇÃO: Bloco comentado para não rodar automaticamente no JupyterLab.
# Descomente apenas quando quiser deixar o processo rodando continuamente.
# =============================================================================

# Para agendar o pipeline para rodar todo dia às 06:00, use:
#
# schedule.every().day.at("06:00").do(execute_pipeline)
#
# # Loop infinito que fica verificando se há tarefas para executar
# while True:
#     schedule.run_pending()  # Executa qualquer tarefa agendada que esteja na hora
#     time.sleep(60)          # Espera 60 segundos antes de verificar novamente


# =============================================================================
# SEÇÃO 11 — EXECUÇÃO DO PIPELINE
# =============================================================================
# Chama o pipeline completo. No JupyterLab, você pode rodar só esta célula
# após ter testado cada função individualmente nas seções anteriores.
# =============================================================================

execute_pipeline()


# =============================================================================
# SEÇÃO 12 — VALIDAÇÃO DO RESULTADO
# =============================================================================
# Após rodar o pipeline, use este bloco para confirmar que tudo funcionou:
# quais tabelas foram criadas, quantos registros cada uma tem e
# uma prévia dos dados da tabela analítica.
# =============================================================================

def validar_resultado(banco: str = BANCO_DADOS) -> None:
    """
    Valida o resultado do pipeline consultando o banco de dados gerado.

    Exibe:
        - Lista de tabelas criadas no banco com contagem de registros
        - Primeiras 5 linhas da tabela analítica
        - Total de registros na tabela analítica
    """

    # Abre a conexão com o banco gerado pelo pipeline
    con = sqlite3.connect(banco)

    # Lista todas as tabelas existentes no banco ordenadas por nome
    tabelas = con.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()

    print("\n📦 Tabelas criadas no banco de dados:")
    print("-" * 50)
    for tabela in tabelas:
        # Conta o total de linhas em cada tabela
        qtd = con.execute(f"SELECT COUNT(*) FROM [{tabela[0]}]").fetchone()[0]
        print(f"  ✔ {tabela[0]:30} → {qtd:,} registros")

    # Lê e exibe as primeiras 5 linhas da tabela analítica
    print("\n📊 Prévia da tabela analitico_covid (5 primeiras linhas):")
    print("-" * 60)
    df_preview = pd.read_sql("SELECT * FROM analitico_covid LIMIT 5", con)
    print(df_preview.to_string(index=False))

    # Contagem total de registros na tabela analítica
    total = con.execute("SELECT COUNT(*) FROM analitico_covid").fetchone()[0]
    print(f"\n✅ Total de registros em analitico_covid: {total:,}")

    con.close()


# Executa a validação
validar_resultado()


# =============================================================================
# FIM DO SCRIPT
# =============================================================================
# Resumo das seções:
#   Seção 1-3  : Imports, configurações globais e logging
#   Seção 4    : Extract  → download dos CSVs da fonte
#   Seção 5    : Transform → limpeza e padronização dos dados
#   Seção 6    : Load     → carga no banco SQLite
#   Seção 7    : Criação da tabela analítica denormalizada
#   Seção 8    : Pipeline orquestrado — execute_pipeline()
#   Seção 9    : Estrutura dos Jobs/Steps do Rundeck
#   Seção 10   : Agendamento com Schedule (comentado)
#   Seção 11   : Execução do pipeline completo
#   Seção 12   : Validação do resultado no banco de dados
# =============================================================================
