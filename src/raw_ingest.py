import os
import json
import logging
import boto3
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime

# Configuração de logs
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Pastas que nunca devem ser processadas
PASTAS_IGNORADAS = {"20250214"}

# Configurações vindas do .env
# AWS_ACCESS_KEY / AWS_SECRET_KEY não são mais usadas: em produção (ECS) as
# credenciais vêm automaticamente da task role (etl-role-ecs). Para rodar
# localmente, configure suas credenciais via `aws configure` ou variáveis
# padrão AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY, que o boto3 detecta sozinho.
S3_BUCKET = os.environ["S3_BUCKET_NAME"]
S3_PREFIX = "prontlife/"

DB_HOST = os.environ["DB_HOST"]
DB_USER = os.environ["DB_USER"]
DB_PASSWORD = os.environ["DB_PASSWORD"]
DB_NAME = os.environ["DB_NAME"]
DB_PORT = os.environ["DB_PORT"]

RAW_SCHEMA = "prontlife_raw"


# ── Conexões ──────────────────────────────────────────────────────────────────


def conectar_s3():
    """
    Cria e retorna um cliente S3 autenticado.
    Não passamos credenciais explícitas: o boto3 detecta automaticamente
    a credencial disponível (task role no ECS, ou `aws configure` local).
    """
    logger.info("Conectando ao S3...")
    return boto3.client("s3")


def conectar_banco():
    """Cria e retorna uma conexão com o Postgres."""
    logger.info("Conectando ao banco de dados...")
    return psycopg2.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        dbname=DB_NAME,
        port=DB_PORT,
    )


# ── Controle de pastas ────────────────────────────────────────────────────────


def listar_pastas_s3(s3):
    """Lista todas as pastas de entrega no S3 (ex: 20260601)."""
    logger.info(f"Listando pastas em s3://{S3_BUCKET}/{S3_PREFIX}")

    resposta = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=S3_PREFIX, Delimiter="/")

    pastas = []
    for prefixo in resposta.get("CommonPrefixes", []):
        nome = prefixo["Prefix"].replace(S3_PREFIX, "").strip("/")
        if nome and nome not in PASTAS_IGNORADAS:
            pastas.append(nome)

    logger.info(f"{len(pastas)} pastas encontradas: {pastas}")
    return sorted(pastas)


def listar_pastas_processadas(cursor):
    """Retorna as delivery_dates que já estão na raw, no formato YYYYMMDD."""
    try:
        cursor.execute(f"""
            select distinct delivery_date::text
            from {RAW_SCHEMA}.patient
        """)
        return {row[0].replace("-", "") for row in cursor.fetchall()}
    except Exception:
        cursor.connection.rollback()
        return set()


def pastas_pendentes(s3, cursor):
    """Retorna as pastas do S3 que ainda não foram carregadas."""
    todas = set(listar_pastas_s3(s3))
    ja_processadas = listar_pastas_processadas(cursor)
    pendentes = sorted(todas - ja_processadas)
    logger.info(f"{len(pendentes)} pastas pendentes: {pendentes}")
    return pendentes


# ── Banco de dados ────────────────────────────────────────────────────────────


def garantir_schema(cursor):
    """Cria o schema prontlife_raw se não existir."""
    logger.info(f"Garantindo schema {RAW_SCHEMA}...")
    cursor.execute(f"create schema if not exists {RAW_SCHEMA}")


def garantir_tabela(cursor, nome_tabela):
    """Cria a tabela na raw se não existir."""
    sql = f"""
        create table if not exists {RAW_SCHEMA}.{nome_tabela} (
            id              text,
            resource_type   text,
            source_file     text,
            delivery_date   date,
            ingested_at     timestamp default now(),
            data            jsonb
        )
    """
    cursor.execute(sql)
    cursor.connection.commit()
    logger.info(f"Tabela {RAW_SCHEMA}.{nome_tabela} garantida.")


# ── Ingestão de arquivos ──────────────────────────────────────────────────────


def deduzir_nome_tabela(chave):
    """
    Deduz o nome da tabela a partir do nome do arquivo.
    ex: patient_20260601.json → patient
    """
    nome = chave.split("/")[-1]
    nome = nome.replace(".json", "")
    nome = "_".join(nome.split("_")[:-1])
    return nome.lower()


def processar_arquivo(s3, cursor, bucket, chave, delivery_date):
    """Baixa um arquivo do S3, lê os registros e insere na raw."""
    nome_arquivo = chave.split("/")[-1]
    logger.info(f"Processando {nome_arquivo}...")

    conteudo = s3.get_object(Bucket=bucket, Key=chave)["Body"].read().decode("utf-8")

    if len(conteudo.strip()) <= 2:
        logger.warning(f"{nome_arquivo} vazio, pulando.")
        return 0

    registros = json.loads(conteudo)

    if not isinstance(registros, list) or len(registros) == 0:
        logger.warning(f"{nome_arquivo} sem registros, pulando.")
        return 0

    nome_tabela = deduzir_nome_tabela(chave)
    garantir_tabela(cursor, nome_tabela)

    resource_type = registros[0].get("resourceType", nome_tabela)

    sql = f"""
        insert into {RAW_SCHEMA}.{nome_tabela}
            (id, resource_type, source_file, delivery_date, data)
        values %s
    """

    valores = [
        (
            registro.get("id"),
            resource_type,
            nome_arquivo,
            delivery_date,
            json.dumps(registro, ensure_ascii=False),
        )
        for registro in registros
    ]

    execute_values(cursor, sql, valores, page_size=1000)
    logger.info(f"{nome_arquivo}: {len(valores)} registros inseridos.")
    return len(valores)


def processar_responses(s3, cursor, pasta, delivery_date):
    """
    Processa todos os arquivos da pasta responses/ de uma entrega.
    Todos vão pra mesma tabela: questionnaire_response.
    """
    prefixo = f"{S3_PREFIX}{pasta}/responses/"
    resposta = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefixo)
    arquivos = [
        obj["Key"]
        for obj in resposta.get("Contents", [])
        if not obj["Key"].endswith("/")
    ]

    if not arquivos:
        logger.warning(f"Pasta responses/ vazia em {pasta}, pulando.")
        return 0

    logger.info(f"Processando {len(arquivos)} arquivos de responses/...")

    garantir_tabela(cursor, "questionnaire_response")

    sql = f"""
        insert into {RAW_SCHEMA}.questionnaire_response
            (id, resource_type, source_file, delivery_date, data)
        values %s
    """

    total = 0
    for chave in arquivos:
        nome_arquivo = chave.split("/")[-1]
        conteudo = (
            s3.get_object(Bucket=S3_BUCKET, Key=chave)["Body"].read().decode("utf-8")
        )

        if len(conteudo.strip()) <= 2:
            logger.warning(f"{nome_arquivo} vazio, pulando.")
            continue

        registros = json.loads(conteudo)
        if not isinstance(registros, list) or len(registros) == 0:
            continue

        valores = [
            (
                registro.get("id"),
                "QuestionnaireResponse",
                nome_arquivo,
                delivery_date,
                json.dumps(registro, ensure_ascii=False),
            )
            for registro in registros
        ]

        execute_values(cursor, sql, valores, page_size=1000)
        total += len(valores)

    logger.info(f"questionnaire_response: {total} registros inseridos.")
    return total


def listar_arquivos_pasta(s3, pasta):
    """Lista todos os arquivos de uma pasta de entrega no S3."""
    prefixo = f"{S3_PREFIX}{pasta}/"
    resposta = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefixo)
    arquivos = []
    for obj in resposta.get("Contents", []):
        chave = obj["Key"]
        if chave.endswith("/") or "/responses/" in chave:
            continue
        arquivos.append(chave)
    return arquivos


# ── Principal ─────────────────────────────────────────────────────────────────


def main():
    inicio = datetime.now()
    logger.info("=== Iniciando ingestão prontlife_raw ===")

    s3 = conectar_s3()
    conn = conectar_banco()
    cursor = conn.cursor()

    try:
        garantir_schema(cursor)
        conn.commit()

        pendentes = pastas_pendentes(s3, cursor)

        if not pendentes:
            mensagem = f"Nenhuma pasta nova encontrada em s3://{S3_BUCKET}/{S3_PREFIX}"
            logger.warning(mensagem)
            raise RuntimeError(mensagem)

        resumo = {}

        for pasta in pendentes:
            logger.info(f"--- Processando pasta {pasta} ---")
            delivery_date = f"{pasta[:4]}-{pasta[4:6]}-{pasta[6:]}"

            arquivos = listar_arquivos_pasta(s3, pasta)
            for chave in arquivos:
                nome_tabela = deduzir_nome_tabela(chave)
                total = processar_arquivo(s3, cursor, S3_BUCKET, chave, delivery_date)
                resumo[nome_tabela] = resumo.get(nome_tabela, 0) + total

            total_responses = processar_responses(s3, cursor, pasta, delivery_date)
            resumo["questionnaire_response"] = (
                resumo.get("questionnaire_response", 0) + total_responses
            )

            conn.commit()
            logger.info(f"Pasta {pasta} commitada.")

        # Resumo final
        logger.info("=== Resumo da execução ===")
        total_geral = 0
        for nome, total in sorted(resumo.items()):
            if total == 0:
                logger.warning(f"  {nome}: vazio")
            else:
                logger.info(f"  {nome}: {total} registros")
                total_geral += total

        tempo = datetime.now() - inicio
        logger.info(f"  Total: {total_geral} registros inseridos")
        logger.info(f"  Tempo: {str(tempo).split('.')[0]}")
        logger.info("=== Ingestão concluída ===")

    except Exception as e:
        conn.rollback()
        logger.error(f"Erro durante a ingestão: {e}")
        raise

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()
