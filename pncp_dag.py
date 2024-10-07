from datetime import datetime, timedelta

import pytz
import requests
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator


def extract_ops(**kwargs):
    ti = kwargs["ti"]
    uri = 'https://pncp.gov.br/api/search/?tipos_documento=edital&ordenacao=-data&pagina=1&tam_pagina=200&status=recebendo_proposta'
    headers = {
        'User-Agent': 'Integraplace'
    }

    try:
        request = requests.get(uri, headers=headers, timeout=15)
        if request.status_code == 200:
            list_ops = request.json()['items']

            brasilia_tz = pytz.timezone('America/Sao_Paulo')
            date_now = f'{datetime.now(brasilia_tz)}'[:10]
            date_now_datetime = datetime.strptime(date_now, "%Y-%m-%d")
            query = 'INSERT INTO public.pncp_op("Id", data_publicacao_pncp, url, orgao_cnpj, orgao_nome, description, esfera_nome, uf, modalidade_licitacao_nome, situacao_nome) VALUES'

            for op in list_ops:
                data_atualizacao_pncp = op['data_atualizacao_pncp'][:10]

                if datetime.strptime(data_atualizacao_pncp, "%Y-%m-%d") == date_now_datetime:
                    query = (f"{query} ("
                             f"'{op['id']}',"
                             f"'{op['data_publicacao_pncp']}',"
                             f"'https://pncp.gov.br/app/editais/{op['item_url'][9:]}',"
                             f"'{op['orgao_cnpj'] if op['orgao_cnpj'] != None else ''}',"
                             f"'{op['orgao_nome'][:80].replace("'", "")}',"
                             f"'{op['description'][:500].replace("'", "")}',"
                             f"'{op['esfera_nome'][:30].replace("'", "") if op['esfera_nome'] != None else ''}',"
                             f"'{op['uf']}',"
                             f"'{op['modalidade_licitacao_nome'][:40]}',"
                             f"'{op['situacao_nome'][:40]}'"
                             f"),")

            query_pncp = query[:-1] + 'ON CONFLICT ("Id") DO NOTHING;'
            ti.xcom_push('query_pncp', query_pncp)
        else:
            raise AirflowSkipException

    except:
        raise AirflowSkipException


default_args = {
    'owner': 'Matheus Silva',
    'retries': 0,
    'start_date': datetime(2024, 9, 9),
    'retry_delay': timedelta(minutes=10)
}

with DAG(
        dag_id='PNCP',
        default_args=default_args,
        schedule_interval="0 7 * * *",
        catchup=False,
        max_active_runs=1
) as dag:
    extract_task = PythonOperator(
        task_id='extract_ops',
        python_callable=extract_ops
    )

    load_task = PostgresOperator(
        task_id='load_pncp_op',
        postgres_conn_id='BD_local',
        sql="{{ ti.xcom_pull(key='query_pncp') }}"
    )

    extract_task >> load_task
