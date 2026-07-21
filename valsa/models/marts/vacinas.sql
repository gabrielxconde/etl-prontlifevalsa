{{
    config(
        materialized = 'table',
        tags = ['mart', 'vacinas']
    )
}}
with base_resposta as (
    select
        id_resposta,
        id_paciente,
        id_atendimento,
        max(data_resposta) as data_resposta,
        max(status) as status,
        max(case when texto_pergunta = 'Vacinas em dia?' then nullif(trim(coalesce(resposta_escolha, resposta_codigo, resposta_texto)), '') end) as vacinas_em_dia,
        max(case when texto_pergunta = 'Possui calendário vacinal atualizado?' then nullif(trim(coalesce(resposta_escolha, resposta_codigo, resposta_texto)), '') end) as calendario_vacinal_atualizado,
        max(case when texto_pergunta = 'Administrada vacinas com a valsa?' then nullif(trim(coalesce(resposta_escolha, resposta_codigo, resposta_texto)), '') end) as administrada_valsa,
        max(case when texto_pergunta = 'Data da administração:' then resposta_data end) as data_administracao
    from {{ ref('resposta_valsa_visita_enfermagem_rotina') }}
    where status = 'Concluído'
    group by
        id_resposta,
        id_paciente,
        id_atendimento
),
vacinas_raw as (
    select
        id_resposta,
        nullif(trim(coalesce(resposta_escolha, resposta_codigo, resposta_texto)), '') as vacina_original
    from {{ ref('resposta_valsa_visita_enfermagem_rotina') }}
    where status = 'Concluído'
      and texto_pergunta in (
          'Vacinas em dia:',
          'Quais vacinas?',
          'Quais:'
      )
      and nullif(trim(coalesce(resposta_escolha, resposta_codigo, resposta_texto)), '') is not null
),
vacinas_padronizadas as (
    select
        id_resposta,
        case
            when upper(vacina_original) = 'INFLUENZA' then 'Influenza'
            when upper(vacina_original) = 'PNEMONIA 13' then 'Pneumocócicas'
            when upper(vacina_original) = 'PNEUMOCÓCICAS V13 E V26' then 'Pneumocócicas'
            else null
        end as vacina
    from vacinas_raw
),
final as (
    -- Deduplicamos por paciente + vacina + data de administração, e não por
    -- id_resposta: a mesma dose real pode ser reconfirmada em visitas
    -- posteriores, gerando várias respostas de questionário diferentes para
    -- a mesma vacina/data. Mantemos apenas 1 linha por dose real, priorizando
    -- a resposta mais recente (data_resposta desc) como representante.
    select distinct on (
        b.id_paciente,
        v.vacina,
        b.data_administracao
    )
        b.id_resposta,
        b.id_paciente,
        b.id_atendimento,
        v.vacina,
        b.data_administracao,
        b.administrada_valsa,
        b.vacinas_em_dia,
        b.calendario_vacinal_atualizado,
        b.data_resposta,
        b.status
    from base_resposta b
    inner join vacinas_padronizadas v
        on b.id_resposta = v.id_resposta
    where v.vacina in (
        'Influenza',
        'Pneumocócicas'
    )
    and b.data_administracao is not null
    order by
        b.id_paciente,
        v.vacina,
        b.data_administracao,
        b.data_resposta desc
)
select *
from final
