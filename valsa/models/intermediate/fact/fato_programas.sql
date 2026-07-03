{{
    config(
        materialized = 'table',
        tags = ['intermediate', 'fact']
    )
}}

with stg as (
    select * from {{ ref('stg_customepisodeofcare') }}
),

normalizado as (
    select
        *,
        {{ capitalize_name('category_display') }} as category_display_norm
    from stg
),

numerado_episodio as (
    select
        *,
        row_number() over (
            partition by episode_of_care_id
            order by delivery_date desc
        ) as rn_episodio
    from normalizado
),

mais_recente_episodio as (
    select *
    from numerado_episodio
    where rn_episodio = 1
),

numerado_negocio as (
    select
        *,
        row_number() over (
            partition by patient_id, category_display_norm, period_start
            order by
                delivery_date desc,
                case when status = 'active' then 0 else 1 end
        ) as rn_negocio
    from mais_recente_episodio
),

base_negocio as (
    select *
    from numerado_negocio
    where rn_negocio = 1
),

classificado as (
    select
        *,
        max(
            case
                when status = 'active'
                 and inactivation_datetime is null
                then 1
                else 0
            end
        ) over (
            partition by patient_id, category_display_norm
            order by period_start
            rows between unbounded preceding and 1 preceding
        ) as ja_tinha_ativo_anterior
    from base_negocio
),

programas_validos as (
    select *
    from classificado
    where coalesce(ja_tinha_ativo_anterior, 0) = 0
      and category_display_norm is not null
),

final as (
    select
        -- chave
        episode_of_care_id                                                  as id_episodio,

        -- paciente
        patient_id                                                          as id_paciente,

        -- programa
        category_display_norm                                               as nome_programa,

        -- gestor
        care_manager_id                                                     as id_gestor,
        care_manager_name                                                   as nome_gestor,
        care_manager_role                                                   as papel_gestor,

        -- status traduzido
        case status
            when 'active'   then 'Ativo'
            when 'inactive' then 'Inativo'
            else status
        end                                                                  as status_paciente,

        -- período
        (period_start at time zone 'America/Sao_Paulo')::timestamp          as data_admissao,
        (period_end at time zone 'America/Sao_Paulo')::timestamp            as data_fim_prevista,
        (inactivation_datetime at time zone 'America/Sao_Paulo')::timestamp as data_inativacao,

        -- metadado de origem (data em que esse registro foi entregue pela Prontlife)
        delivery_date                                                       as data_entrega,

        -- observação
        note                                                                as observacao

    from programas_validos
)

select * from final
