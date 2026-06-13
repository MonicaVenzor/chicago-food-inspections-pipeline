/*
    Modelo: mart_food_inspections
    Fuente: staging.stg_food_inspections

    Propósito: tabla analítica desnormalizada lista para Power BI.
    Granularidad: una fila por inspección.

    Decisiones de gobernanza:
    - Se excluyen registros out_of_jurisdiction del mart principal
    - facility_type nulo -> 'UNKNOWN' (decisión documentada: nulo en dimensión
      analítica rompe agregados; se preserva facility_type_original para auditoría)
    - facility_type con < 10 registros -> 'OTHER'
    - violation_count = 0 cuando violations IS NULL
    - repeat_fail_flag: establecimientos con > 1 falla histórica
*/

with staging as (
    select * from {{ ref('stg_food_inspections') }}
    where flag_out_of_jurisdiction = false
),

facility_type_counts as (
    select
        facility_type,
        count(*) as type_count
    from staging
    group by facility_type
),

license_history as (
    select
        license_number,
        count(*) filter (where results = 'Fail')            as total_fails,
        count(*) filter (where results in ('Pass',
            'Pass w/ Conditions'))                          as total_passes,
        count(*)                                            as total_inspections
    from staging
    where license_number is not null
    group by license_number
),

with_previous as (
    select
        inspection_id,
        license_number,
        inspection_date,
        lag(inspection_date) over (
            partition by license_number
            order by inspection_date
        ) as previous_inspection_date
    from staging
    where license_number is not null
),

final as (
    select
        -- Identificadores
        s.inspection_id,
        s.license_number,

        -- Establecimiento
        s.dba_name,
        s.aka_name,
        case
            when s.facility_type is null          then 'UNKNOWN'
            when fc.type_count < 10               then 'OTHER'
            else s.facility_type
        end                                                 as facility_type_grouped,
        s.facility_type                                     as facility_type_original,

        -- Riesgo
        s.risk_level,
        s.risk_rank,

        -- Ubicación
        s.address,
        s.city,
        s.zip,
        s.latitude,
        s.longitude,

        -- Fecha y dimensiones temporales
        s.inspection_date,
        extract(year  from s.inspection_date)::int          as inspection_year,
        extract(month from s.inspection_date)::int          as inspection_month,
        extract(quarter from s.inspection_date)::int        as inspection_quarter,
        to_char(s.inspection_date, 'YYYY-MM')               as inspection_year_month,

        -- Tipo de inspección
        s.inspection_type,

        -- Resultado
        s.results,
        case
            when s.results in ('Pass',
                'Pass w/ Conditions')             then 1
            else 0
        end                                                 as pass_flag,
        case
            when s.results = 'Fail'               then 1
            else 0
        end                                                 as fail_flag,

        -- Violaciones
        s.violation_count,
        s.violations_raw,

        -- Métricas historial
        coalesce(lh.total_inspections, 1)                   as license_total_inspections,
        coalesce(lh.total_fails, 0)                         as license_total_fails,
        coalesce(lh.total_passes, 0)                        as license_total_passes,
        case
            when coalesce(lh.total_fails, 0) > 1  then true
            else false
        end                                                 as repeat_fail_flag,

        -- Días entre inspecciones
        case
            when wp.previous_inspection_date is not null
            then (s.inspection_date - wp.previous_inspection_date)
            else null
        end                                                 as days_since_previous_inspection,

        -- Flags de calidad
        s.flag_fail_without_violations,
        s.flag_invalid_risk,
        s.flag_missing_license,

        -- Metadata
        s.ingested_at

    from staging s
    left join facility_type_counts fc
        on s.facility_type = fc.facility_type
    left join license_history lh
        on s.license_number = lh.license_number
    left join with_previous wp
        on s.inspection_id = wp.inspection_id
)

select * from final
