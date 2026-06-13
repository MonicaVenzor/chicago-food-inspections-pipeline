/*
    Modelo: stg_food_inspections
    Fuente: raw_data.food_inspections
    
    Transformaciones aplicadas en esta capa:
    1. Renombrado a snake_case consistente
    2. Conversión de tipos: inspection_date TEXT -> DATE
    3. Normalización de city, facility_type, inspection_type a UPPER TRIM
    4. Parseo de risk: extrae nivel numérico y etiqueta limpia
    5. violation_count: cuenta infracciones por pipe sin parsear texto libre
    6. Flags de calidad de datos documentados y trazables
    7. Descarte de columna location (redundante con lat/lon)

    Decisiones de gobernanza:
    - city != 'CHICAGO' se marca out_of_jurisdiction, no se elimina
    - violations IS NULL con results = 'Fail' se marca fail_without_violations
    - risk = 'All' se mapea a 'UNKNOWN'
    - facility_type con freq < umbral se agrupa en 'OTHER' en capa mart
*/

with source as (
    select * from {{ source('raw_data', 'food_inspections') }}
),

cleaned as (
    select
        -- Identificadores
        inspection_id,
        license_number,

        -- Nombres de establecimiento
        trim(dba_name)                          as dba_name,
        trim(aka_name)                          as aka_name,

        -- Tipo de establecimiento normalizado
        upper(trim(facility_type))              as facility_type,

        -- Riesgo: extrae número y etiqueta limpia
        case
            when upper(trim(risk)) like '%1%'   then 'HIGH'
            when upper(trim(risk)) like '%2%'   then 'MEDIUM'
            when upper(trim(risk)) like '%3%'   then 'LOW'
            else 'UNKNOWN'
        end                                     as risk_level,

        case
            when upper(trim(risk)) like '%1%'   then 1
            when upper(trim(risk)) like '%2%'   then 2
            when upper(trim(risk)) like '%3%'   then 3
            else null
        end                                     as risk_rank,

        -- Dirección
        trim(address)                           as address,
        upper(trim(city))                       as city,
        upper(trim(state))                      as state,
        trim(zip)                               as zip,

        -- Fecha: conversión explícita desde texto MM/DD/YYYY
        to_date(inspection_date, 'MM/DD/YYYY')  as inspection_date,

        -- Tipo de inspección normalizado
        upper(trim(inspection_type))            as inspection_type,

        -- Resultado
        trim(results)                           as results,

        -- Violations: texto limpio + conteo por pipe
        violations                              as violations_raw,
        case
            when violations is null then 0
            else array_length(
                string_to_array(violations, '|'), 1
            )
        end                                     as violation_count,

        -- Coordenadas
        latitude,
        longitude,

        -- Metadata de ingesta
        ingested_at,

        -- FLAGS DE CALIDAD DE DATOS
        case
            when upper(trim(city)) != 'CHICAGO'
            then true else false
        end                                     as flag_out_of_jurisdiction,

        case
            when violations is null
             and trim(results) = 'Fail'
            then true else false
        end                                     as flag_fail_without_violations,

        case
            when upper(trim(risk)) not like '%1%'
             and upper(trim(risk)) not like '%2%'
             and upper(trim(risk)) not like '%3%'
            then true else false
        end                                     as flag_invalid_risk,

        case
            when license_number is null
            then true else false
        end                                     as flag_missing_license

    from source
)

select * from cleaned
