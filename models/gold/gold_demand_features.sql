{{ config(materialized='table') }}

with vendas_diarias as (
    -- 1. Agrega as vendas por dia e por produto (SKU)
    select
        data_venda,
        sku,
        sum(quantidade) as total_vendido
    from {{ ref('stg_vendas') }}
    group by 1, 2
),

features_calculadas as (
    -- 2. Cria as variáveis explicativas (Features) para o algoritmo
    select
        data_venda,
        sku,
        total_vendido,
        -- Calcula a média móvel dos últimos 7 dias para capturar tendências
        avg(total_vendido) over (
            partition by sku 
            order by data_venda 
            rows between 6 preceding and current row
        ) as media_movel_7d_vendas
    from vendas_diarias
)

select * from features_calculadas
