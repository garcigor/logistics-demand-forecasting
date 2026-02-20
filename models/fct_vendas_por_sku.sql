{{ config(materialized='table') }}

SELECT 
    sku,
    SUM(quantidade) as total_vendido,
    COUNT(DISTINCT id_venda) as qtd_pedidos,
    MAX(data_venda) as ultima_venda
FROM {{ ref('stg_vendas') }}
GROUP BY 1
ORDER BY total_vendido DESC
