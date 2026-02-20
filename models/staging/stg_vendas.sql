WITH source_data AS (
    SELECT 
        id_venda,
        sku,
        quantidade,
        preco_unitario,
        CAST(data_venda AS DATE) as data_venda
    FROM {{ source('raw_data', 'vendas_bruto') }}
),

deduplicated AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY id_venda ORDER BY data_venda DESC) as rn
    FROM source_data
)

SELECT 
    id_venda,
    sku,
    quantidade,
    preco_unitario,
    (quantidade * preco_unitario) as faturamento_venda,
    data_venda,
    -- Variáveis de Sazonalidade (essenciais para ML)
    EXTRACT(DAYOFWEEK FROM data_venda) as dia_semana,
    EXTRACT(MONTH FROM data_venda) as mes,
    EXTRACT(DAY FROM data_venda) as dia_mes
FROM deduplicated
WHERE rn = 1
