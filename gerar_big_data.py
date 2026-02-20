import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def generate_data(n_rows=50000):
    start_date = datetime(2024, 1, 1)
    skus = ['PROD_A', 'PROD_B', 'PROD_C', 'PROD_D', 'PROD_E']
    
    data = {
        'id_venda': np.arange(1, n_rows + 1),
        'sku': np.random.choice(skus, n_rows),
        'quantidade': np.random.randint(1, 100, n_rows),
        'data_venda': [(start_date + timedelta(days=np.random.randint(0, 400))).strftime('%Y-%m-%d') for _ in range(n_rows)],
        'preco_unitario': np.random.uniform(10.0, 500.0, n_rows).round(2)
    }
    
    df = pd.DataFrame(data)
    # Adicionando 500 duplicatas propositais para testar seu dbt
    df = pd.concat([df, df.sample(500)])
    
    df.to_parquet('vendas_vasto.parquet')
    print(f"✅ 50.500 linhas geradas em vendas_vasto.parquet!")

if __name__ == "__main__":
    generate_data()
