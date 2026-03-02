import pandas as pd
import numpy as np

def feature_engineering(chunk):
    try:
        df_c = chunk.copy()
        
        # A. Interação (Produto)
        df_c['radius_x_texture'] = df_c['mean_radius'] * df_c['mean_texture']
        
        # B. Termo Quadrático
        df_c['area_sq'] = df_c['mean_area'] ** 2
        
        # C. Razão
        eps = 1e-8
        df_c['concavity_per_smoothness'] = df_c['mean_concavity'] / (df_c['mean_smoothness'] + eps)
        
        # D. Agregações por linha
        # Filtrando apenas o que é numérico para não dar erro com a coluna 'target' ou 'diagnosis'
        numeric_df = df_c.select_dtypes(include=[np.number])
        df_c['row_mean'] = numeric_df.mean(axis=1)
        df_c['row_std'] = numeric_df.std(axis=1)
        
        return df_c, True
    except Exception as e:
        return f"Erro na coluna: {str(e)}", False

def worker_process(input_q, output_q, worker_id):
    while True:
        item = input_q.get()
        if item is None:
            output_q.put((None, None))
            break
        idx, chunk = item
        res, success = feature_engineering(chunk)
        output_q.put((idx, res))