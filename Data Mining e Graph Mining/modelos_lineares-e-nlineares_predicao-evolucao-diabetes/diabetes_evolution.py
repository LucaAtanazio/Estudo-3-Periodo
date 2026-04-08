# 0. SETUP E IMPORTAÇÕES
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.datasets import load_diabetes
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_squared_error, r2_score, mean_absolute_error

# Modelos Lineares e Não Lineares (Aula 08)
from sklearn.linear_model import LinearRegression
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor, AdaBoostRegressor
from xgboost import XGBRegressor

# 1. CARREGAMENTO DOS DADOS
diabetes = load_diabetes()
X = pd.DataFrame(diabetes.data, columns=diabetes.feature_names)
y = diabetes.target

# 2. PREPARAÇÃO (Split e Scaling)
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

scaler = StandardScaler()
X_train_scaled = scaler.fit_transform(X_train)
X_test_scaled = scaler.transform(X_test)

# 3. MODELAGEM
# Criando a "equipe" de modelos conforme a aula
models = {
    "Linear Regression (Baseline)": LinearRegression(),
    "Decision Tree": DecisionTreeRegressor(random_state=42),
    "Random Forest": RandomForestRegressor(n_estimators=100, random_state=42),
    "AdaBoost": AdaBoostRegressor(n_estimators=100, random_state=42),
    "XGBoost": XGBRegressor(n_estimators=100, learning_rate=0.1, random_state=42)
}

# 4. AVALIAÇÃO
results = []

for name, model in models.items():
    model.fit(X_train_scaled, y_train)
    predictions = model.predict(X_test_scaled)
    
    # Métricas de Regressão
    mse = mean_squared_error(y_test, predictions)
    rmse = np.sqrt(mse)
    r2 = r2_score(y_test, predictions)
    mae = mean_absolute_error(y_test, predictions)
    
    results.append({
        "Modelo": name,
        "R2 Score": r2,
        "RMSE": rmse,
        "MAE": mae
    })

# 5. RESULTADOS
df_results = pd.DataFrame(results).sort_values(by='R2 Score', ascending=False)
print("--- COMPARATIVO DE MODELOS (DIABETES) ---")
print(df_results)

# Plotando a comparação
plt.figure(figsize=(10, 6))
sns.barplot(x='R2 Score', y='Modelo', data=df_results, hue='Modelo', legend=False, palette='viridis')
plt.title('Comparação de Performance (R² Score)')
plt.show()