# Lab13: Detecção de Anomalias em Transações Financeiras

Este repositório contém a implementação e análise do **Lab13**, focado na detecção de fraudes em cartões de crédito utilizando técnicas de aprendizado não supervisionado.

## Objetivo
O objetivo central é identificar padrões raros e suspeitos em um dataset de transações financeiras (Credit Card Fraud), onde as anomalias representam apenas 0,17% do volume total de dados.

## Dataset
* **Fonte:** Credit Card Fraud Dataset (via TensorFlow/Google Storage).
* **Características:** Dados transformados via PCA (V1 a V28), além das variáveis `Time` e `Amount`.
* **Desafio:** Desbalanceamento extremo entre transações legítimas e fraudulentas.

## Algoritmos Implementados
A análise experimental cobriu quatro abordagens principais:

1. **Isolation Forest (IF):** Modelo baseado no isolamento de anomalias em árvores de decisão. Apresentou o melhor equilíbrio entre *Precision* e *Recall*.
2. **Local Outlier Factor (LOF):** Técnica baseada na densidade local dos dados.
3. **DBSCAN:** Algoritmo de clustering baseado em densidade (testado com e sem redução de dimensionalidade PCA).
4. **One-Class SVM:** Modelo robusto para definir fronteiras de normalidade em alta dimensão.

## Resultados e Insights
* **Melhor Desempenho:** O **Isolation Forest** se destacou pela eficiência computacional e capacidade de identificar fraudes com um F1-Score de 0.26, mostrando-se a escolha mais viável para cenários de produção.
* **O Trade-off do SVM:** O One-Class SVM demonstrou um *Recall* altíssimo (0.96), mas com o custo de gerar um volume massivo de falsos positivos (>135.000), exigindo um ajuste fino de hiperparâmetros.
* **Impacto da Escala:** A normalização via Z-Score (StandardScaler) na variável `Amount` foi crucial para que os modelos baseados em distância/densidade não fossem ofuscados por valores extremos.

## Como Executar
1. Clone o repositório.
2. Certifique-se de ter as bibliotecas instaladas:
   ```bash
   pip install pandas numpy scikit-learn matplotlib seaborn