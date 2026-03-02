# Previsão de Churn - Telecom 

Este projeto de Data Mining foi desenvolvido para a disciplina de **Data Mining e Graph Mining** no curso de Tecnologia em Inteligência Artificial da **SENAI FATESG**. O objetivo principal é prever a rotatividade de clientes (Churn) utilizando técnicas avançadas de engenharia de atributos e modelos de Ensemble.

---

## Performance Final
Os resultados superaram as metas estabelecidas para o projeto:
- **ROC AUC:** 0.9464 (Meta: 0.80)
![ROC](graficos_training\06_curva_roc_final.png)
- **PR AUC:** 0.9537 (Meta: 0.40)
![PR](graficos_training\07_curva_pr_final.png)
- **Acurácia:** 88%

---

##  Etapas do Projeto

### 1. Preparação e Limpeza de Dados
- **Tratamento de Nulos:** Imputação via Mediana para variáveis como `total_charges`.
- **Padronização:** Conversão de colunas para *snake_case* e limpeza de identificadores não preditivos.
- **Análise Exploratória (EDA):** Identificação de padrões de faturamento e tempo de contrato (tenure).

### 2. Engenharia de Atributos (Feature Engineering)
Criação de variáveis estratégicas para aumentar o poder preditivo:
- `newbie_at_risk`: Clientes novos em contratos sem fidelidade.
- `financial_instability_score`: Pontuação baseada em histórico de pagamentos e faturas atrasadas.
- `log_total_charges`: Normalização da escala financeira.

### 3. Seleção de Atributos e Balanceamento
- **Lasso (L1):** Utilizado para eliminar colinearidade e reduzir o ruído, resultando em 22 atributos finais.
- **SMOTE:** Aplicação de sobreamostragem da classe minoritária para equilibrar o dataset (50/50), corrigindo o desbalanceamento original de 85/15.

### 4. Modelagem (Ensemble Learning)
Utilização de um **Voting Classifier (Soft)** combinando dois modelos robustos:
- **Random Forest:** Estabilidade e redução de variância (Bagging).
- **XGBoost:** Alta precisão e foco em erros residuais (Boosting).

---

## Estrutura do Repositório
- `notebooks/`: Contém os pipelines de preparação e avaliação.
- `graficos_prepracao_csv/`: Visualizações da EDA e balanceamento.
- `graficos_avaliacao/`: Curvas ROC e Precision-Recall.
- `Relatorio_Final_Data_Mining.pdf`: Documentação técnica completa em formato acadêmico.

---

## Autores
- **Discente:** Luca
- **Auxílio:** Frederico e Wassil
- **Docente:** André Luiz Esperidião

---
*Este projeto foi gerado automaticamente através de um pipeline de automação em Python.*