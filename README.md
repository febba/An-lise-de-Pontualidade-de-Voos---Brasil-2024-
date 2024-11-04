**Projeto de Análise de Pontualidade de Voos ✈️**

Este projeto realiza uma análise detalhada da pontualidade e atrasos de voos, permitindo identificar tendências e padrões nos dados de companhias aéreas e aeroportos. Os insights são exibidos em um dashboard criado no Databricks, facilitando a interpretação e a tomada de decisões baseadas nos dados.

**Objetivo 🎯**

Analisar e visualizar o desempenho das companhias aéreas e aeroportos em relação à pontualidade e atraso de voos, identificando as empresas e aeroportos mais pontuais e os que mais atrasam.

**Estrutura do Projeto 🗂️**

Organização dos Dados em Camadas Medallion

O projeto adota o modelo Medallion Architecture para gerenciar e transformar os dados em três camadas principais:

- Bronze: Dados brutos e originais.
- Silver: Dados limpos e pré-processados.
- Gold: Dados prontos para análise e visualização, otimizados para consultas.

**Dataframes Utilizados**
Principais DataFrames

- df_voos: Contém informações detalhadas dos voos, incluindo horários de partida e chegada, situação do voo, e detalhes das companhias aéreas.
- df_aerodromos: Inclui informações dos aeroportos, como localidade e coordenadas geográficas.

**DataFrames Processados para a Camada Gold**

- top_5_empresas_pontuais e top_5_empresas_atrasos: Identificam as cinco empresas aéreas mais pontuais e as cinco com mais atrasos, considerando apenas companhias com mais de 100 voos.
- top_5_pontuais_aeroporto e top_5_atrasos_aeroporto: Informam os cinco aeroportos mais pontuais e os cinco que mais atrasam, considerando apenas aeroportos com mais de 100 voos.
- df_data_gold: Agrupa os voos por data para análise temporal dos atrasos e percentuais de atraso.

**Transformações e Tratamento de Dados 🔄**

- Conversão de Horários: Ajuste de colunas de horários de strings para timestamps, permitindo cálculos de atraso e antecipação.
- Cálculo de Métricas: Geração de métricas como percentuais de atraso, adiantamento e pontualidade, essenciais para a visualização dos dados.
- Agrupamento e Filtragem: Agrupamento por mês-ano e dia da semana para análise sazonal e semanal. O filtro de 100 voos por companhia e por aeroporto garante relevância nos resultados.
- Formatação de Dados: Formatação de percentuais com duas casas decimais para melhorar a legibilidade no dashboard.

**Dashboard e Visualizações 📊**

O dashboard foi configurado no Databricks com as seguintes visualizações:

- Top 5 Empresas Aéreas Mais Pontuais e Mais Atrasadas: Um gráfico de barras compara as cinco companhias mais pontuais e as cinco com maior índice de atraso.

- Top 5 Aeroportos Mais Pontuais e Mais Atrasados: Outra visualização de barras mostra os aeroportos mais pontuais e com mais atrasos, facilitando a análise por localização.

- Análise Temporal por Dia da Semana: Mostra a variação dos atrasos ao longo dos dias da semana.

- Atrasos por Mês-Ano: Um gráfico de linha exibe os atrasos por mês, identificando tendências ao longo do tempo.
