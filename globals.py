# Bibliotecas ----
import pandas as pd


# Objetos globais ----
pasta = "dados/"
df_previsao = pd.read_parquet(pasta + "df_previsao.parquet")
df_ipca = pd.concat([
    (
        pd.read_parquet(pasta + "df_mensal.parquet")
        .filter(["ipca"])
        .dropna()
        .reset_index()
        .rename(columns = {"ipca": "valor", "data": "data_referencia", "index": "data_referencia"})
        .assign(variavel = "IPCA", tipo = "Observado")
    ),
    df_previsao
])

modelos = df_previsao.query("variavel == 'IPCA'")["tipo"].unique().tolist()
