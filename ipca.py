# Bibliotecas ----

# Importa bibliotecas
import numpy as np
import pandas as pd
from skforecast.ForecasterAutoreg import ForecasterAutoreg
from sklearn.linear_model import Ridge, HuberRegressor
from sklearn.preprocessing import PowerTransformer
from utils import transformar

# Organização de dados ----

# Planilha de metadados
df_metadados = pd.read_excel(
    io = "https://docs.google.com/spreadsheets/d/1NB1fkck-ol1y5fIETWDkoYG2sFhlgyKsADPfADK_98s/export?format=xlsx",
    sheet_name = "Metadados"
    ).set_index("Identificador").filter(["Transformação"]).astype(str)

# Importa dados online
pasta = "dados/"
dados_brutos = pd.read_parquet(pasta + "df_mensal.parquet")

# Converte frequência
dados_tratados = dados_brutos.asfreq("MS")

# Separa Y
y = dados_tratados.ipca.dropna()

# Separa X
x = dados_tratados.drop(labels = "ipca", axis = "columns").copy()

# Computa transformações
for col in x.columns.to_list():
  x[col] = transformar(x[col], df_metadados.loc[col, "Transformação"])

# Filtra amostra
inicio_treino = pd.to_datetime("2004-01-01") # amostra inicial de treinamento
y = y[y.index >= inicio_treino]
x = x.query("index >= @inicio_treino and index <= @y.index.max()")

# Conta por coluna proporção de NAs em relação ao nº de obs. do IPCA
prop_na = x.isnull().sum() / y.shape[0]

# Remove variáveis que possuem mais de 20% de NAs
x = x.drop(labels = prop_na[prop_na >= 0.2].index.to_list(), axis = "columns")

# Preenche NAs restantes com a vizinhança
x = x.bfill().ffill()

# Adiciona dummies sazonais
dummies_sazonais = (
    pd.get_dummies(y.index.month_name())
    .astype(int)
    .drop(labels = "December", axis = "columns")
    .set_index(y.index)
)
x = x.join(other = dummies_sazonais, how = "outer")


# Produção de previsões ----

# Semente de reprodução
semente = 1984
# Horizonte de previsão
h = 12

# Seleção final de variáveis
x_reg = [
    "expec_ipca_top5_curto_prazo",
    "ic_br",
    "cambio_brl_eur",
    "ipc_s"
    ] + dummies_sazonais.columns.to_list() # + 1 lag

# Reestima os 2 melhores modelos com amostra completa
modelo1 = ForecasterAutoreg(
    regressor = Ridge(random_state = semente),
    lags = 1,
    transformer_y = PowerTransformer(),
    transformer_exog = PowerTransformer()
    )
modelo1.fit(y, x[x_reg])

modelo2 = ForecasterAutoreg(
    regressor = HuberRegressor(),
    lags = 1,
    transformer_y = PowerTransformer(),
    transformer_exog = PowerTransformer()
    )
modelo2.fit(y, x[x_reg])

# Período de previsão fora da amostra
periodo_previsao = pd.date_range(
    start = modelo1.last_window.index[0] + pd.offsets.MonthBegin(1),
    end = modelo1.last_window.index[0] + pd.offsets.MonthBegin(h),
    freq = "MS"
    )

## Cenários

### Expectativas de inflação

# Coleta dados de expectativas de inflação (expec_ipca_top5_curto_prazo)
dados_focus_exp_ipca = (
    pd.read_csv(
        filepath_or_buffer = f"https://olinda.bcb.gov.br/olinda/servico/Expectativas/versao/v1/odata/ExpectativasMercadoTop5Mensais?$filter=Indicador%20eq%20'IPCA'%20and%20tipoCalculo%20eq%20'C'%20and%20Data%20ge%20'{periodo_previsao.min().strftime('%Y-%m-%d')}'&$format=text/csv",
        decimal = ",",
        converters = {
            "Data": pd.to_datetime,
            "DataReferencia": lambda x: pd.to_datetime(x, format = "%m/%Y")
            }
        ))

# Data do relatório Focus usada para construir cenário para expectativas de inflação
data_focus_exp_ipca = (
    dados_focus_exp_ipca
    .query("DataReferencia in @periodo_previsao")
    .Data
    .value_counts()
    .to_frame()
    .reset_index()
    .query("count == @h").query("Data == Data.max()")
    .Data
    .to_list()[0]
)

# Constrói cenário para expectativas de inflação (expec_ipca_top5_curto_prazo)
dados_cenario_exp_ipca = (
    dados_focus_exp_ipca
    .query("DataReferencia in @periodo_previsao")
    .query("Data == @data_focus_exp_ipca")
    .set_index("DataReferencia")
    .filter(["Mediana"])
    .rename(columns = {"Mediana": "expec_ipca_top5_curto_prazo"})
)

### Commodities

# Constrói cenário para commodities (ic_br)
dados_cenario_ic_br = (
    x
    .filter(["ic_br"])
    .dropna()
    .query("index >= @inicio_treino")
    .assign(mes = lambda x: x.index.month_name())
    .groupby(["mes"], as_index = False)
    .ic_br
    .median()
    .set_index("mes")
    .join(
        other = (
            periodo_previsao
            .rename("data")
            .to_frame()
            .assign(mes = lambda x: x.data.dt.month_name())
            .drop("data", axis = "columns")
            .reset_index()
            .set_index("mes")
        ),
        how = "outer"
    )
    .set_index("data")
    .sort_index()
)

### Câmbio

# Coleta dados de expectativas do câmbio (cambio_brl_eur)
dados_focus_cambio = (
    pd.read_csv(
        filepath_or_buffer = f"https://olinda.bcb.gov.br/olinda/servico/Expectativas/versao/v1/odata/ExpectativasMercadoTop5Mensais?$filter=Indicador%20eq%20'C%C3%A2mbio'%20and%20tipoCalculo%20eq%20'M'%20and%20Data%20ge%20'{modelo1.last_window.index[0].strftime('%Y-%m-%d')}'&$format=text/csv",
        decimal = ",",
        converters = {
            "Data": pd.to_datetime,
            "DataReferencia": lambda x: pd.to_datetime(x, format = "%m/%Y")
            }
        ))

# Data do relatório Focus usada para construir cenário para câmbio
data_focus_cambio = (
    dados_focus_cambio
    .query("DataReferencia in @periodo_previsao or DataReferencia == @modelo1.last_window.index[0]")
    .Data
    .value_counts()
    .to_frame()
    .reset_index()
    .query("count == @h+1").query("Data == Data.max()")
    .Data
    .to_list()[0]
)

# Constrói cenário para câmbio (cambio_brl_eur)
dados_cenario_cambio = (
    dados_focus_cambio
    .query("DataReferencia in @periodo_previsao or DataReferencia == @modelo1.last_window.index[0]")
    .query("Data == @data_focus_cambio")
    .set_index("DataReferencia")
    .filter(["Mediana"])
    .rename(columns = {"Mediana": "cambio_brl_eur"})
    .assign(
        cambio_brl_eur = lambda x: transformar(x.cambio_brl_eur, df_metadados.loc["cambio_brl_eur"].iloc[0])
        )
    .dropna()
)

### Prévia da inflação

# Constrói cenário para prévia de preços (ipc_s)
dados_cenario_ipc_s = (
    x
    .filter(["ipc_s"])
    .dropna()
    .query("index >= @inicio_treino")
    .assign(mes = lambda x: x.index.month_name())
    .groupby(["mes"], as_index = False)
    .ipc_s
    .median()
    .set_index("mes")
    .join(
        other = (
            periodo_previsao
            .rename("data")
            .to_frame()
            .assign(mes = lambda x: x.data.dt.month_name())
            .drop("data", axis = "columns")
            .reset_index()
            .set_index("mes")
        ),
        how = "outer"
    )
    .set_index("data")
    .sort_index()
)

### Dummies

# Junta cenários e gera dummies sazonais
dados_cenarios = (
    dados_cenario_exp_ipca
    .join(
        other = [
            dados_cenario_ic_br,
            dados_cenario_cambio,
            dados_cenario_ipc_s,
            (
                pd.get_dummies(dados_cenario_exp_ipca.index.month_name())
                .astype(int)
                .drop(labels = "December", axis = "columns")
                .set_index(dados_cenario_exp_ipca.index)
            )
            ],
        how = "outer"
        )
    .asfreq("MS")
)

## Previsão

# Produz previsões
previsao1 = (
   modelo1.predict_interval(
      steps = h,
      exog = dados_cenarios,
      n_boot = 5000,
      random_state = semente
      )
    .reset_index()
    .assign(
        variavel = "IPCA",
        tipo = "Ridge",
        data_previsao = pd.Timestamp.today()
        )
    .rename(
        columns = {
        "index": "data_referencia",
        "pred": "valor",
        "lower_bound": "ic_inferior",
        "upper_bound": "ic_superior"
        }
    )
)

previsao2 = (
   modelo2.predict_interval(
      steps = h,
      exog = dados_cenarios,
      n_boot = 5000,
      random_state = semente
      )
    .reset_index()
    .assign(
        variavel = "IPCA",
        tipo = "Huber",
        data_previsao = pd.Timestamp.today()
        )
    .rename(
        columns = {
        "index": "data_referencia",
        "pred": "valor",
        "lower_bound": "ic_inferior",
        "upper_bound": "ic_superior"
        }
    )
)

# Salvar previsões
df_previsao = pd.concat([previsao1, previsao2])
df_previsao.drop(labels = "data_previsao", axis = "columns").to_parquet(pasta + "df_previsao.parquet")
df_previsao.to_csv(pasta + "tracking.csv", mode = "a", index = False, header = False)
