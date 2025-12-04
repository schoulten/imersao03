# Bibliotecas ----

import time
import pandas as pd
import numpy as np
from datetime import datetime
from dateutil.relativedelta import relativedelta


# Funções ----

# Função de coleta JSON com retentativas
def ler_json_com_retentativa(url):
  max_retentativas = 5
  tentativa = 1
  while tentativa <= max_retentativas:
    try:
      df = pd.read_json(url)
      return df
    except Exception as e:
      tentativa =+ 1
      print(f"Falha na coleta de dados: {e}")
      time.sleep(2)
  print(f"Falha após {max_retentativas} tentativas")

# Função de coleta CSV com retentativas
def ler_csv_com_retentativa(*ars, **kwargs):
  max_retentativas = 5
  tentativa = 1
  while tentativa <= max_retentativas:
    try:
      df = pd.read_csv(*ars, **kwargs)
      return df
    except Exception as e:
      tentativa =+ 1
      print(f"Falha na coleta de dados: {e}")
      time.sleep(2)
  print(f"Falha após {max_retentativas} tentativas")

# Função para calcular intervalos de datas
def criar_intervalo_datas(data_inicio: datetime):
    intervalos_data = []
    data_inicio_corrente = data_inicio

    while data_inicio_corrente < datetime.now():
        end_date = data_inicio_corrente + relativedelta(years=5) - relativedelta(days=1)
        intervalos_data.append((data_inicio_corrente, end_date))
        data_inicio_corrente = end_date + relativedelta(days=1)

    return intervalos_data

# Função de coleta de dados do BCB/SGS
def coleta_bcb_sgs(codigo, id, data_inicio, freq):

  urls = []
  if freq == "Diária":
    data_inicio = pd.to_datetime(data_inicio, format = "%d/%m/%Y").to_pydatetime()
    intervalos_data = criar_intervalo_datas(data_inicio)
    for intervalo in intervalos_data:
      url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados?formato=json&dataInicial={intervalo[0].strftime('%d/%m/%Y')}&dataFinal={intervalo[1].strftime('%d/%m/%Y')}"
      urls.append(url)
  else:
    url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados?formato=json&dataInicial={data_inicio}"
    urls.append(url)

  print(f"Coletando a série {codigo} do BCB/SGS...")
  dfs = []
  for url in urls:
    dfs.append(ler_json_com_retentativa(url))

  df = (
      pd.concat(dfs)
      .assign(data = lambda x: pd.to_datetime(x.data, format = "%d/%m/%Y"))
      .set_index("data")
      .rename(columns = {"valor": id})
      )
  return df

# Função de coleta de dados do BCB/ODATA
def coleta_bcb_odata(url, id):
  print(f"Coletando a série {id} do BCB/ODATA...")
  df = ler_csv_com_retentativa(url, decimal = ",")
  return(df)

# Função de coleta de dados do IBGE/SIDRA
def coleta_ibge_sidra(url, id):
  print(f"Coletando a série {id} do IBGE/SIDRA...")
  df = (
      ler_json_com_retentativa(f"{url}?formato=json")
      .query("V not in ['Valor', '...', '-']")
      .assign(
          data = lambda x: pd.to_datetime(x["D3C"], format = "%Y%m"),
          **{id: lambda x: x["V"].astype(float)}
        )
      .filter(["data", id])
      .set_index("data")
      )
  return(df)

# Função de coleta de dados do IPEADATA
def coleta_ipeadata(codigo, id):
  print(f"Coletando a série {id} do IPEADATA...")
  df = ler_json_com_retentativa(f"http://www.ipeadata.gov.br/api/odata4/ValoresSerie(SERCODIGO='{codigo}')")
  df = (
      pd.DataFrame.from_records(df.value)
      .assign(
          data = lambda x: pd.to_datetime(pd.to_datetime(x["VALDATA"], utc = True).dt.strftime("%Y-%m-%d")),
          **{id: lambda x: x["VALVALOR"].astype(float)}
        )
      .filter(["data", id])
      .set_index("data")
    )
  return(df)

# Função de coleta de dados do FRED
def coleta_fred(codigo, id):
  print(f"Coletando a série {id} do FRED...")
  df = (
      ler_csv_com_retentativa(f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={codigo}")
      .assign(
          data = lambda x: pd.to_datetime(x["observation_date"]),
          **{id: lambda x: x[codigo].astype(float)}
        )
      .filter(["data", id])
      .set_index("data")
    )
  return(df)

# Função para transformar dados, conforme definido nos metadados
def transformar(x, tipo):

  switch = {
      "1": lambda x: x,
      "2": lambda x: x.diff(),
      "3": lambda x: x.diff().diff(),
      "4": lambda x: np.log(x),
      "5": lambda x: np.log(x).diff(),
      "6": lambda x: np.log(x).diff().diff()
  }

  if tipo not in switch:
      raise ValueError("Tipo inválido")

  return switch[tipo](x)