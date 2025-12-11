# Bibliotecas ----
from shiny import Inputs, Outputs, Session, render, reactive, ui
from shinywidgets import render_widget
from globals import df_ipca
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import numpy as np
from faicons import icon_svg


# Back end ----
def server(input: Inputs, output: Outputs, session: Session):

    @reactive.calc
    def obter_dados_fanchart():
        modelo_selecionado = input.modelos()
        dados = (
            df_ipca
            .query("tipo in [@modelo_selecionado, 'Observado']")
            .assign(
                tipo = lambda x: x["tipo"].replace({"Observado": "IPCA"}),
                data_referencia = lambda x: x["data_referencia"].dt.strftime("%Y-%m-%d")
                )
            .tail(12*15)
        )
        return dados
    
    @reactive.calc
    def preparar_dados_fantable():
        modelo_selecionado = input.modelos()
        df_fantable = (
            obter_dados_fanchart()
            .query("tipo == @modelo_selecionado")
            .filter(["data_referencia", "ic_inferior", "valor", "ic_superior"])
            .assign(data_referencia = lambda x: pd.to_datetime(x.data_referencia).dt.strftime("%m/%Y"))
            .rename(
                columns = {
                    "data_referencia": "Período",
                    "ic_inferior": "I.C. Inferior",
                    "valor": "Previsão",
                    "ic_superior": "I.C. Superior"
                }
            )
            .round(2)
        )
        return df_fantable

    @reactive.calc
    def acumular_previsao_mom_para_yoy():
        df_acumulado = (
            obter_dados_fanchart()
            .assign(
                valor_yoy = lambda x: ((((x.valor / 100) + 1).rolling(12).apply(lambda x: np.prod(x), raw = True) - 1)) * 100
            )
        )
        return df_acumulado

    @reactive.calc
    def obter_previsao_ano_corrente():
        modelo_selecionado = input.modelos()
        yoy = acumular_previsao_mom_para_yoy().assign(data_referencia = lambda x: pd.to_datetime(x.data_referencia))
        previsao = yoy.query("tipo == @modelo_selecionado")
        ano_corrente = previsao.data_referencia.min().year
        valor_previsao = previsao.query("data_referencia == data_referencia.min()").valor_yoy.round(2).iloc[0]
        return (ano_corrente, valor_previsao)

    @reactive.calc
    def obter_previsao_mensal():
        modelo_selecionado = input.modelos()
        mom = obter_dados_fanchart().assign(data_referencia = lambda x: pd.to_datetime(x.data_referencia))
        previsao = mom.query("tipo == @modelo_selecionado")
        mes_corrente = previsao.data_referencia.min().strftime("%m/%Y")
        valor_previsao = previsao.query("data_referencia == data_referencia.min()").valor.round(2).iloc[0]
        return (mes_corrente, valor_previsao)
    
    @reactive.calc
    def obter_ultimo_valor_mensal():
        mom = obter_dados_fanchart().assign(data_referencia = lambda x: pd.to_datetime(x.data_referencia))
        previsao = mom.query("tipo == 'IPCA'")
        mes_corrente = previsao.data_referencia.max().strftime("%m/%Y")
        valor_realizado = previsao.query("data_referencia == data_referencia.max()").valor.round(2).iloc[0]
        return (mes_corrente, valor_realizado)

    @reactive.calc
    def gerar_tabela_tracking():
        modelo_selecionado = input.modelos()
        df_tracking = pd.read_csv("dados/tracking.csv")
        df_historico = pd.read_parquet("dados/df_mensal.parquet")
        df_tracking = (
            df_tracking
            .assign(data_referencia = lambda x: pd.to_datetime(x.data_referencia))
            .set_index("data_referencia")
            .join(df_historico.filter(["ipca"]), how = "left")
            .reset_index()
            .drop(labels = ["ic_inferior", "ic_superior", "variavel"], axis = "columns")
            .query("tipo == @modelo_selecionado")
            .assign(
                data_referencia = lambda x: pd.to_datetime(x.data_referencia).dt.strftime("%m/%Y"),
                **{"Erro de Previsão": lambda x: x.ipca - x.valor}
                )
            .rename(
                columns = {
                    "data_referencia": "Data Referência",
                    "valor": "Previsão",
                    "tipo": "Modelo",
                    "data_previsao": "Data de Previsão",
                    "ipca": "Observado"
                }
            )
            .round(2)
        )
        return df_tracking

    @render_widget
    def fanchart():
        df_fanchart = obter_dados_fanchart()
        fig = px.line(
            data_frame = df_fanchart,
            x = "data_referencia",
            y = "valor",
            color = "tipo",
            title = "Previsão do IPCA",
            labels = {"data_referencia": "Data", "valor": "Valor", "tipo": "Série"},
            hover_data = {"data_referencia": True, "valor": ':.2f', "tipo": True}
        )


        df_ic = df_fanchart.query("tipo != 'Observado'").sort_values("data_referencia")
        fig.add_trace(
            go.Scatter(
                x = df_ic["data_referencia"],
                y = df_ic["ic_superior"],
                mode = 'lines',
                line = dict(width=0),
                showlegend = False,
                hoverinfo = 'skip'
            )
        )
        fig.add_trace(
            go.Scatter(
                x = df_ic["data_referencia"],
                y = df_ic["ic_inferior"],
                mode = 'lines',
                line = dict(width=0),
                fill = 'tonexty',
                fillcolor = 'rgba(30,144,255,0.18)',
                showlegend = False,
                hoverinfo = 'skip'
            )
        )

        return fig

    @render.data_frame
    def fantable():
        return preparar_dados_fantable()
    
    @render.ui
    def card_yoy():
        return ui.value_box(
            f"Previsão {obter_previsao_ano_corrente()[0]}",
            f"{obter_previsao_ano_corrente()[1]}%",
            showcase = icon_svg("calendar")
        )
    
    @render.ui
    def card_mom():
        return ui.value_box(
            f"Previsão {obter_previsao_mensal()[0]}",
            f"{obter_previsao_mensal()[1]}%",
            showcase = icon_svg("percent")
        )
    
    @render.ui
    def card_last():
        return ui.value_box(
            f"IPCA {obter_ultimo_valor_mensal()[0]}",
            f"{obter_ultimo_valor_mensal()[1]}%",
            showcase = icon_svg("magnifying-glass-chart")
        )
    
    @render.data_frame
    def tracking():
        return render.DataGrid(gerar_tabela_tracking(), summary = False)
