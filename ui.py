# Bibliotecas ----
from shiny import ui
from shinywidgets import output_widget
from globals import modelos

# Front end ----
sidebar = ui.sidebar(
    ui.markdown("Acompanhe as previsões automatizadas dos principais indicadores macroeconômicos do Brasil."),
    ui.input_select(
        id = "modelos",
        label = ui.strong("Selecione o modelo:"),
        choices = modelos,
        selected = modelos[0],
        multiple = False
    )
)

app_ui = ui.page_navbar(
    ui.nav_panel(
        "Previsão",
        ui.layout_columns(
            ui.output_ui("card_yoy"),
            ui.output_ui("card_mom"),
            ui.output_ui("card_last")
        ),
        ui.layout_columns(
            ui.card(output_widget("fanchart")),
            ui.card(ui.output_data_frame("fantable"))
        )
    ),
    ui.nav_panel(
        "Tracking",
        ui.card(ui.output_data_frame("tracking"))
    ),
    sidebar = sidebar,
    title = ui.img(src = "https://aluno.analisemacro.com.br/download/59787/?tmstv=1712933415", height = "40px"),
    window_title = "Painel de Previsões"
)
