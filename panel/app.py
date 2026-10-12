"""Panel de la plataforma: lo que producen los datos, el modelo y la monitorización.

Una aplicación de Streamlit que lee del lago y enseña, en cinco pestañas, el estado
de la plataforma de extremo a extremo. Solo pinta: las consultas están en
credit_risk/panel_datos.py.

Uso (necesita el grupo de dependencias `panel`):
    uv run --group panel streamlit run panel/app.py
"""

from __future__ import annotations

import altair as alt
import pandas as pd
import streamlit as st

from credit_risk import panel_datos as datos
from credit_risk.config import (
    BRONZE_URI,
    DERIVA_URI,
    LAKE_DIR,
    MLFLOW_URI,
    MODELS_DIR,
    SCORING_URI,
)
from credit_risk.deriva import MINIMO_SOLICITUDES

ALMACEN = LAKE_DIR / "warehouse.duckdb"
AUC_TFM = 0.7900
PSI_MODERADO, PSI_IMPORTANTE = 0.10, 0.25

# Un solo azul para las magnitudes; gris para las referencias. Los colores de estado
# solo se usan para el estado, y siempre acompañados de su etiqueta escrita.
AZUL, GRIS = "#2a78d6", "#898781"
ESTADOS = {
    "estable": "#0ca30c",
    "moderado": "#fab219",
    "importante": "#d03b3b",
    "sin valorar": GRIS,  # PSI calculado con muy pocos casos
}
ICONO = {"estable": "✅", "moderado": "⚠️", "importante": "🔴"}

st.set_page_config(page_title="Credit Risk Data Platform", page_icon="📊", layout="wide")


@st.cache_data(ttl=60)
def cargar(nombre: str, *args):
    """Las consultas se guardan un minuto: el panel no relee el lago en cada clic."""
    return getattr(datos, nombre)(*args)


def decimal(valor: float | None, decimales: int = 4) -> str:
    """Número con coma decimal (los miles van con punto)."""
    if valor is None or pd.isna(valor):
        return "—"
    return f"{valor:.{decimales}f}".replace(".", ",")


def porcentaje(valor: float | None, decimales: int = 2) -> str:
    return "—" if valor is None or pd.isna(valor) else f"{decimal(100 * valor, decimales)} %"


def miles(valor: float | None) -> str:
    return "—" if valor is None else f"{valor:,.0f}".replace(",", ".")


# Formato de los números en ejes y descripciones: miles con punto y coma decimal
ESPANOL = alt.Locale(
    number=alt.NumberLocale(decimal=",", thousands=".", grouping=[3], currency=["", " €"])
)


def mostrar(grafico: alt.Chart | alt.LayerChart, use_container_width: bool = True) -> None:
    st.altair_chart(grafico.configure(locale=ESPANOL), use_container_width=use_container_width)


def barras(tabla: pd.DataFrame, x: alt.X, y: alt.Y, descripcion: list[alt.Tooltip]) -> alt.Chart:
    return (
        alt.Chart(tabla)
        .mark_bar(color=AZUL, cornerRadiusEnd=4)
        .encode(x=x, y=y, tooltip=descripcion)
    )


def referencia(
    valor: float, texto: str, horizontal: bool = True, a_la_derecha: bool = True
) -> alt.LayerChart:
    """Línea de referencia discontinua con su etiqueta (en lugar de un segundo eje).

    La etiqueta se pone en el lado donde las barras quedan por debajo de la línea.
    """
    base = alt.Chart(pd.DataFrame({"valor": [valor], "texto": [texto]}))
    if horizontal:
        linea = base.mark_rule(color=GRIS, strokeDash=[4, 4]).encode(y="valor:Q")
        etiqueta = base.mark_text(
            align="right" if a_la_derecha else "left",
            dx=-4 if a_la_derecha else 4,
            dy=-8,
            color=GRIS,
        ).encode(
            y="valor:Q", x=alt.value("width") if a_la_derecha else alt.value(0), text="texto:N"
        )
    else:
        linea = base.mark_rule(color=GRIS, strokeDash=[4, 4]).encode(x="valor:Q")
        etiqueta = base.mark_text(align="left", dx=4, dy=-4, color=GRIS).encode(
            x="valor:Q", y=alt.value(0), text="texto:N"
        )
    return linea + etiqueta


# --- Datos de cabecera --------------------------------------------------------------

mart = cargar("resumen_mart", ALMACEN)
modelo = cargar("modelo_vigente", MODELS_DIR)
informe = cargar("deriva", DERIVA_URI, modelo["modelo"]) if modelo else pd.DataFrame()
psi_puntuacion = float(informe.iloc[0]["psi"]) if len(informe) else None
# Con pocas solicitudes el PSI es ruido: se enseña, pero sin darle un estado (la misma
# regla que aplica el comando monitor-drift para no dar falsas alarmas)
comparadas = int(informe.iloc[0]["solicitudes"]) if len(informe) else 0
psi_fiable = comparadas >= MINIMO_SOLICITUDES
POCO_FIABLE = (
    f"Con {comparadas} solicitudes (menos de {MINIMO_SOLICITUDES}) el PSI no es fiable: "
    "cada tramo tiene un puñado de casos y las diferencias son ruido, no deriva."
)

st.title("Credit Risk Data Platform")
st.caption(f"Lago: `{LAKE_DIR}` · los datos se releen cada minuto")

if mart is None:
    st.warning(
        "Todavía no hay mart en este lago. Construye el pipeline (`simulate-source`, "
        "`ingest-bronze` y `dbt build`) y vuelve a cargar la página."
    )
    st.stop()

resumen, pestana_datos, cartera, pestana_modelo, puntuacion = st.tabs(
    ["Resumen", "Datos", "Cartera", "Modelo", "Puntuación y deriva"]
)

# --- Resumen ------------------------------------------------------------------------

with resumen:
    st.subheader("El estado de la plataforma, de un vistazo")
    a, b, c, d = st.columns(4)
    a.metric("Solicitudes en el mart", miles(mart["solicitudes"]))
    b.metric("Tasa de impago", porcentaje(mart["tasa_impago"]))
    c.metric("Solicitudes por puntuar", miles(mart["pendientes"]))
    d.metric("Columnas del mart", miles(mart["columnas"]))

    a, b, c, d = st.columns(4)
    if modelo:
        a.metric(
            "AUC del modelo vigente",
            decimal(modelo["auc"]),
            f"{modelo['auc'] - AUC_TFM:+.4f} frente al TFM".replace(".", ","),
            delta_color="off",
        )
        b.metric("Gini", decimal(modelo["gini"]))
        c.metric("Variables del modelo", miles(modelo["variables"]))
    else:
        a.metric("AUC del modelo vigente", "—")
        b.caption("Aún no hay modelo publicado: ejecuta `train-model`.")
    if psi_puntuacion is None:
        d.metric("Deriva de la puntuación (PSI)", "—")
    else:
        situacion = informe.iloc[0]["estado"]
        d.metric("Deriva de la puntuación (PSI)", decimal(psi_puntuacion, 3))
        if psi_fiable:
            d.caption(f"{ICONO[situacion]} {situacion}")
        else:
            d.caption("no fiable: pocas solicitudes")

    st.caption(f"Última ingesta incluida en el mart: {mart['ingerido_hasta']:%d/%m/%Y %H:%M}")
    st.markdown(
        "**Cómo leerlo.** Los datos llegan en entregas diarias, se limpian y agregan con dbt "
        "hasta el *mart* (una fila por solicitud), sobre el que se entrena el modelo. El "
        "modelo puntúa las solicitudes que aún no tienen resultado, y la deriva vigila que "
        "esas solicitudes se sigan pareciendo a las del entrenamiento."
    )

# --- Datos --------------------------------------------------------------------------

with pestana_datos:
    st.subheader("¿Llegan los datos?")
    control = cargar("ingestas", BRONZE_URI)
    if control.empty:
        st.info("No hay entregas ingeridas todavía (`ingest-bronze`).")
    else:
        por_dia = control.groupby("fecha_entrega", as_index=False)["filas"].sum()
        por_dia["dia"] = pd.to_datetime(por_dia["fecha_entrega"]).dt.strftime("%d/%m")
        a, b, c = st.columns(3)
        a.metric("Entregas ingeridas", miles(por_dia["fecha_entrega"].nunique()))
        b.metric("Filas en bronze", miles(control["filas"].sum()))
        c.metric("Última ingesta", f"{control['ingerido_en'].max():%d/%m/%Y %H:%M}")

        st.markdown("**Filas ingeridas por día de entrega**")
        mostrar(
            barras(
                por_dia,
                alt.X(
                    "dia:O",
                    sort=None,
                    title="Día de entrega",
                    axis=alt.Axis(labelAngle=0, labelOverlap=True),
                ),
                alt.Y("filas:Q", title="Filas"),
                [
                    alt.Tooltip("dia:O", title="Día"),
                    alt.Tooltip("filas:Q", title="Filas", format=",.0f"),
                ],
            ).properties(height=300),
            use_container_width=True,
        )
        st.caption(
            "La primera entrega es mayor a propósito: el simulador del origen envía ese día "
            "las filas del historial que no se pueden asociar a ninguna solicitud."
        )
        st.markdown("**Filas por tabla** (suma de todas las entregas)")
        por_tabla = control.groupby("tabla", as_index=False)["filas"].sum()
        mostrar(
            barras(
                por_tabla,
                alt.X("filas:Q", title="Filas"),
                alt.Y("tabla:N", sort="-x", title=None, axis=alt.Axis(labelLimit=320)),
                [alt.Tooltip("tabla:N", title="Tabla"), alt.Tooltip("filas:Q", format=",.0f")],
            ).properties(height=260),
            use_container_width=True,
        )
        with st.expander("Ver la tabla de control completa"):
            st.dataframe(control, use_container_width=True, hide_index=True)

# --- Cartera ------------------------------------------------------------------------

with cartera:
    st.subheader("¿Quién impaga más?")
    dimension = st.selectbox("Desglosar por", list(datos.DIMENSIONES))
    desglose = cargar("impago_por", ALMACEN, dimension)
    st.caption(
        f"Solicitudes con resultado conocido: {miles(mart['con_resultado'])}. "
        "La línea discontinua es la tasa media de la cartera."
    )
    grafico = barras(
        desglose,
        # tickMinStep: una marca por punto porcentual (sin él, 1 % y 1,5 % se leen igual)
        alt.X(
            "tasa_impago:Q",
            title="Tasa de impago",
            axis=alt.Axis(format=".0%", tickMinStep=0.01),
        ),
        alt.Y("categoria:N", sort=None, title=None, axis=alt.Axis(labelLimit=320)),
        [
            alt.Tooltip("categoria:N", title=dimension),
            alt.Tooltip("tasa_impago:Q", title="Tasa de impago", format=".2%"),
            alt.Tooltip("solicitudes:Q", title="Solicitudes", format=",.0f"),
        ],
    )
    media = referencia(mart["tasa_impago"], f"media {porcentaje(mart['tasa_impago'], 1)}", False)
    mostrar(
        (grafico + media).properties(height=max(180, 38 * len(desglose))),
        use_container_width=True,
    )
    with st.expander("Ver los datos"):
        st.dataframe(
            desglose.assign(tasa_impago=desglose["tasa_impago"].map(porcentaje)),
            use_container_width=True,
            hide_index=True,
        )

# --- Modelo -------------------------------------------------------------------------

with pestana_modelo:
    st.subheader("¿Es bueno el modelo?")
    if not modelo:
        st.info("Aún no hay modelo publicado: ejecuta `train-model`.")
    else:
        a, b, c, d = st.columns(4)
        a.metric("AUC (prueba)", decimal(modelo["auc"]))
        b.metric("AUC del TFM", decimal(AUC_TFM))
        c.metric("Árboles", miles(modelo["arboles"]))
        d.metric("Solicitudes de prueba", miles(modelo["filas_prueba"]))
        st.caption(
            f"Modelo `{modelo['modelo']}` · entrenado con {miles(modelo['filas_entrenamiento'])} "
            f"solicitudes · commit `{modelo['commit'] or 'desconocido'}`"
        )

        if modelo.get("impago_por_decil"):
            st.markdown("**Tasa de impago real en cada decil de riesgo** (conjunto de prueba)")
            st.caption(
                "El modelo ordena las solicitudes por riesgo y las reparte en diez grupos "
                "iguales. Si ordena bien, el impago real crece de un decil al siguiente."
            )
            deciles = pd.DataFrame(
                {"decil": range(1, 11), "tasa_impago": modelo["impago_por_decil"]}
            )
            mostrar(
                (
                    barras(
                        deciles,
                        alt.X(
                            "decil:O",
                            title="Decil de riesgo (1 = menos, 10 = más)",
                            axis=alt.Axis(labelAngle=0),
                        ),
                        alt.Y("tasa_impago:Q", title="Tasa de impago", axis=alt.Axis(format=".0%")),
                        [
                            alt.Tooltip("decil:O", title="Decil"),
                            alt.Tooltip("tasa_impago:Q", title="Tasa de impago", format=".2%"),
                        ],
                    )
                    + referencia(
                        mart["tasa_impago"],
                        f"media de la cartera: {porcentaje(mart['tasa_impago'], 1)}",
                        a_la_derecha=False,
                    )
                ).properties(height=320),
                use_container_width=True,
            )

        st.markdown("**Historial de entrenamientos** (registro de MLflow)")
        historial = cargar("entrenamientos", MLFLOW_URI)
        if historial.empty:
            st.info("No hay entrenamientos registrados.")
        else:
            st.dataframe(
                pd.DataFrame(
                    {
                        "Modelo": historial["modelo"],
                        "Inicio (UTC)": historial["inicio"],
                        "AUC": historial["auc"].map(decimal),
                        "Gini": historial["gini"].map(decimal),
                        "KS": historial["ks"].map(decimal),
                        "Segundos": historial["segundos"].map(lambda v: decimal(v, 0)),
                        "Publicado": historial["publicado"].map({"True": "sí", "False": "no"}),
                        "Commit": historial["commit"],
                    }
                ),
                use_container_width=True,
                hide_index=True,
            )

# --- Puntuación y deriva ------------------------------------------------------------

with puntuacion:
    st.subheader("¿Qué riesgo tienen las solicitudes nuevas?")
    if not modelo:
        st.info("Aún no hay modelo publicado: ejecuta `train-model` y después `score`.")
    else:
        reparto = cargar("puntuaciones_por_decil", SCORING_URI, modelo["modelo"])
        if reparto.empty:
            st.info("El modelo vigente todavía no ha puntuado nada: ejecuta `score`.")
        else:
            st.caption(
                f"{miles(reparto['solicitudes'].sum())} solicitudes puntuadas con el modelo "
                f"`{modelo['modelo']}`. Los deciles se fijaron al entrenar: si la población "
                "no cambia, cada uno recibe el 10 %."
            )
            mostrar(
                (
                    barras(
                        reparto,
                        alt.X(
                            "decil_riesgo:O",
                            title="Decil de riesgo (1 = menos, 10 = más)",
                            axis=alt.Axis(labelAngle=0),
                        ),
                        alt.Y(
                            "fraccion:Q",
                            title="Solicitudes puntuadas",
                            axis=alt.Axis(format=".0%"),
                        ),
                        [
                            alt.Tooltip("decil_riesgo:O", title="Decil"),
                            alt.Tooltip("fraccion:Q", title="Fracción", format=".1%"),
                            alt.Tooltip("solicitudes:Q", title="Solicitudes", format=",.0f"),
                        ],
                    )
                    + referencia(0.10, "10 % esperado")
                ).properties(height=300),
                use_container_width=True,
            )

            st.markdown("**Consultar una solicitud**")
            cliente = st.number_input(
                "Identificador (SK_ID_CURR)", min_value=0, value=0, step=1, format="%d"
            )
            if cliente:
                ficha = cargar("puntuacion_de", SCORING_URI, modelo["modelo"], int(cliente))
                if ficha is None:
                    st.warning("Esa solicitud no está entre las puntuadas por este modelo.")
                else:
                    a, b, c = st.columns(3)
                    a.metric("Decil de riesgo", f"{ficha['decil_riesgo']} de 10")
                    b.metric("Puntuación del modelo", decimal(ficha["probabilidad_impago"], 3))
                    c.metric("Puntuada el", f"{ficha['puntuado_en']:%d/%m/%Y %H:%M}")
                    st.caption(
                        "La puntuación ordena por riesgo; no es una probabilidad real de impago "
                        "(el modelo se entrena con las clases equilibradas)."
                    )

        st.subheader("¿Se parecen a las del entrenamiento?")
        if informe.empty:
            st.info("No hay informe de deriva para este modelo: ejecuta `monitor-drift`.")
        else:
            situacion = informe.iloc[0]["estado"]
            if psi_fiable:
                st.markdown(
                    f"**Puntuación del modelo: PSI {decimal(psi_puntuacion, 3)} — "
                    f"{ICONO[situacion]} {situacion}.** Por debajo de "
                    f"{decimal(PSI_MODERADO, 2)} la población es estable; por encima de "
                    f"{decimal(PSI_IMPORTANTE, 2)} hay que revisar el modelo."
                )
            else:
                st.warning(POCO_FIABLE)
                st.markdown(f"**Puntuación del modelo: PSI {decimal(psi_puntuacion, 3)}.**")
            variables = informe.iloc[1:].head(15).copy()
            if not psi_fiable:
                variables["estado"] = "sin valorar"
            variables["etiqueta"] = variables.apply(
                lambda f: f"{decimal(f['psi'], 3)} · {f['estado']}", axis=1
            )
            st.markdown("**Las variables del modelo que más han cambiado**")
            base = alt.Chart(variables).encode(
                # Margen a la derecha para que quepa la etiqueta de la barra más larga
                x=alt.X(
                    "psi:Q",
                    title="PSI",
                    scale=alt.Scale(domainMax=max(float(variables["psi"].max()) * 1.2, 0.3)),
                ),
                # labelLimit: los nombres de las variables son largos y no deben cortarse
                y=alt.Y("variable:N", sort=None, title=None, axis=alt.Axis(labelLimit=360)),
            )
            barras_psi = base.mark_bar(cornerRadiusEnd=4).encode(
                color=alt.Color(
                    "estado:N",
                    scale=alt.Scale(domain=list(ESTADOS), range=list(ESTADOS.values())),
                    legend=None,  # cada barra lleva su estado escrito al lado
                ),
                tooltip=[
                    alt.Tooltip("variable:N", title="Variable"),
                    alt.Tooltip("psi:Q", title="PSI", format=".4f"),
                    alt.Tooltip("estado:N", title="Estado"),
                ],
            )
            # El estado va también escrito: el color nunca es la única pista
            texto = base.mark_text(align="left", dx=5).encode(text="etiqueta:N")
            umbral = referencia(PSI_IMPORTANTE, "importante", False)
            mostrar(
                (barras_psi + texto + umbral).properties(height=38 * len(variables) + 40),
                use_container_width=True,
            )
            with st.expander("Ver el informe completo"):
                st.dataframe(informe, use_container_width=True, hide_index=True)
