# main.py
import streamlit as st
from base import processar_dados
import pandas as pd
import matplotlib.pyplot as plt
import datetime

# Configuração inicial
st.set_page_config(layout="wide")
BAR_COLOR = "#1d4c50"  # Cor padrão para todas as barras

# Header com logo e título
col1, col2 = st.columns([1, 3])
with col1:
    st.image("Feet/relatorio_pmt/assets/Hectare_Logo.png", width=150)
with col2:
    st.title("Risco de Enquadramento")
st.markdown("---")

# Widgets de seleção
col1, col2 = st.columns(2)
with col1:
    today = datetime.date.today()
    first_day = today.replace(day=1)
    last_day_prev = first_day - datetime.timedelta(days=1)
    data_base = st.date_input("Data de Referência", value=last_day_prev, max_value=today)
with col2:
    fundo = st.selectbox("Fundo", ["HCTR", "HCHG"])

# Processar e filtrar dados
df_vortx, df_pl, df_fundos = processar_dados(str(data_base))
df_vortx_filtrado = df_vortx[df_vortx["Carteira"] == fundo]
df_pl_filtrado = df_pl[df_pl["Carteira"] == fundo]

if df_vortx_filtrado.empty or df_pl_filtrado.empty:
    st.warning(f"Não há dados disponíveis para {fundo} na data {data_base}")
else:
    pl_fundo = df_pl_filtrado["PL"].sum()
    
    # Gráfico 1: Alocação em CRI
    def criar_grafico_alocacao(valor, titulo, limite_min, limite_max, label):
        fig, ax = plt.subplots(figsize=(10, 2))
        ax.barh([0], [valor], height=0.5, color=BAR_COLOR)
        ax.axvline(x=limite_min, color="red", linestyle="--", alpha=0.5, label=f"Mínimo ({limite_min}%)")
        ax.axvline(x=limite_max, color="green", linestyle="--", alpha=0.5, label=f"Máximo ({limite_max}%)")
        ax.set_xlim(0, max(limite_max, valor * 1.1))
        ax.set_yticks([])
        ax.set_xlabel(f"% do PL alocado em {label}")
        ax.legend(loc="upper right")
        offset = valor * 0.05  # 5% do valor atual # Texto deslocado para a esquerda (subtraindo 5% do valor)
        posicao_texto = max(valor - offset, 0)  # Garante que não seja negativo
        ax.text(posicao_texto, 0, f"{valor:.2f}%", 
            ha="center", va="center", color="white", fontweight="bold")
    
        return fig
    
    st.subheader(f"Alocação em CRI - {fundo}")
    mercado_cri = df_vortx_filtrado["MercadoAtual"].sum()
    perc_cri = (mercado_cri / pl_fundo) * 100
    st.pyplot(criar_grafico_alocacao(perc_cri, "CRI", 67, 100, "CRI"))
    
    # Gráfico 2: Top 10 Operações
    st.subheader(f"Top 10 Operações - {fundo}")
    top_operacoes = (df_vortx_filtrado.groupby("nome_op")["MercadoAtual"].sum()
                    .reset_index().sort_values("MercadoAtual", ascending=False).head(10))
    top_operacoes["% do PL"] = (top_operacoes["MercadoAtual"] / pl_fundo) * 100
    
    fig2, ax2 = plt.subplots(figsize=(10, 6))
    bars = ax2.barh(top_operacoes["nome_op"], top_operacoes["% do PL"], color=BAR_COLOR)
    ax2.axvline(x=10, color="red", linestyle="--", alpha=0.5, label="Limite (10%)")
    ax2.set_xlabel("% do PL")
    ax2.set_ylabel("Operação")
    ax2.legend()
    
    for bar in bars:
        width = bar.get_width()
        ax2.text(width, bar.get_y() + bar.get_height()/2, f"{width:.2f}%", 
                ha="left", va="center", color="white", fontweight="bold")
    st.pyplot(fig2)
    
    # Gráficos específicos para HCTR
    if fundo == "HCTR":
        df_fundos_filtrado = df_fundos[df_fundos["Carteira"] == fundo]
        
        if not df_fundos_filtrado.empty:
            # Gráfico 3: Alocação em Fundos
            st.subheader(f"Alocação em Fundos Imobiliários - {fundo}")
            mercado_fundos = df_fundos_filtrado["MercadoAtual"].sum()
            perc_fundos = (mercado_fundos / pl_fundo) * 100
            st.pyplot(criar_grafico_alocacao(perc_fundos, "Fundos", 0, 33, "Fundos Imobiliários"))
            
            # Gráfico 4: Composição dos Fundos
            st.subheader(f"Composição dos Fundos - {fundo}")
            composicao = (df_fundos_filtrado.groupby("Titulo")["MercadoAtual"].sum()
            .reset_index().sort_values("MercadoAtual", ascending=False))
            composicao["%"] = (composicao["MercadoAtual"] / mercado_fundos) * 100

            # Paleta de cores definida
            cores_pizza = ["#b0d4d6", "#6fa3a5", "#3f7477", "#1d4c50", "#15393b", "#0e2527"]

            fig4, ax4 = plt.subplots(figsize=(8, 8))
            ax4.pie(composicao["MercadoAtual"], 
            labels=composicao["Titulo"],
            autopct='%1.1f%%', 
            startangle=90, 
            colors=cores_pizza[:len(composicao)])  # Usa apenas as cores necessárias
            ax4.axis('equal')
            st.pyplot(fig4)
        else:
            st.info(f"Não há alocação em fundos imobiliários para {fundo} na data {data_base}")
