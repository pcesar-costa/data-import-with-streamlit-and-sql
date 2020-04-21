import os
import pyodbc
import pandas as pd
import streamlit as st

from time import sleep
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

server = os.getenv("SQL_HOST")
database = os.getenv("SQL_DB")
username = os.getenv("SQL_USERNAME")
password = os.getenv("SQL_PASSWORD")

def main():
    st.title("Importação de notas")
    st.info("Esta página tem como objetivo uma **_prova de conceito_** (POC) para importação de dados em um banco de dados SQL a partir de um arquivo csv.")
    
    GITHUB = "https://github.com/pcosta21"
    st.markdown(f"Source Code: [GitHub]({GITHUB}) | Arquivo modelo: [Uploader.CSV]({GITHUB})")

    model = pd.DataFrame(
        columns=['CODTURMA', 'CODDISC', 'NOTA', 'RA']
    )

    file = st.file_uploader('Selecione um arquivo', type='csv', encoding='utf-8')
    sample = st.slider('Tamanho da amostra: ', 1, 30, 5)
    sample_kind = st.checkbox('Amostra aleatória')
    valid = False
    if file is not None:
        with st.spinner("Carregando dados..."):
            df = pd.read_csv(file, sep=';', decimal='.')
            erro_modelo = SystemError('Modelo de importação não é válido')
            try:
                if(df.columns == model.columns).all():
                    valid = True
                else:
                    st.exception(erro_modelo)
            except:
                st.exception(erro_modelo)

    if valid:
        sleep(0.5)
        if(sample_kind):
            st.dataframe(df.sample(sample))
        else:
            st.dataframe(df.head(sample))

        st.title("Informações da importação")
        st.subheader("Quantidade total de registros")
        lendf = len(df)
        st.text(lendf)

        df_notas_zero = df.query('NOTA==0')
        df_null = df[df['NOTA'].isnull()]
        st.subheader("Alunos com notas zero:")
        if(len(df_notas_zero) > 0):
            st.dataframe(df_notas_zero)
            st.text(f"quantidade: {len(df_notas_zero)}/{lendf}")
        else:
            st.text("Não há dados a serem exibidos")

        st.subheader("Alunos sem notas:")
        if(len(df_null) > 0):
            st.dataframe(df_null)
            st.text(f"quantidade: {len(df_null)}/{lendf}")
        else:
            st.text("Não há dados a serem exibidos")

                

        st.title("Configurações de importação")
        codetapa = st.number_input("Código da etapa:", value=10, min_value=10, max_value=13, step=1)
        codprova = st.number_input("Código da prova:", value=1, min_value=1, max_value=10, step=1)
        codper = datetime.now().year
        st.number_input("Período letivo:", value=codper, min_value=codper, max_value=codper, step=1)
        fillna = st.checkbox('Importar com nota zero alunos que não possuem nota')

        if(fillna):
            df_import = df.copy()
            df_import['NOTA'].fillna(value=0, inplace=True)
        else:
            df_import = df.copy()

        importar = st.button("Realizar importação")            

        insertsql = os.getenv("SQL_INSERT")
        if(importar):
            conn_ = False
            placeholder_msg = st.empty()
            try:
                conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+password)
                sql = conn.cursor()
                conn_ = True
                placeholder_msg.success('Conexão com o banco de dados realizada com sucesso.')
                sleep(2)
            except:
                e = RuntimeError('Não foi possível se conectar ao banco de dados')
                placeholder_msg.exception(e)

            if(conn):
                placeholder_msg.info(f"CODPER: **{codper}** | CODETAPA: **{codetapa}** | CODPROVA: **{codprova}**")
                with st.spinner(f"Realizando importação no banco de dados..."):
                    sleep(1)
                    percent_complete = 0
                    sleep(1)
                    progress_bar = st.progress(percent_complete)
                    step = 100 / len(df_import)

                    placeholder_reg = st.empty()
                    placeholder_imp = st.empty()
                    placeholder_imp.button("CANCELAR IMPORTAÇÃO")

                    sleep(2)
                    for l in df_import.values:
                        sql.execute(insertsql, codper, l[1], codetapa, codprova, l[3], l[2])
                        percent_complete += step
                        progress_bar.progress(int(percent_complete))
                        placeholder_reg.text(f"RA: {l[3]} - CODDISC: {l[1]} - NOTA: {l[2]}")
                        
                    progress_bar.progress(100)
                    placeholder_reg.empty()
                    
                    sql.commit()
                    placeholder_imp.success('Importação finalizada')

                
 

if __name__ == '__main__':
    main()