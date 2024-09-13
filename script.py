import pandas as pd
import numpy as np
import os 
import warnings

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=pd.errors.SettingWithCopyWarning)


dataframes = {}
dataframes_atualizados = {}

def replace_outliers_by_median(df, col):
    """
Função criada para substituir outliers de um pequeno conjunto de dados pela mediana. 
Obs: Usar apenas em colunas que você sabe, via boxplot, que possuam outliers.
    """
    # Calcular os quartis e o intervalo interquartil (IQR)
    Q1 = df[col].quantile(0.25)
    Q3 = df[col].quantile(0.75)
    IQR = Q3 - Q1

    # Definir os limites para outliers
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR

    # Calcular a mediana
    median = df[col].median()

    # Substituir os outliers pela mediana
    df.loc[df[col] < lower_bound, df[col]] = median
    df.loc[df[col] > upper_bound, df[col]] = median

def carregar_dfs(path):
    """
    Função para retirar cada sheet presente no arqui xlsx,
    e adicionar em um dicionario de dataframes
    """
    for arquivo in os.listdir(path):
        if arquivo.endswith(".xlsx"):
            file_path = os.path.join(path, arquivo)
            xls = pd.ExcelFile(file_path)
            for sheet_name in xls.sheet_names:
                df = pd.read_excel(xls, sheet_name=sheet_name, skiprows=4)
                df_nome = f"{sheet_name}"
                dataframes[df_nome] = df

def padronizar_nomes_colunas(cols):
    return cols.str.lower().str.replace('(', '').str.replace(')', '').str.replace('ç', 'c').str.replace(' - ' ,'_').str.replace('ã', 'a').str.replace('á', 'a').str.replace('í', 'i').str.replace('ú', 'u').str.replace('é', 'e').str.replace('/', '_').str.replace('%', '').str.strip().str.replace(' ', '_').str.replace('unnamed:_0', 'ano')
    
def tratar_df(df):
   # df.info()
    
    nomes_colunas = padronizar_nomes_colunas(df.columns)
    df.columns = nomes_colunas
        
    df = df.iloc[:-1, :]

    df['ano'] = pd.to_datetime(df['ano'])
    df['ano'] = df['ano'].dt.year

    colunas_numericas = [col for col in df.columns if "ano" not in col and "coeficiente" not in col]
    df[df.isin(['-', "..."])] = np.nan
    df[colunas_numericas].apply(pd.to_numeric)

    for col in colunas_numericas:
        if df[col].dtype != "float64":
            df[col] = pd.to_numeric(df[col])

    for col in colunas_numericas:
        replace_outliers_by_median(df[colunas_numericas], col)
        df[col].fillna(df[col].median(), inplace=True)

    return df


carregar_dfs('./datasets/')    

for key,value in dataframes.items():
    if key != "Notas":
        dataframes_atualizados[key] = tratar_df(value)

