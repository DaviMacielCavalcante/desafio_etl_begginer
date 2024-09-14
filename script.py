import pandas as pd
import numpy as np
import os 
import warnings
from sqlalchemy import create_engine

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
            sheet_renames = {
                'Brasil; Telecomunicações': 'telecom',
                'Brasil; Tecnologia da inform...': 'ti',
                'Brasil; Serviços audiovisuais': 'serv_audiovisuais',
                'Brasil; Edição e edição inte...': 'ed_e_ed_integradas_a_impressao',
                'Brasil; Agências de notícias...': 'agencia_noticias',
                'Notas': 'notas'
            }
            for sheet_name in xls.sheet_names:

                if sheet_name in sheet_renames:

                    if sheet_name in sheet_renames.keys():
                        df_nome = sheet_renames[sheet_name]
                    else:
                        df_nome = sheet_name  

                    df = pd.read_excel(xls, sheet_name=sheet_name, skiprows=4)
                    dataframes[df_nome] = df

def padronizar_nomes_colunas(cols):
    return cols.str.lower().str.replace('(', '').str.replace(')', '').str.replace('ç', 'c').str.replace(' - ' ,'_').str.replace('ã', 'a').str.replace('á', 'a').str.replace('í', 'i').str.replace('ú', 'u').str.replace('é', 'e').str.replace('/', '_').str.replace('%', '').str.strip().str.replace(' ', '_').str.replace('unnamed:_0', 'ano')
    
def tratar_df(df):
    
    nomes_colunas = padronizar_nomes_colunas(df.columns)
    nomes_colunas = ['ano', 'receita_liquida','coef_var_receita_liquida',                             'custo_mercadorias', 'coef_var_custo_mercadorias','subvencoes_receitas_op',                                         'coef_var_subvencoes_receitas', 'valor_bruto_producao','coef_var_valor_bruto_producao',                                        'consumo_intermediario_total', 'coef_var_consumo_intermediario_total',                                         'consumo_mercadorias_reposicao', 'coef_var_consumo_mercadorias_reposicao',                                        'consumo_combustiveis', 'coef_var_combustiveis', 'consumo_servicos_terceiros',                                        'coef_var_servicos_terceiros', 'consumo_alugueis_imoveis','coef_var_alugueis_imoveis',                                        'consumo_seguros', 'coef_var_seguros', 'consumo_comunicacao', 'coef_var_comunicacao',                                        'consumo_energia_gas_agua', 'coef_var_energia_gas_agua', 'consumo_outros_custos',                                        'coef_var_outros_custos', 'valor_adicionado_bruto', 'coef_var_valor_adicionado_bruto',                                        'gastos_pessoal_total', 'coef_var_gastos_pessoal_total', 'gastos_salarios_remuneracoes',                                         'coef_var_salarios_remuneracoes', 'gastos_previdencia_social',                                         'coef_var_previdencia_social', 'gastos_fgts', 'coef_var_fgts',                                         'gastos_previdencia_privada', 'coef_var_previdencia_privada',                                         'gastos_indenizacoes_trabalhistas', 'coef_var_indenizacoes_trabalhistas',                                         'gastos_beneficios_empregados', 'coef_var_beneficios_empregados', 'pis_folha_pagamento',                                         'coef_var_pis_folha_pagamento', 'excedente_operacional_bruto',                                         'coef_var_excedente_operacional_bruto', 'pessoal_ocupado', 'coef_var_pessoal_ocupado',                                         'numero_empresas', 'coef_var_numero_empresas']

    df.columns = nomes_colunas

    if len(nomes_colunas) == len(df.columns):
        df.columns = nomes_colunas
        
    df = df.iloc[:-1, :]

    df['ano'] = pd.to_datetime(df['ano'])
    df['ano'] = df['ano'].dt.year

    colunas_numericas = [col for col in df.columns if "ano" not in col and "coef" not in col]
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
    if key != "notas":
        dataframes_atualizados[key] = tratar_df(value)


user = ""
pwd = ""
host = "localhost"
porta = ''
db = ''

engine = create_engine(f'postgresql+psycopg2://{user}:{pwd}@{host}:{porta}/{db}')

for key,value in dataframes_atualizados.items():
    value.to_sql(f'{key}', engine, if_exists='replace', index=False)