import pandas as pd
import numpy as np
import os 
import warnings
from sqlalchemy import create_engine, Column, Integer, String, Numeric,  MetaData 
from sqlalchemy.ext.declarative import as_declarative, declared_attr
from sqlalchemy.orm import sessionmaker
from sqlalchemy import select
from pydantic import BaseModel
from fastapi import FastAPI, Depends, HTTPException

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=pd.errors.SettingWithCopyWarning)

DATABASE_URL = os.getenv("DATABASE_URL")

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autoflush=False,autocommit=False, bind=engine)

dataframes = {}
dataframes_atualizados = {}

app = FastAPI()

@as_declarative()
class Base:
    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()
    
    id = Column(Integer, primary_key=True, index=True)
    ano = Column(Integer, nullable=False)
    receita_liquida = Column(Numeric(10, 4), nullable=False)
    coef_var_receita_liquida = Column(String, nullable=False)
    custo_mercadorias = Column(Numeric(10, 4), nullable=False)
    coef_var_custo_mercadorias = Column(String, nullable=False)
    subvencoes_receitas_op = Column(Numeric(10, 4), nullable=False)
    coef_var_subvencoes_receitas = Column(String, nullable=False)
    valor_bruto_producao = Column(Numeric(10, 4), nullable=False)
    coef_var_valor_bruto_producao = Column(String, nullable=False)
    consumo_intermediario_total = Column(Numeric(10, 4), nullable=False)
    coef_var_consumo_intermediario_total = Column(String, nullable=False)           
    consumo_mercadorias_reposicao = Column(Numeric(10, 4), nullable=False)
    coef_var_consumo_mercadorias_reposicao = Column(String, nullable=False)
    consumo_combustiveis = Column(Numeric(10, 4), nullable=False)
    coef_var_combustiveis = Column(String, nullable=False)
    consumo_servicos_terceiros = Column(Numeric(10, 4), nullable=False)
    coef_var_servicos_terceiros = Column(String, nullable=False)
    consumo_alugueis_imoveis = Column(Numeric(10, 4), nullable=False)
    coef_var_alugueis_imoveis = Column(String, nullable=False)
    consumo_seguros = Column(Numeric(10, 4), nullable=False)
    coef_var_seguros = Column(String, nullable=False)
    consumo_comunicacao = Column(Numeric(10, 4), nullable=False)
    coef_var_comunicacao = Column(String, nullable=False)
    consumo_energia_gas_agua = Column(Numeric(10, 4), nullable=False)
    coef_var_energia_gas_agua = Column(String, nullable=False)
    consumo_outros_custos = Column(Numeric(10, 4), nullable=False)
    coef_var_outros_custos = Column(String, nullable=False)
    valor_adicionado_bruto = Column(Numeric(10, 4), nullable=False)
    coef_var_valor_adicionado_bruto = Column(String, nullable=False)
    gastos_pessoal_total = Column(Numeric(10, 4), nullable=False)
    coef_var_gastos_pessoal_total = Column(String, nullable=False)
    gastos_salarios_remuneracoes = Column(Numeric(10, 4), nullable=False)
    coef_var_salarios_remuneracoes = Column(String, nullable=False)
    gastos_previdencia_social = Column(Numeric(10, 4), nullable=False)
    coef_var_previdencia_social = Column(String, nullable=False)
    gastos_fgts = Column(Numeric(10, 4), nullable=False)
    coef_var_fgts = Column(String, nullable=False)
    gastos_previdencia_privada = Column(Numeric(10, 4), nullable=False)
    coef_var_previdencia_privada = Column(String, nullable=False)
    gastos_indenizacoes_trabalhistas = Column(Numeric(10, 4), nullable=False)
    coef_var_indenizacoes_trabalhistas = Column(String, nullable=False)
    gastos_beneficios_empregados = Column(Numeric(10, 4), nullable=False)
    coef_var_beneficios_empregados = Column(String, nullable=False)
    pis_folha_pagamento = Column(Numeric(10, 4), nullable=False)
    coef_var_pis_folha_pagamento = Column(String, nullable=False)
    excedente_operacional_bruto = Column(Numeric(10, 4), nullable=False)
    coef_var_excedente_operacional_bruto = Column(String, nullable=False)
    pessoal_ocupado = Column(Numeric(10, 4), nullable=False)
    coef_var_pessoal_ocupado = Column(String, nullable=False)
    numero_empresas = Column(Numeric(10, 4), nullable=False)
    coef_var_numero_empresas = Column(String, nullable=False)


def get_db_local():
    db= SessionLocal()
    try:
        yield db
    finally:
        db.close()

def get_table(name: str, metadata: MetaData):
    try:
        return Table(name, metadata, autoload_with=engine)
    except NoSuchTableError:
        raise HTTPException(status_code=404, detail=f"Tabela {name} não encontrada.")


class BaseCreate(BaseModel):
    ano: int
    receita_liquida: float
    coef_var_receita_liquida: str
    custo_mercadorias: float
    coef_var_custo_mercadorias: str
    subvencoes_receitas_op: float
    coef_var_subvencoes_receitas: str
    valor_bruto_producao: float
    coef_var_valor_bruto_producao: str
    consumo_intermediario_total: float
    coef_var_consumo_intermediario_total: str           
    consumo_mercadorias_reposicao: float
    coef_var_consumo_mercadorias_reposicao: str
    consumo_combustiveis: float
    coef_var_combustiveis: str
    consumo_servicos_terceiros: float
    coef_var_servicos_terceiros: str
    consumo_alugueis_imoveis: float
    coef_var_alugueis_imoveis: str
    consumo_seguros: float
    coef_var_seguros: str
    consumo_comunicacao: float
    coef_var_comunicacao: str
    consumo_energia_gas_agua: float
    coef_var_energia_gas_agua: str
    consumo_outros_custos: float
    coef_var_outros_custos: str
    valor_adicionado_bruto: float
    coef_var_valor_adicionado_bruto: str
    gastos_pessoal_total: float
    coef_var_gastos_pessoal_total: str
    gastos_salarios_remuneracoes: float
    coef_var_salarios_remuneracoes: str
    gastos_previdencia_social: float
    coef_var_previdencia_social: str
    gastos_fgts: float
    coef_var_fgts: str
    gastos_previdencia_privada: float
    coef_var_previdencia_privada: str
    gastos_indenizacoes_trabalhistas: float
    coef_var_indenizacoes_trabalhistas: str
    gastos_beneficios_empregados: float
    coef_var_beneficios_empregados: str
    pis_folha_pagamento: float
    coef_var_pis_folha_pagamento: str
    excedente_operacional_bruto: float
    coef_var_excedente_operacional_bruto: str
    pessoal_ocupado: float
    coef_var_pessoal_ocupado: str
    numero_empresas: float
    coef_var_numero_empresas: str


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

# Rota para buscar todos os dados de uma tabela
@app.get("/{name}/dados")
def get_table_data(name: str, db: Session = Depends(get_db_local)):
    tabela = get_table(name, metadata)  # Busca a tabela pelo nome
    
    query = select([tabela])  # Cria a query para selecionar todos os dados da tabela
    result = db.execute(query).fetchall()  # Executa a query e busca todos os resultados

    # Verifica se existem dados
    if not result:
        raise HTTPException(status_code=404, detail=f"Nenhum dado encontrado na tabela {name}")

    # Formata o resultado em uma lista de dicionários
    dados = [dict(r) for r in result]

    return {"dados": dados}

# Rota para inserir dados na tabela dinamicamente
@app.post("/{name}/")
def create(name: str, base: BaseCreate, db: Session = Depends(get_db_local)):
    tabela = get_table(name, metadata)
    query = tabela.insert().values(
        ano=base.ano,
        receita_liquida=base.receita_liquida,
        coef_var_receita_liquida=base.coef_var_receita_liquida,
        custo_mercadorias=base.custo_mercadorias,
        coef_var_custo_mercadorias=base.coef_var_custo_mercadorias,
        subvencoes_receitas_op=base.subvencoes_receitas_op,
        coef_var_subvencoes_receitas=base.coef_var_subvencoes_receitas,
        valor_bruto_producao=base.valor_bruto_producao,
        coef_var_valor_bruto_producao=base.coef_var_valor_bruto_producao,
        consumo_intermediario_total=base.consumo_intermediario_total,
        coef_var_consumo_intermediario_total=base.coef_var_consumo_intermediario_total,
        consumo_mercadorias_reposicao=base.consumo_mercadorias_reposicao,
        coef_var_consumo_mercadorias_reposicao=base.coef_var_consumo_mercadorias_reposicao,
        consumo_combustiveis=base.consumo_combustiveis,
        coef_var_combustiveis=base.coef_var_combustiveis,
        consumo_servicos_terceiros=base.consumo_servicos_terceiros,
        coef_var_servicos_terceiros=base.coef_var_servicos_terceiros,
        consumo_alugueis_imoveis=base.consumo_alugueis_imoveis,
        coef_var_alugueis_imoveis=base.coef_var_alugueis_imoveis,
        consumo_seguros=base.consumo_seguros,
        coef_var_seguros=base.coef_var_seguros,
        consumo_comunicacao=base.consumo_comunicacao,
        coef_var_comunicacao=base.coef_var_comunicacao,
        consumo_energia_gas_agua=base.consumo_energia_gas_agua,
        coef_var_energia_gas_agua=base.coef_var_energia_gas_agua,
        consumo_outros_custos=base.consumo_outros_custos,
        coef_var_outros_custos=base.coef_var_outros_custos,
        valor_adicionado_bruto=base.valor_adicionado_bruto,
        coef_var_valor_adicionado_bruto=base.coef_var_valor_adicionado_bruto,
        gastos_pessoal_total=base.gastos_pessoal_total,
        coef_var_gastos_pessoal_total=base.coef_var_gastos_pessoal_total,
        gastos_salarios_remuneracoes=base.gastos_salarios_remuneracoes,
        coef_var_salarios_remuneracoes=base.coef_var_salarios_remuneracoes,
        gastos_previdencia_social=base.gastos_previdencia_social,
        coef_var_previdencia_social=base.coef_var_previdencia_social,
        gastos_fgts=base.gastos_fgts,
        coef_var_fgts=base.coef_var_fgts,
        gastos_previdencia_privada=base.gastos_previdencia_privada,
        coef_var_previdencia_privada=base.coef_var_previdencia_privada,
        gastos_indenizacoes_trabalhistas=base.gastos_indenizacoes_trabalhistas,
        coef_var_indenizacoes_trabalhistas=base.coef_var_indenizacoes_trabalhistas,
        gastos_beneficios_empregados=base.gastos_beneficios_empregados,
        coef_var_beneficios_empregados=base.coef_var_beneficios_empregados,
        pis_folha_pagamento=base.pis_folha_pagamento,
        coef_var_pis_folha_pagamento=base.coef_var_pis_folha_pagamento,
        excedente_operacional_bruto=base.excedente_operacional_bruto,
        coef_var_excedente_operacional_bruto=base.coef_var_excedente_operacional_bruto,
        pessoal_ocupado=base.pessoal_ocupado,
        coef_var_pessoal_ocupado=base.coef_var_pessoal_ocupado,
        numero_empresas=base.numero_empresas,
        coef_var_numero_empresas=base.coef_var_numero_empresas
    )
    db.execute(query)
    db.commit()
    return {"message": f"Venda inserida na tabela {table_name}"}

# Rota para atualizar dados na tabela dinamicamente
@app.put("/{name}/{id}/receita_liquida")
def update(name: str, base_id: int, nova_receita_liquida: float, db: Session = Depends(get_db)):
    tabela = get_table(name, metadata)
    query = tabela.update().where(tabela.c.id == id).values(
        receita_liquida = nova_receita_liquida
    )
    db.execute(query)
    
    db.commit()
    return {"message": f"Linha {id} atualizada na tabela {name}"}

carregar_dfs('./datasets/')    

for key,value in dataframes.items():
    if key != "notas":
        dataframes_atualizados[key] = tratar_df(value)


for key,value in dataframes_atualizados.items():
    value.to_sql(f'{key}', engine, if_exists='replace', index=False)