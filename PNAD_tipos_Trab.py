import pandas as pd
import requests as rq 
import pprint
import sqlite3
from localidades import nacional
import ssl
from Google import Create_Service
from googleapiclient.http import MediaFileUpload
import openpyxl
from ajustar_planilha import ajustar_colunas, ajustar_bordas

tabela4093 = 4093
tabela6398 = 6388
tabela4094 = 4094
tabela6399 = 6399
tabela6403 = 6403
tabela6402 = 6402
tabela4095 = 4095

api_trab_sexo = f'https://servicodados.ibge.gov.br/api/v3/agregados/{tabela4093}/periodos/201201|201202|201203|201204|201301|201302|201303|201304|201401|201402|201403|201404|201501|201502|201503|201504|201601|201602|201603|201604|201701|201702|201703|201704|201801|201802|201803|201804|201901|201902|201903|201904|202001|202002|202003|202004|202101|202102|202103|202104|202201|202202|202203|202204|202301|202302|202303/variaveis/1641|4088|4090|4092|4094?{nacional}&classificacao=2[4,5]'
api_forca_sexo = f'https://servicodados.ibge.gov.br/api/v3/agregados/6398/periodos/201201%7C201202%7C201203%7C201204%7C201301%7C201302%7C201303%7C201304%7C201401%7C201402%7C201403%7C201404%7C201501%7C201502%7C201503%7C201504%7C201601%7C201602%7C201603%7C201604%7C201701%7C201702%7C201703%7C201704%7C201801%7C201802%7C201803%7C201804%7C201901%7C201902%7C201903%7C201904%7C202001%7C202002%7C202003%7C202004%7C202101%7C202102%7C202103%7C202104%7C202201%7C202202%7C202203%7C202204%7C202301%7C202302%7C202303%7C202304/variaveis/8344%7C8346?localidades=N1[all]&classificacao=2[4,5]'

api_anos_idade = f' https://servicodados.ibge.gov.br/api/v3/agregados/{tabela4094}/periodos/201201|201202|201203|201204|201301|201302|201303|201304|201401|201402|201403|201404|201501|201502|201503|201504|201601|201602|201603|201604|201701|201702|201703|201704|201801|201802|201803|201804|201901|201902|201903|201904|202001|202002|202003|202004|202101|202102|202103|202104|202201|202202|202203|202204|202301|202302|202303|202304/variaveis/1641|4088|4090|4092|4094?{nacional}&classificacao=58[114535,100052,108875,99127,3302]'
api_trab_idade = f'https://servicodados.ibge.gov.br/api/v3/agregados/6399/periodos/201201|201202|201203|201204|201301|201302|201303|201304|201401|201402|201403|201404|201501|201502|201503|201504|201601|201602|201603|201604|201701|201702|201703|201704|201801|201802|201803|201804|201901|201902|201903|201904|202001|202202|202203|202204|202301|202302|202303|202304/variaveis/8344|8346?localidades=N1[all]&classificacao=58[114535,100052,108875,99127,3302]'

api_trab_raca = f'https://servicodados.ibge.gov.br/api/v3/agregados/{tabela6402}/periodos/201201|201202|201203|201204|201301|201302|201303|201304|201401|201402|201403|201404|201501|201502|201503|201504|201601|201602|201603|201604|201701|201702|201703|201704|201801|201802|201803|201804|201901|201902|201903|201904|202001|202002|202003|202004|202101|202102|202103|202104|202201|202202|202203|202204|202301|202302|202303|202304/variaveis/4088|4090|4092|4094?{nacional}&classificacao=86[2776,2777,2779]'
api_trab_alfab = f'https://servicodados.ibge.gov.br/api/v3/agregados/{tabela4095}/periodos/201201|201202|201203|201204|201301|201302|201303|201304|201401|201402|201403|201404|201501|201502|201503|201504|201601|201602|201603|201604|201701|201702|201703|201704|201801|201802|201803|201804|201901|201902|201903|201904|202001|202002|202003|202004|202101|202102|202103|202104|202201|202202|202203|202204|202301|202302|202303|202304/variaveis/1641|4088|4090|4092|4094?{nacional}&classificacao=1568[120706,11779,11628,11629,11630,11631,11632,11626]'


class TLSAdapter(rq.adapters.HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        ctx = ssl.create_default_context()
        ctx.set_ciphers("DEFAULT@SECLEVEL=1")
        ctx.options |= 0x4   # OP_LEGACY_SERVER_CONNECT
        kwargs["ssl_context"] = ctx
        return super(TLSAdapter, self).init_poolmanager(*args, **kwargs)

def requisitando_dados(api):
    with rq.session() as s:
        s.mount("https://", TLSAdapter())
        dados_brutos_api = s.get(api, verify=True)

    # Verificação se a solicitação foi bem-sucedida antes de continuar
    if dados_brutos_api.status_code != 200:
        raise Exception(f"A solicitação à API falhou com o código de status: {dados_brutos_api.status_code}")

    # Verificação se a resposta pode ser convertida para JSON
    try:
        dados_brutos = dados_brutos_api.json()
    except Exception as e:
        raise Exception(f"Erro ao analisar a resposta JSON da API: {str(e)}")

    return dados_brutos

def extrair_dados(api, tabela_id):
    dados_brutos = requisitando_dados(api)
    if dados_brutos:
        if tabela_id == 4093 or tabela_id == 4094 or tabela_id == 4095:
            variavel1641 = dados_brutos[0]
            variavel4088 = dados_brutos[1]
            variavel4090 = dados_brutos[2]
            variavel4092 = dados_brutos[3]
            variavel4094 = dados_brutos[4]
            return variavel1641, variavel4088, variavel4090, variavel4092, variavel4094
        elif tabela_id == 6388 or tabela_id == 6399:
            variavel8344 = dados_brutos[0]
            variavel8346 = dados_brutos[1]
            return variavel8344, variavel8346
        elif tabela_id == 6402:
            variavel4088 = dados_brutos[0]
            variavel4090 = dados_brutos[1]
            variavel4092 = dados_brutos[2]
            variavel4094 = dados_brutos[3]
            return variavel4088, variavel4090, variavel4092, variavel4094
    else:
        pass
    
def tratando_dados_cinco(variavel1641, variavel4088, variavel4090, variavel4092, variavel4094):
    dados_limpos1641 = []
    dados_limpos4088 = []
    dados_limpos4090 = []
    dados_limpos4092  = []
    dados_limpos4094 = []
    
    variaveis = [variavel1641, variavel4088, variavel4090, variavel4092, variavel4094]
    for i in variaveis:
        id_tabela = i['id']
        variavel = i['variavel']
        unidade = i['unidade']
        dados = i['resultados']
        
        for ii in dados:
            dados_produto = ii['classificacoes']
            dados_producao = ii['series']

            for iii in dados_produto:
                dados_id_produto = iii['categoria']

                for id_produto, nome_produto in dados_id_produto.items():

                    for iv in dados_producao:
                        id = iv['localidade']['id']
                        local = iv['localidade']['nome']
                        dados_ano_producao = iv['serie']
                        

                        for ano, producao in dados_ano_producao.items():
                            producao = producao.replace('-', '0').replace('...', '0')
                            
                            partes = ano.split("/")
                            ano_sem_trimestre = int(partes[0][:4])
                            trimestre = int(partes[0][4:6])
                            
                            dict = {
                                'id': id,
                                'local': local,
                              #  'id_produto': id_produto,
                                'Categoria': nome_produto,
                                variavel: producao,
                                'unidade': unidade,
                                'ano': f'01/01/{ano_sem_trimestre}',
                                'Trimestre': trimestre
                            }

                            if id_tabela == '1641':
                                dados_limpos1641.append(dict)
                            elif id_tabela == '4088':
                                dados_limpos4088.append(dict)
                            elif id_tabela == '4090':
                                dados_limpos4090.append(dict)
                            elif id_tabela == '4092':
                                dados_limpos4092.append(dict)
                            elif id_tabela == '4094':
                                dados_limpos4094.append(dict)
                                
    return dados_limpos1641,  dados_limpos4088, dados_limpos4090, dados_limpos4092, dados_limpos4094

def tratando_dados_quatro(variavel4088, variavel4090, variavel4092, variavel4094):
    dados_limpos4088 = []
    dados_limpos4090 = []
    dados_limpos4092  = []
    dados_limpos4094 = []
    
    variaveis = [variavel4088, variavel4090, variavel4092, variavel4094]
    for i in variaveis:
        id_tabela = i['id']
        variavele = i['variavel']
        unidade = i['unidade']
        dados = i['resultados']
        
        for ii in dados:
            dados_produto = ii['classificacoes']
            dados_producao = ii['series']

            for iii in dados_produto:
                dados_id_produto = iii['categoria']

                for id_produto, nome_produto in dados_id_produto.items():

                    for iv in dados_producao:
                        id = iv['localidade']['id']
                        local = iv['localidade']['nome']
                        dados_ano_producao = iv['serie']
                        

                        for ano, producao in dados_ano_producao.items():
                            producao = producao.replace('-', '0').replace('...', '0')
                            
                            partes = ano.split("/")
                            ano_sem_trimestre = int(partes[0][:4])
                            trimestre = int(partes[0][4:6])
                            
                            dict = {
                                'id': id,
                                'local': local,
                                #'id_produto': id_produto,
                                'Categoria': nome_produto,
                                variavele: producao,
                                'unidade': unidade,
                                'ano': f'01/01/{ano_sem_trimestre}',
                                'Trimestre': trimestre
                            }
                            
                            if id_tabela == '4088':
                                dados_limpos4088.append(dict)
                            elif id_tabela == '4090':
                                dados_limpos4090.append(dict)
                            elif id_tabela == '4092':
                                dados_limpos4092.append(dict)
                            elif id_tabela == '4094':
                                dados_limpos4094.append(dict)
                                
    return dados_limpos4088, dados_limpos4090, dados_limpos4092, dados_limpos4094

def tratando_dados_dois(variavel8344, variave8346):
    dados_limpos8344  = []
    dados_limpos8346 = []
    
    variaveis = [variavel8344, variave8346]
    for i in variaveis:
        id_tabela = i['id']
        variavel = i['variavel']
        unidade = i['unidade']
        dados = i['resultados']
        
        for ii in dados:
            dados_produto = ii['classificacoes']
            dados_producao = ii['series']

            for iii in dados_produto:
                dados_id_produto = iii['categoria']

                for id_produto, nome_produto in dados_id_produto.items():

                    for iv in dados_producao:
                        id = iv['localidade']['id']
                        local = iv['localidade']['nome']
                        dados_ano_producao = iv['serie']
                        

                        for ano, producao in dados_ano_producao.items():
                            producao = producao.replace('-', '0').replace('...', '0')
                            
                            partes = ano.split("/")
                            ano_sem_trimestre = int(partes[0][:4])
                            trimestre = int(partes[0][4:6])
                            
                            dict = {
                                'id': id,
                                'local': local,
                                # 'id_produto': id_produto,
                                'Categoria': nome_produto,
                                variavel: producao,
                                'unidade': unidade,
                                'ano': f'01/01/{ano_sem_trimestre}',
                                'Trimestre': trimestre
                            }
                            
                            if id_tabela == '8344':
                                dados_limpos8344.append(dict)
                            elif id_tabela == '8346':
                                dados_limpos8346.append(dict)

                                
    return dados_limpos8344, dados_limpos8346

def executando_funcoes():
   
    variavel_trab_sexo_1641, variavel_trab_sexo_4088, variavel_trab_sexo_4090, variavel_trab_sexo_4092, variavel_trab_sexo_4094 = extrair_dados(api_trab_sexo, tabela4093)
    variavel_forca_sexo_8344, variavel_forca_sexo_8346 = extrair_dados(api_forca_sexo, tabela6398)
    
    variavel_anos_idade_1641, variavel_anos_idade_4088, variavel_anos_idade_4090, variavel_anos_idade_4092, variavel_anos_sexo_4094 = extrair_dados(api_anos_idade, tabela4094)
    variavel_trab_idade_8344,  variavel_trab_idade_8346 = extrair_dados(api_trab_idade, tabela6399)
    
    variavel_trab_raca_4088, variavel_trab_raca_4090, variavel_trab_raca_4092, variavel_idade_raca_4094 = extrair_dados(api_trab_raca, tabela6402)
    variavel_trab_alfab_1641, variavel_trab_alfab_4088, variavel_trab_alfab_4090, variavel_trab_alfab_4092, variavel_idade_alfab_4094  = extrair_dados(api_trab_alfab, tabela4095)
    
    dados_limpos1641_trab_sexo,  dados_limpos4088_trab_sexo, dados_limpos4090_trab_sexo, dados_limpos4092_trab_sexo, dados_limpos4094_trab_sexo = tratando_dados_cinco(variavel_trab_sexo_1641, variavel_trab_sexo_4088, variavel_trab_sexo_4090, variavel_trab_sexo_4092, variavel_trab_sexo_4094)
    dados_limpos8344_forca_sexo, dados_limpos8346_forca_sexo = tratando_dados_dois(variavel_forca_sexo_8344, variavel_forca_sexo_8346)
    
    dados_limpos1641_anos_idade,  dados_limpos4088_anos_idade, dados_limpos4090_anos_idade, dados_limpos4092_anos_idade, dados_limpos4094_anos_idade = tratando_dados_cinco(variavel_anos_idade_1641, variavel_anos_idade_4088, variavel_anos_idade_4090, variavel_anos_idade_4092, variavel_anos_sexo_4094)
    dados_limpos8344_forca_idade, dados_limpos8346_trab_idade = tratando_dados_dois(variavel_trab_idade_8344,  variavel_trab_idade_8346)
    
    dados_limpos4088_trab_raca, dados_limpos4090_trab_raca, dados_limpos4092_trab_raca, dados_limpos4094_trab_raca = tratando_dados_quatro(variavel_trab_raca_4088, variavel_trab_raca_4090, variavel_trab_raca_4092, variavel_idade_raca_4094)
    
    dados_limpos1641_trab_alfab,  dados_limpos4088_trab_alfab, dados_limpos4090_trab_alfab, dados_limpos4092_trab_alfab, dados_limpos4094_trab_alfab = tratando_dados_cinco(variavel_trab_alfab_1641, variavel_trab_alfab_4088, variavel_trab_alfab_4090, variavel_trab_alfab_4092, variavel_idade_alfab_4094)
    
    return  dados_limpos1641_trab_sexo,  dados_limpos4088_trab_sexo, dados_limpos4090_trab_sexo, dados_limpos4092_trab_sexo, dados_limpos4094_trab_sexo, \
        dados_limpos8344_forca_sexo, dados_limpos8346_forca_sexo, dados_limpos1641_anos_idade,  dados_limpos4088_anos_idade, dados_limpos4090_anos_idade, dados_limpos4092_anos_idade, dados_limpos4094_anos_idade, \
            dados_limpos8344_forca_idade, dados_limpos8346_trab_idade, dados_limpos4088_trab_raca, dados_limpos4090_trab_raca, dados_limpos4092_trab_raca, dados_limpos4094_trab_raca, dados_limpos1641_trab_alfab, \
                dados_limpos4088_trab_alfab, dados_limpos4090_trab_alfab, dados_limpos4092_trab_alfab, dados_limpos4094_trab_alfab
            
    
def gerando_dataframe_cinco(dados_limpos1641,  dados_limpos4088, dados_limpos4090, dados_limpos4092, dados_limpos4094):
    
    df1641 = pd.DataFrame(dados_limpos1641)
    df4088 = pd.DataFrame(dados_limpos4088)
    df4090 = pd.DataFrame(dados_limpos4090)
    df4092 = pd.DataFrame(dados_limpos4092)
    df4094 = pd.DataFrame(dados_limpos4094)
    
    df = pd.merge(df1641, df4088, on=['id', 'local', 'Categoria','unidade', 'ano', 'Trimestre'], how='inner')
    df = pd.merge(df, df4090, on=['id', 'local', 'Categoria', 'unidade', 'ano', 'Trimestre'], how='inner')
    df = pd.merge(df, df4092, on=['id', 'local', 'Categoria','unidade', 'ano', 'Trimestre'], how='inner')
    df = pd.merge(df, df4094, on=['id', 'local', 'Categoria', 'unidade', 'ano', 'Trimestre'], how='inner')
    df.columns = df.columns.str.strip()
    df.rename(columns={
        'Pessoas de 14 anos ou mais de idade, na força de trabalho, na semana de referência': 'Força de Trabalho',
        'Pessoas de 14 anos ou mais de idade ocupadas na semana de referência': 'Ocupadas',
        'Pessoas de 14 anos ou mais de idade, desocupadas na semana de referência': 'Desocupadas',
        'Pessoas de 14 anos ou mais de idade, fora da força de trabalho, na semana de referência': 'Fora da Força de Trabalho'
    }, inplace=True)
    return df

def gerando_dataframe_tres(dados_limpos8344, dados_limpos8346):
    df8344 = pd.DataFrame(dados_limpos8344)
    df8346 = pd.DataFrame(dados_limpos8346)
    
    df = pd.merge(df8344, df8346, on=['id', 'local', 'Categoria','unidade', 'ano', 'Trimestre'], how='inner')
    df.rename(columns={
        'Pessoas de 14 anos ou mais de idade, subocupadas por insuficiência de horas trabalhadas': 'Subocupadas por insuficiência de horas trabalhadas',
        'Pessoas de 14 anos ou mais de idade, na força de trabalho potencial':'Força de trabalho potencial'
        }, inplace=True)
    return df

def gerando_dataframe_quatro(dados_limpos4088, dados_limpos4090, dados_limpos4092, dados_limpos4094):
    df4088 = pd.DataFrame(dados_limpos4088)
    df4090 = pd.DataFrame(dados_limpos4090)
    df4092 = pd.DataFrame(dados_limpos4092)
    df4094 = pd.DataFrame(dados_limpos4094)
    df = pd.merge(df4088, df4090, on=['id', 'local', 'Categoria', 'unidade', 'ano', 'Trimestre'], how='inner')
    df = pd.merge(df, df4092, on=['id', 'local', 'Categoria','unidade', 'ano', 'Trimestre'], how='inner')
    df = pd.merge(df, df4094, on=['id', 'local', 'Categoria', 'unidade', 'ano', 'Trimestre'], how='inner')
    df.columns = df.columns.str.strip()
    df.rename(columns={
        'Pessoas de 14 anos ou mais de idade, na força de trabalho, na semana de referência': 'Força de Trabalho',
        'Pessoas de 14 anos ou mais de idade ocupadas na semana de referência': 'Ocupadas',
        'Pessoas de 14 anos ou mais de idade, desocupadas na semana de referência': 'Desocupadas',
        'Pessoas de 14 anos ou mais de idade, fora da força de trabalho, na semana de referência': 'Fora da Força de Trabalho'
    }, inplace=True)
    return df

pp = pprint.PrettyPrinter(indent=4)
dados_limpos1641_trab_sexo,  dados_limpos4088_trab_sexo, dados_limpos4090_trab_sexo, dados_limpos4092_trab_sexo, dados_limpos4094_trab_sexo, \
        dados_limpos8344_forca_sexo, dados_limpos8346_forca_sexo, dados_limpos1641_anos_idade,  dados_limpos4088_anos_idade, dados_limpos4090_anos_idade, dados_limpos4092_anos_idade, dados_limpos4094_anos_idade, \
            dados_limpos8344_forca_idade, dados_limpos8346_trab_idade, dados_limpos4088_trab_raca, dados_limpos4090_trab_raca, dados_limpos4092_trab_raca, dados_limpos4094_trab_raca, dados_limpos1641_trab_alfab, \
                dados_limpos4088_trab_alfab, dados_limpos4090_trab_alfab, dados_limpos4092_trab_alfab, dados_limpos4094_trab_alfab = executando_funcoes()

dftrab_sexo  = gerando_dataframe_cinco(dados_limpos1641_trab_sexo,  dados_limpos4088_trab_sexo, dados_limpos4090_trab_sexo, dados_limpos4092_trab_sexo, dados_limpos4094_trab_sexo)
dfforca_sexo = gerando_dataframe_tres(dados_limpos8344_forca_sexo, dados_limpos8346_forca_sexo)
dfanos_idade = gerando_dataframe_cinco( dados_limpos1641_anos_idade,  dados_limpos4088_anos_idade, dados_limpos4090_anos_idade, dados_limpos4092_anos_idade, dados_limpos4094_anos_idade)
dfforca_idade = gerando_dataframe_tres(dados_limpos8344_forca_idade, dados_limpos8346_trab_idade)
dftrab_raca = gerando_dataframe_quatro(dados_limpos4088_trab_raca, dados_limpos4090_trab_raca, dados_limpos4092_trab_raca, dados_limpos4094_trab_raca)
dftrab_alfab = gerando_dataframe_cinco(dados_limpos1641_trab_alfab, dados_limpos4088_trab_alfab, dados_limpos4090_trab_alfab, dados_limpos4092_trab_alfab, dados_limpos4094_trab_alfab)

print(dftrab_raca)

dftrab_sexo.to_excel("C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\DADOS\\PNAD\\Planilhas Tratadas\\trabalho SEXO NACIONAL.xlsx", index=False)
dfforca_sexo.to_excel('C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\DADOS\\PNAD\\Planilhas Tratadas\\', index=False)
dfanos_idade.to_excel('C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\DADOS\\PNAD\\Planilhas Tratadas\\Anos IDADE NACIONAL.xlsx', index=False)
dfforca_sexo.to_excel('C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\DADOS\\PNAD\\Planilhas Tratadas\\', index=False)
dftrab_raca.to_excel("C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\DADOS\\PNAD\\Planilhas Tratadas\\Trabalho RACA NACIONAL", index=False)
dftrab_alfab.to_excel("C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\DADOS\\PNAD\\Planilhas Tratadas\\Trabalha Grau de instrução NACIONAL.xlsx", index=False)