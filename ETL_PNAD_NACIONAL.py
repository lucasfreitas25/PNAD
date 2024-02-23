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

tabela1209 = 1209
tabela5918 = 5918
tabela6463 = 6463
tabela6482 = 6482

api_populacao = f'https://servicodados.ibge.gov.br/api/v3/agregados/{tabela1209}/periodos/2022/variaveis/606?{nacional}&classificacao=58[0]'

api_anos = f'https://servicodados.ibge.gov.br/api/v3/agregados/{tabela5918}/periodos/201201%7C201202%7C201203%7C201204%7C201301%7C201302%7C201303%7C201304%7C201401%7C201402%7C201403%7C201404%7C201501%7C201502%7C201503%7C201504%7C201601%7C201602%7C201603%7C201604%7C201701%7C201702%7C201703%7C201704%7C201801%7C201802%7C201803%7C201804%7C201901%7C201902%7C201903%7C201904%7C202001%7C202002%7C202003%7C202004%7C202101%7C202102%7C202103%7C202104%7C202201%7C202202%7C202203%7C202204%7C202301%7C202302%7C202303%7C202304/variaveis/606?{nacional}&classificacao=58[40288,114535,100052,108875,99127,3302]'

api_trab = f'https://servicodados.ibge.gov.br/api/v3/agregados/6463/periodos/201201|201202|201203|201204|201301|201302|201303|201304|201401|201402|201403|201404|201501|201502|201503|201504|201601|201602|201603|201604|201701|201702|201703|201704|201801|201802|201803|201804|201901|201902|201903|201904|202001|202002|202003|202004|202101|202102|202103|202104|202201|202202|202203|202204|202301|202302|202303|202304/variaveis/1641?localidades=N1[all]&classificacao=629[32386,32387,32446,32447]'
    
api_potencial = f'https://servicodados.ibge.gov.br/api/v3/agregados/6482/periodos/201201|201202|201203|201204|201301|201302|201303|201304|201401|201402|201403|201404|201501|201502|201503|201504|201601|201602|201603|201604|201701|201702|201703|201704|201801|201802|201803|201804|201901|201902|201903|201904|202001|202002|202003|202004|202101|202102|202103|202104|202201|202202|202203|202204|202301|202302|202303|202304/variaveis/1641?localidades=N1[all]&classificacao=604[31751,31752,46254]'

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
        if tabela_id == tabela1209:
            variavel606 = dados_brutos[0]
            return variavel606
        elif tabela_id == tabela5918:
            variavel606 = dados_brutos[0]
            return variavel606
        elif tabela_id == tabela6463:
            variavel1641 = dados_brutos[0]
            return variavel1641
        elif tabela_id == tabela6482:
            variavel1641 = dados_brutos[0]
            return variavel1641
    else:
        pass
    
def tratando_dados1209(variavel1209):
    dados_limpos_1209 = []

    
    id_tabela = variavel1209['id']
    variavel = variavel1209['variavel']
    unidade = variavel1209['unidade']
    dados = variavel1209['resultados']
    
    for ii in dados:
        dados_produto = ii['classificacoes']
        dados_producao = ii['series']

        for iii in dados_produto:
            dados_id_produto = iii['categoria']

            for id_produto, nome_produto in dados_id_produto.items():
                #nome_produto = 'Outros produtos ' + nome_produto if not nome_produto else nome_produto

                for iv in dados_producao:
                    id = iv['localidade']['id']
                    local = iv['localidade']['nome']
                    dados_ano_producao = iv['serie']
                    

                    for ano, producao in dados_ano_producao.items():
                        producao = producao.replace('-', '0').replace('...', '0')
                        
                        
                        dict = {
                            'id': id,
                            'local': local,
                            'id_produto': id_produto,
                            'Categoria': nome_produto,
                            variavel: producao,
                            'unidade': unidade,
                            'ano': f'01/01/{ano}'
                        }

                        dados_limpos_1209.append(dict)
    return dados_limpos_1209     
    
def tratando_dados(variavel):
    dados_limpos = []

    
    id_tabela = variavel['id']
    variavele = variavel['variavel']
    unidade = variavel['unidade']
    dados = variavel['resultados']
    
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
                            'id_produto': id_produto,
                            'Categoria': nome_produto,
                            variavele: producao,
                            'unidade': unidade,
                            'ano': f'01/01/{ano_sem_trimestre}',
                            'Trimestre': trimestre
                        }

                        dados_limpos.append(dict)
    return dados_limpos 


def executando_funcoes():
    variavel1209 = extrair_dados(api_populacao, tabela1209)
    variavel5918 = extrair_dados(api_anos, tabela5918)
    variavel6463 = extrair_dados(api_trab, tabela6463)
    varaivel6482 = extrair_dados(api_potencial, tabela6482)
    
    dados_limpos_1209 = tratando_dados1209(variavel1209)
    dados_limpos_5918 = tratando_dados(variavel5918)
    dados_limpos_6463 = tratando_dados(variavel6463)
    dados_limpos_6482 = tratando_dados(varaivel6482)
    
    return dados_limpos_1209, dados_limpos_5918, dados_limpos_6463, dados_limpos_6482
    
def gerando_dataframe(dados_limpos_1209, dados_limpos_5918, dados_limpos_6463, dados_limpos_6482):
    df1209 = pd.DataFrame(dados_limpos_1209)
    df5918 = pd.DataFrame(dados_limpos_5918)
    df6463 = pd.DataFrame(dados_limpos_6463)
    df6482 = pd.DataFrame(dados_limpos_6482)
    
    df1209['População'] = df1209['População'].astype(float)
    df5918['População'] = df5918['População'].astype(float)
    df6463['Pessoas de 14 anos ou mais de idade'] = df6463['Pessoas de 14 anos ou mais de idade'].astype(float)
    df6482['Pessoas de 14 anos ou mais de idade'] = df6463['Pessoas de 14 anos ou mais de idade'].astype(float)
    
    df5918['População'] = df5918['População'] * 1000
    df6463['Pessoas de 14 anos ou mais de idade'] = df6463['Pessoas de 14 anos ou mais de idade'] * 1000
    df6482['Pessoas de 14 anos ou mais de idade'] = df6463['Pessoas de 14 anos ou mais de idade'] * 1000
    
    return df1209, df5918, df6463, df6482

pp = pprint.PrettyPrinter(indent=4)
dados_limpos_1209, dados_limpos_5918, dados_limpos_6463, dados_limpos_6482 = executando_funcoes()
dataframe1209, dataframe5918, dataframe6463, dataframe6482 = gerando_dataframe(dados_limpos_1209, dados_limpos_5918, dados_limpos_6463, dados_limpos_6482)
print(dataframe6482)

dataframe1209.to_excel('C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\DADOS\\PNAD\\Planilhas\\População NACIONAL.xlsx', index=False)
dataframe5918.to_excel('C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\DADOS\\PNAD\\Planilhas\\Idade NACIONAL.xlsx', index=False)
dataframe6463.to_excel('C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\DADOS\\PNAD\\Planilhas\\Força de trabalho NACIONAL.xlsx', index=False)
dataframe6482.to_excel('C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\DADOS\\PNAD\\Planilhas\\Subutilização NACIONAL.xlsx', index=False)


planilha_principal = openpyxl.Workbook()

wb_1209 = openpyxl.load_workbook('C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\DADOS\\PNAD\\Planilhas\\População NACIONAL.xlsx')
wb_5918 = openpyxl.load_workbook('C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\DADOS\\PNAD\\Planilhas\\Idade NACIONAL.xlsx')
wb_6463 = openpyxl.load_workbook('C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\DADOS\\PNAD\\Planilhas\\Força de trabalho NACIONAL.xlsx')
wb_6482 = openpyxl.load_workbook('C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\DADOS\\PNAD\\Planilhas\\Subutilização NACIONAL.xlsx')

aba_1209 = planilha_principal.create_sheet("População Total")
aba_5918 = planilha_principal.create_sheet("Pessoas aptam a trabalhar")
aba_6463 = planilha_principal.create_sheet("Relação de força de trabalho")
aba_6482 = planilha_principal.create_sheet("Subutilização")


for linha in wb_1209.active.iter_rows(values_only=True):
    aba_1209.append(linha)

for linha in wb_5918.active.iter_rows(values_only=True):
    aba_5918.append(linha)
    
for linha in wb_6463.active.iter_rows(values_only=True):
    aba_6463.append(linha)
    
for linha in wb_6482.active.iter_rows(values_only=True):
    aba_6482.append(linha)
    
for aba in planilha_principal.sheetnames:
    if aba not in ["População Total", "Pessoas aptam a trabalhar", "Relação de força de trabalho", "Subutilização"]:
        del planilha_principal[aba]
        
ajustar_bordas(planilha_principal)

lista_aba = [aba_1209, aba_5918, aba_6463, aba_6482]
for abas in lista_aba:
    ajustar_colunas(abas)
    
planilha_principal.save("C:\\Users\\LucasFreitas\\Documents\\Lucas Freitas Arquivos\\DATAHUB\\DADOS\\PNAD\\PNAD NACIONAL.xlsx")   