import psycopg2
from ETL_PNAD_ESTADUAL import dataframe1209, dataframe5918, dftrab_estadual
from ETL_PNAD_NACIONAL import dfpop
from conexão import conexao

def executar_sql():
    cur = conexao.cursor()
    
    cur.execute('SET search_path TO pnad, public')
    
    pnad_populacao_geral = \
    '''
    CREATE TABLE IF NOT EXISTS pnad.populacao_geral (
        id_pnad_municipal SERIAL PRIMARY KEY,
        id INTEGER,
        local TEXT,
        Categoria TEXT,
        Populacao INTEGER,
        Unidade TEXT,
        Data DATE);
    '''
    
    pnad_populacao_estadual = \
    '''
    CREATE TABLE IF NOT EXISTS pnad.populacao_estadual (
        id_populacao_estadual SERIAL PRIMARY KEY,
        id INTEGER,
        local TEXT,
        Categoria TEXT,
        Populacao INTEGER,
        Unidade TEXT,
        Data DATE);
    '''
    pnad_populacao_apta_trabalhar = \
    '''
    CREATE TABLE IF NOT EXISTS pnad.pessoas_aptas_trabalho (
        id_pessoas_aptas_trabalho SERIAL PRIMARY KEY,
        id INTEGER,
        local VARCHAR(40),
        unidade VARCHAR(20),
        ano DATE,
        Trimestre INTEGER,
        AnoSedec DATE,
        "0 a 13 anos" NUMERIC,
        "14 a 17 anos" NUMERIC,
        "18 a 24 anos" NUMERIC,
        "25 a 39 anos" NUMERIC,
        "40 a 59 anos" NUMERIC,
        "60 anos ou mais" NUMERIC,
        "Pessoas que podem trabalhar" NUMERIC);
    '''
    pnad_populacao_relaçao_forca = \
    '''
    CREATE TABLE IF NOT EXISTS pnad.relacao_forca_trabalho (
        id_relacao_forca_trabalhol SERIAL PRIMARY KEY,
        id INTEGER,
        local VARCHAR(40),
        unidade VARCHAR(25),
        ano DATE,
        Trimestre INTEGER,
        AnoSedec DATE,
        "Força de trabalho" INTEGER,
        Ocupado INTEGER,
        Desocupado INTEGER,
        "Fora da Força de trabalho" INTEGER,
        "Subocupado por insuficiência de horas trabalhadas" INTEGER,
        "Força de trabalho potencial" INTEGER,
        Desalentado INTEGER);
    '''


    cur.execute(pnad_populacao_geral)
    cur.execute(pnad_populacao_estadual)
    cur.execute(pnad_populacao_apta_trabalhar)
    cur.execute(pnad_populacao_relaçao_forca)

    verificando_existencia_pnad_populacao_geral = '''
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema= 'pnad' AND table_type='BASE TABLE' AND table_name='populacao_geral';
    '''
    verificando_existencia_pnad_populacao_estadual = '''
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema= 'pnad' AND table_type='BASE TABLE' AND table_name='populacao_estadual';
    '''
    verificando_existencia_pnad_pessoas_aptas_trabalho = '''
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema= 'pnad' AND table_type='BASE TABLE' AND table_name='pessoas_aptas_trabalho';
    '''
    verificando_existencia_pnad_relacao_forca_trabalho = '''
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema= 'pnad' AND table_type='BASE TABLE' AND table_name='relacao_forca_trabalho';
    '''

    # Execute as consultas de verificação
    cur.execute(verificando_existencia_pnad_populacao_geral)
    resultado_pnad_pop_geral = cur.fetchone()
    
    cur.execute(verificando_existencia_pnad_populacao_estadual)
    resultado_pnad_pop_estadual= cur.fetchone()
    cur.execute(verificando_existencia_pnad_pessoas_aptas_trabalho)
    resultado_pnad_pessoas_aptas_trabalho = cur.fetchone()
    cur.execute(verificando_existencia_pnad_relacao_forca_trabalho)
    resultado_pnad_relacao_forca_trabalho = cur.fetchone()
    
    # Verifique se as tabelas existem e exclua, se necessário
    if resultado_pnad_pop_geral[0] == 1:
        dropando_tabela_populacao_geral = '''
        TRUNCATE TABLE pnad.populacao_geral;
        '''
        cur.execute(dropando_tabela_populacao_geral)
        
    if resultado_pnad_pop_estadual[0] == 1:
        dropando_tabela_populacao_estadual = '''
        TRUNCATE TABLE pnad.populacao_estadual;
        '''
        cur.execute(dropando_tabela_populacao_estadual)
        
    if resultado_pnad_pessoas_aptas_trabalho[0] == 1:
        dropando_tabela_pessoas_aptas_trabalho = '''
        TRUNCATE TABLE pnad.pessoas_aptas_trabalho;
        '''
        cur.execute(dropando_tabela_pessoas_aptas_trabalho)
        
    if resultado_pnad_relacao_forca_trabalho[0] == 1:
        dropando_tabela_relacao_forca_trabalho = '''
        TRUNCATE TABLE pnad.relacao_forca_trabalho;
        '''
        cur.execute(dropando_tabela_relacao_forca_trabalho)

    #INSERINDO DADOS
    inserindo_pnad_geral = \
    '''
    INSERT INTO pnad.populacao_geral (id, local, categoria, populacao, unidade, data)
    VALUES(%s,%s,%s,%s,%s,%s) 
    '''
    try:
        for idx, i in dfpop.iterrows():
            dados = (
                i['id'],
                i['local'],
                i['Categoria'],
                i['População'],
                i['unidade'],
                i['ano']
            )
            cur.execute(inserindo_pnad_geral, dados)
    except psycopg2.Error as e:
        print(f"Erro ao inserir dados geral: {e}")
        
    inserindo_pnad_estadual= \
    '''
    INSERT INTO pnad.populacao_estadual (id, local, categoria, populacao, unidade, data)
    VALUES(%s,%s,%s,%s,%s,%s) 
    '''
    try:
        for idx, i in dataframe1209.iterrows():
            dados = (
                i['id'],
                i['local'],
                i['Categoria'],
                i['População'],
                i['unidade'],
                i['ano']
            )
            cur.execute(inserindo_pnad_estadual, dados)
    except psycopg2.Error as e:
        print(f"Erro ao inserir dados estadual: {e}")
        
    inserindo_pnad_pessoas_aptas_trabalho = \
    '''
    INSERT INTO pnad.pessoas_aptas_trabalho (id, local, unidade, ano, Trimestre, AnoSedec, "0 a 13 anos", "14 a 17 anos", "18 a 24 anos", "25 a 39 anos", "40 a 59 anos", "60 anos ou mais", "Pessoas que podem trabalhar")
    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) 
    '''
    try:
        for idx, i in dataframe5918.iterrows():
            dados = (
                i['id'],
                i['local'],
                i['unidade'],
                i['ano'],
                i['Trimestre'],
                i['AnoSedec'],
                i['0 a 13 anos'],
                i['14 a 17 anos'],
                i['18 a 24 anos'],
                i['25 a 39 anos'],
                i['40 a 59 anos'],
                i['60 anos ou mais'],
                i['Pessoas que podem trabalhar']
            )
            cur.execute(inserindo_pnad_pessoas_aptas_trabalho, dados)
    except psycopg2.Error as e:
        print(f"Erro ao inserir dados aptas ao trabalho: {e}")
        
    inserindo_pnad_relacao_forca_trabalho= \
    '''
    INSERT INTO pnad.relacao_forca_trabalho (id, local, unidade, ano, Trimestre, AnoSedec, "Força de trabalho", Ocupado, Desocupado, "Fora da Força de trabalho", "Subocupado por insuficiência de horas trabalhadas", 
    "Força de trabalho potencial", Desalentado)
    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) 
    '''
    try:
        for idx, i in dftrab_estadual.iterrows():
            dados = (
                i['id'],
                i['local'],
                i['unidade'],
                i['ano'],
                i['Trimestre'],
                i['AnoSedec'],
                i['Força de trabalho'],
                i['Ocupado'],
                i['Desocupado'],
                i['Fora da Força de trabalho'],
                i['Subocupado por insuficiência de horas trabalhadas'],
                i['Força de trabalho potencial'],
                i['Desalentado']
            )
            cur.execute(inserindo_pnad_relacao_forca_trabalho, dados)
    except psycopg2.Error as e:
        print(f"Erro ao inserir dados forca trabalho: {e}")
        
    conexao.commit()
    conexao.close()