import psycopg2
from PNAD_Tipos_ESTADUAL import df_alfab, df_idade, df_sexo, df_raca
from conexão import conexao

def executar_sql():
    cur = conexao.cursor()
    
    cur.execute('SET search_path TO pnad, public')
    
    pnad_sexo = \
    '''
    CREATE TABLE IF NOT EXISTS pnad.sexo (
        id_pnad_sexo SERIAL PRIMARY KEY,
        id INTEGER,
        local VARCHAR(255),
        Categoria VARCHAR(255),
        Populacao DECIMAL(18, 2),
        unidade VARCHAR(50),
        ano DATE,
        Trimestre INT,
        AnoSedec DATE,
        Pessoas_14_anos_ou_mais_de_idade INT,
        Forca_de_Trabalho INT,
        Ocupadas INT,
        Desocupadas INT,
        Fora_da_Forca_de_Trabalho INT,
        Subocupadas_por_insuficiencia_de_horas_trabalhadas INT,
        Forca_de_trabalho_potencial INT);
    '''
    
    pnad_idade = \
    '''
    CREATE TABLE IF NOT EXISTS pnad.idade (
        id_pna_idade SERIAL PRIMARY KEY,
        id INTEGER,
        local VARCHAR(255),
        Categoria VARCHAR(255),
        unidade VARCHAR(50),
        ano DATE,
        Trimestre INT,
        AnoSedec DATE,
        Populacao_Geral DECIMAL(18, 2),
        Pessoas_14_anos_ou_mais_de_idade INT,
        Forca_de_Trabalho INT,
        Ocupadas INT,
        Desocupadas INT,
        Fora_da_Forca_de_Trabalho INT,
        Subocupadas_por_insuficiencia_de_horas_trabalhadas INT,
        Forca_de_trabalho_potencial INT);
    '''
    
    pnad_raca = \
    '''
    CREATE TABLE IF NOT EXISTS pnad.raca (
        id_pnad_raca SERIAL PRIMARY KEY,
        id INTEGER,
        local VARCHAR(255),
        Categoria VARCHAR(255),
        Populacao VARCHAR(50),
        unidade VARCHAR(50),
        ano DATE,
        Trimestre INT,
        AnoSedec DATE,
        Pessoas_14_anos_ou_mais_de_idade INT,
        Forca_de_Trabalho INT,
        Ocupadas INT,
        Desocupadas INT,
        Fora_da_Forca_de_Trabalho INT);
    '''
    pnad_alfab = \
    '''
    CREATE TABLE IF NOT EXISTS pnad.grau_de_instrucao (
        id_pnad_grau_de_instrucao SERIAL PRIMARY KEY,
        id INTEGER,
        local VARCHAR(255),
        Categoria VARCHAR(255),
        Populacao VARCHAR(50),
        unidade VARCHAR(50),
        ano DATE,
        Trimestre INT,
        AnoSedec DATE,
        Pessoas_14_anos_ou_mais_de_idade INT,
        Forca_de_Trabalho INT,
        Ocupadas INT,
        Desocupadas INT,
        Fora_da_Forca_de_Trabalho INT);
    '''


    cur.execute(pnad_sexo)
    cur.execute(pnad_idade)
    cur.execute(pnad_raca)
    cur.execute(pnad_alfab)

    verificando_existencia_pnad_sexo = '''
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema= 'pnad' AND table_type='BASE TABLE' AND table_name='sexo';
    '''
    verificando_existencia_pnad_idade = '''
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema= 'pnad' AND table_type='BASE TABLE' AND table_name='idade';
    '''
    verificando_existencia_pnad_raca = '''
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema= 'pnad' AND table_type='BASE TABLE' AND table_name='raca';
    '''
    verificando_existencia_pnad_grau_de_instrucao = '''
    SELECT 1
    FROM information_schema.tables
    WHERE table_schema= 'pnad' AND table_type='BASE TABLE' AND table_name='grau_de_instrucao';
    '''

    # Execute as consultas de verificação
    cur.execute(verificando_existencia_pnad_sexo)
    resultado_pnad_pop_sexo = cur.fetchone()
    
    cur.execute(verificando_existencia_pnad_idade)
    resultado_pnad_pop_idade = cur.fetchone()
    cur.execute(verificando_existencia_pnad_raca)
    resultado_pnad_raca = cur.fetchone()
    cur.execute(verificando_existencia_pnad_grau_de_instrucao)
    resultado_pnad_grau_de_instrucao = cur.fetchone()
    
    # Verifique se as tabelas existem e exclua, se necessário
    if resultado_pnad_pop_sexo[0] == 1:
        dropando_tabela_sexo = '''
        TRUNCATE TABLE pnad.sexo;
        '''
        cur.execute(dropando_tabela_sexo)
        
    if resultado_pnad_pop_idade [0] == 1:
        dropando_tabela_idade = '''
        TRUNCATE TABLE pnad.idade;
        '''
        cur.execute(dropando_tabela_idade)
        
    if resultado_pnad_raca[0] == 1:
        dropando_tabela_raca = '''
        TRUNCATE TABLE pnad.raca;
        '''
        cur.execute(dropando_tabela_raca)
        
    if resultado_pnad_grau_de_instrucao[0] == 1:
        dropando_tabela_grau_de_instrucao = '''
        TRUNCATE TABLE pnad.grau_de_instrucao;
        '''
        cur.execute(dropando_tabela_grau_de_instrucao)

    #INSERINDO DADOS
    inserindo_pnad_sexo = \
    '''
    INSERT INTO pnad.sexo (id,local, Categoria, Populacao, unidade, ano, Trimestre, AnoSedec,
    Pessoas_14_anos_ou_mais_de_idade, Forca_de_Trabalho, Ocupadas, Desocupadas,
    Fora_da_Forca_de_Trabalho, Subocupadas_por_insuficiencia_de_horas_trabalhadas,
    Forca_de_trabalho_potencial)
    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) 
    '''
    try:
        for idx, i in df_sexo.iterrows():
            dados = (
                i['id'],
                i['local'],
                i['Categoria'],
                i['População'],
                i['unidade'],
                i['ano'],
                i['Trimestre'],
                i['AnoSedec'],
                i['Pessoas de 14 anos ou mais de idade'],
                i['Força de Trabalho'],
                i['Ocupadas'],
                i['Desocupadas'],
                i['Fora da Força de Trabalho'],
                i['Subocupadas por insuficiência de horas trabalhadas'],
                i['Força de trabalho potencial']
            )
            cur.execute(inserindo_pnad_sexo, dados)
    except psycopg2.Error as e:
        print(f"Erro ao inserir dados sexo: {e}")
        
    inserindo_pnad_idade = \
    '''
    INSERT INTO pnad.idade (id, local, Categoria, unidade, ano, Trimestre, AnoSedec,
    Populacao_Geral, Pessoas_14_anos_ou_mais_de_idade, Forca_de_Trabalho,
    Ocupadas, Desocupadas, Fora_da_Forca_de_Trabalho,
    Subocupadas_por_insuficiencia_de_horas_trabalhadas, Forca_de_trabalho_potencial
)
    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) 
    '''
    try:
        for idx, i in df_idade.iterrows():
            dados = (
                i['id'],
                i['local'],
                i['Categoria'],
                i['unidade'],
                i['ano'],
                i['Trimestre'],
                i['AnoSedec'],
                i['População Geral'],
                i['Pessoas de 14 anos ou mais de idade'],
                i['Força de Trabalho'],
                i['Ocupadas'],
                i['Desocupadas'],
                i['Fora da Força de Trabalho'],
                i['Subocupadas por insuficiência de horas trabalhadas'],
                i['Força de trabalho potencial']
            )
            cur.execute(inserindo_pnad_idade , dados)
    except psycopg2.Error as e:
        print(f"Erro ao inserir dados idade : {e}")
        
    inserindo_pnad_raca = \
    '''
    INSERT INTO pnad.raca (id, local, Categoria, Populacao, unidade, ano, Trimestre, AnoSedec,
    Pessoas_14_anos_ou_mais_de_idade, Forca_de_Trabalho, Ocupadas, Desocupadas,
    Fora_da_Forca_de_Trabalho)
    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) 
    '''
    try:
        for idx, i in df_raca.iterrows():
            dados = (
                i['id'],
                i['local'],
                i['Categoria'],
                i['População'],
                i['unidade'],
                i['ano'],
                i['Trimestre'],
                i['AnoSedec'],
                i['Pessoas de 14 anos ou mais de idade'],
                i['Força de Trabalho'],
                i['Ocupadas'],
                i['Desocupadas'],
                i['Fora da Força de Trabalho']
            )
            cur.execute(inserindo_pnad_raca, dados)
    except psycopg2.Error as e:
        print(f"Erro ao inserir dados raca: {e}")
        
    inserindo_pnad_grau_de_instrucao = \
    '''
    INSERT INTO pnad.grau_de_instrucao (id, local, Categoria, Populacao, unidade, ano, Trimestre, AnoSedec,
    Pessoas_14_anos_ou_mais_de_idade, Forca_de_Trabalho, Ocupadas, Desocupadas,
    Fora_da_Forca_de_Trabalho)
    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) 
    '''
    try:
        for idx, i in df_alfab.iterrows():
            dados = (
                i['id'],
                i['local'],
                i['Categoria'],
                i['População'],
                i['unidade'],
                i['ano'],
                i['Trimestre'],
                i['AnoSedec'],
                i['Pessoas de 14 anos ou mais de idade'],
                i['Força de Trabalho'],
                i['Ocupadas'],
                i['Desocupadas'],
                i['Fora da Força de Trabalho']
            )
            cur.execute(inserindo_pnad_grau_de_instrucao, dados)
    except psycopg2.Error as e:
        print(f"Erro ao inserir grau de instrucao: {e}")
        
    conexao.commit()
    conexao.close()