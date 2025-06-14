import pandas as pd
from pymongo import MongoClient
import os
import unicodedata
from collections import defaultdict

# --- 1. CONFIGURAÇÃO E CONEXÃO ---
print("🚀 Iniciando migração direta dos arquivos para MongoDB...")
try:
    # Conecta usando o nome do serviço do docker-compose
    client = MongoClient('mongodb://mongo:27017/', serverSelectionTimeoutMS=5000)
    client.server_info()
    db = client['educacao_indigena']
    print("✅ Conexão com MongoDB estabelecida com sucesso.")
except Exception as e:
    print(f"❌ Erro de conexão com MongoDB: {e}")
    exit()

# --- 2. FUNÇÕES AUXILIARES E DE PROCESSAMENTO ---

def normalize_string(s):
    """Converte para maiúsculas, remove espaços e acentos."""
    if not isinstance(s, str):
        return ""
    s = s.strip().upper()
    return ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn')

def processar_indicadores():
    """Lê todos os arquivos XLSX e os processa em dicionários prontos para uso."""
    print("... Processando arquivos de indicadores (.xlsx)...")
    try:
        df_frequencia = pd.read_excel('./datasets/frequencia_escolar.xlsx', header=None, skiprows=5)
        df_anos_estudo = pd.read_excel('./datasets/media_anos.xlsx', header=None, skiprows=5)
        df_instrucao = pd.read_excel('./datasets/nivel_instrucao.xlsx', header=None, skiprows=5)
    except FileNotFoundError as e:
        print(f"❌ Erro: Arquivo de indicador não encontrado. Detalhes: {e}")
        return None, None, None

    uf_to_sigla = {
        'Rondônia': 'RO', 'Acre': 'AC', 'Amazonas': 'AM', 'Roraima': 'RR',
        'Pará': 'PA', 'Amapá': 'AP', 'Tocantins': 'TO', 'Maranhão': 'MA',
        'Piauí': 'PI', 'Ceará': 'CE', 'Rio Grande do Norte': 'RN', 'Paraíba': 'PB',
        'Pernambuco': 'PE', 'Alagoas': 'AL', 'Sergipe': 'SE', 'Bahia': 'BA',
        'Minas Gerais': 'MG', 'Espírito Santo': 'ES', 'Rio de Janeiro': 'RJ',
        'São Paulo': 'SP', 'Paraná': 'PR', 'Santa Catarina': 'SC',
        'Rio Grande do Sul': 'RS', 'Mato Grosso do Sul': 'MS', 'Mato Grosso': 'MT',
        'Goiás': 'GO', 'Distrito Federal': 'DF'
    }

    faixas_etarias_map = {
        '0 a 3 anos': 1, '4 a 5 anos': 2, '6 a 14 anos': 3,
        '15 a 17 anos': 4, '18 a 24 anos': 5, '25 anos ou mais': 6
    }

    # Processa Frequência e Anos de Estudo
    frequencia_por_uf, anos_estudo_por_uf = {}, {}
    for df, target_dict, value_key in [(df_frequencia, frequencia_por_uf, "taxa"), (df_anos_estudo, anos_estudo_por_uf, "media_anos")]:
        df.columns = ['UF'] + [f'col_{i}' for i in range(1, len(df.columns))]
        for _, row in df.iterrows():
            if row['UF'] == 'Brasil' or pd.isna(row['UF']):
                continue
            sigla = uf_to_sigla.get(row['UF'])
            if not sigla:
                continue
            dados_para_uf = []
            for faixa, idx in faixas_etarias_map.items():
                col_name = f'col_{idx}'
                if col_name not in row or pd.isna(row[col_name]):
                    continue
                try:
                    valor_str = str(row[col_name]).replace(',', '.').strip()
                    if valor_str in ['-', '', 'X', '..', '...']:
                        continue
                    dados_para_uf.append({"faixa_etaria": faixa, value_key: float(valor_str)})
                except (ValueError, TypeError):
                    continue
            target_dict[sigla] = dados_para_uf

    # ===== CORREÇÃO: Processa Nível de Instrução =====
    print("🔍 Debug: Processando nível de instrução...")
    print(f"🔍 Debug: Dataframe tem {len(df_instrucao)} linhas e {len(df_instrucao.columns)} colunas")

    instrucao_por_municipio = defaultdict(list)

    # Definições corretas baseadas no código PostgreSQL
    niveis_instrucao = [
        'Sem instrução e fundamental incompleto',
        'Fundamental completo e médio incompleto',
        'Médio completo e superior incompleto',
        'Superior completo'
    ]

    faixas_etarias_inst = [
        'Total', '18 a 24 anos', '18 a 19 anos', '20 a 24 anos',
        '25 anos ou mais', '25 a 64 anos', '25 a 29 anos', '30 a 34 anos',
        '35 a 39 anos', '40 a 44 anos', '45 a 49 anos', '50 a 54 anos',
        '55 a 59 anos', '60 a 64 anos', '65 anos ou mais', '65 a 69 anos',
        '70 a 74 anos', '75 a 79 anos', '80 anos ou mais'
    ]

    # Mapeamento para nomes abreviados
    nivel_abreviado = {
        'Sem instrução e fundamental incompleto': 'Sem instrução',
        'Fundamental completo e médio incompleto': 'Fundamental completo',
        'Médio completo e superior incompleto': 'Médio completo',
        'Superior completo': 'Superior completo'
    }

    linhas_processadas = 0
    registros_inseridos = 0

    # Processar cada linha do DataFrame
    for idx, row in df_instrucao.iterrows():
        # Verificar se a primeira célula contém um nome válido
        if pd.isna(row[0]) or not isinstance(row[0], str):
            continue

        nome_local = str(row[0]).strip()

        # Pular totais nacionais
        if nome_local == 'Brasil':
            continue

        # Normalizar nome do município
        nome_municipio_normalizado = normalize_string(nome_local)
        if not nome_municipio_normalizado:
            continue

        linhas_processadas += 1
        if linhas_processadas <= 5:  # Debug apenas dos primeiros 5
            print(f"🔍 Debug: Processando município {linhas_processadas}: {nome_local} -> {nome_municipio_normalizado}")

        # Para cada nível de instrução
        for nivel_idx, nivel_original in enumerate(niveis_instrucao):
            # Calcular deslocamento das colunas para este nível
            col_offset = 1 + (len(faixas_etarias_inst) * nivel_idx)

            # Para cada faixa etária
            for faixa_idx, faixa in enumerate(faixas_etarias_inst):
                col_num = col_offset + faixa_idx

                # Verificar se a coluna existe
                if col_num >= len(row):
                    continue

                valor = row[col_num]

                # Verificar se o valor é válido
                if pd.isna(valor):
                    continue

                try:
                    # Converter valor para string e limpar
                    valor_str = str(valor).strip()

                    # Pular valores inválidos
                    if valor_str in ['-', '', 'X', '..', '...']:
                        continue

                    # Converter para número
                    qt_pessoas = int(float(valor_str))

                    # Se chegou até aqui, é um valor válido
                    nivel_para_inserir = nivel_abreviado[nivel_original]

                    instrucao_por_municipio[nome_municipio_normalizado].append({
                        "faixa_etaria": faixa,
                        "nivel": nivel_para_inserir,
                        "qt_pessoas": qt_pessoas
                    })

                    registros_inseridos += 1

                except (ValueError, TypeError) as e:
                    if linhas_processadas <= 5:  # Debug apenas dos primeiros 5
                        print(f"⚠️  Erro ao processar valor '{valor}' para {nome_local}, nível {nivel_original}, faixa {faixa}: {e}")
                    continue

    print(f"✅ Nível de instrução processado: {linhas_processadas} municípios, {registros_inseridos} registros")
    print(f"🔍 Debug: Dicionário final tem {len(instrucao_por_municipio)} municípios com dados")

    # Debug: mostrar alguns municípios processados
    for i, (municipio, dados) in enumerate(instrucao_por_municipio.items()):
        if i < 3:  # Mostrar apenas os 3 primeiros
            print(f"🔍 Debug: {municipio} tem {len(dados)} registros de instrução")

    return frequencia_por_uf, anos_estudo_por_uf, instrucao_por_municipio

# --- 3. FUNÇÃO PRINCIPAL DE MIGRAÇÃO ---
def run_migration():
    """Lê os arquivos de origem, transforma os dados e os insere no MongoDB."""
    try:
        df_censo = pd.read_csv('./datasets/microdados_ed_basica_2023.csv', sep=';', low_memory=False, encoding='latin1')
        colunas_para_zerar = ['QT_MAT_BAS', 'QT_MAT_BAS_INDIGENA', 'QT_TUR_INF', 'QT_TUR_FUND', 'QT_TUR_MED', 'QT_TUR_EJA', 'NU_ANO_CENSO', 'IN_INF', 'IN_FUND_AI', 'IN_FUND_AF', 'IN_MED', 'IN_EJA']
        for col in colunas_para_zerar:
            if col in df_censo.columns:
                df_censo[col] = pd.to_numeric(df_censo[col], errors='coerce').fillna(0)
    except FileNotFoundError as e:
        print(f"❌ Erro fatal: Arquivo 'microdados_ed_basica_2023.csv' não encontrado. Detalhes: {e}")
        return

    # Processar indicadores
    frequencia_por_uf, anos_estudo_por_uf, instrucao_por_municipio = processar_indicadores()

    if instrucao_por_municipio is None:
        print("❌ Erro fatal: Não foi possível processar os indicadores.")
        return

    # Debug temporário - verificar correspondência de nomes
    print("\n🔍 TESTE: Primeiros 5 municípios do arquivo de instrução:")
    for i, municipio in enumerate(list(instrucao_por_municipio.keys())[:5]):
        print(f"  {i+1}. {municipio} -> {len(instrucao_por_municipio[municipio])} registros")

    # --- Migração de Municípios ---
    print("\n🏛️  Migrando Municípios...")
    db.Municipios.drop()
    municipios_agrupados = df_censo.groupby('CO_MUNICIPIO').agg(
        nome_municipio=('NO_MUNICIPIO', 'first'),
        uf_sigla=('SG_UF', 'first'),
        regiao_nome=('NO_REGIAO', 'first'),
        populacao_total=('QT_MAT_BAS', 'sum'),
        populacao_indigena=('QT_MAT_BAS_INDIGENA', 'sum')
    ).reset_index()

    print(f"\n🔍 TESTE: Primeiros 5 municípios do censo:")
    for i, municipio in enumerate(municipios_agrupados['nome_municipio'].head()):
        normalizado = normalize_string(municipio)
        tem_dados = "SIM" if normalizado in instrucao_por_municipio else "NÃO"
        print(f"  {i+1}. {municipio} -> {normalizado} -> Tem dados: {tem_dados}")

    municipios_docs, municipio_map = [], {}
    municipios_com_instrucao = 0

    for _, row in municipios_agrupados.iterrows():
        nome_normalizado = normalize_string(row['nome_municipio'])

        # Buscar dados de instrução para este município
        dados_instrucao = instrucao_por_municipio.get(nome_normalizado, [])

        if dados_instrucao:
            municipios_com_instrucao += 1
            if municipios_com_instrucao <= 5:  # Debug apenas dos primeiros 5
                print(f"✅ Município {row['nome_municipio']} ({nome_normalizado}) tem {len(dados_instrucao)} registros de instrução")

        doc = {
            "nome_municipio": row['nome_municipio'],
            "uf_sigla": row['uf_sigla'],
            "regiao_nome": row['regiao_nome'],
            "populacao_total": int(row['populacao_total']),
            "populacao_indigena": int(row['populacao_indigena']),
            "indicadores_educacionais": {
                "frequencia_escolar": frequencia_por_uf.get(row['uf_sigla'], []),
                "anos_estudo": anos_estudo_por_uf.get(row['uf_sigla'], []),
                "nivel_instrucao": dados_instrucao
            }
        }
        municipios_docs.append(doc)

    print(f"🔍 Debug: {municipios_com_instrucao} municípios têm dados de instrução de um total de {len(municipios_agrupados)}")

    if municipios_docs:
        result = db.Municipios.insert_many(municipios_docs)
        for i, row in municipios_agrupados.iterrows():
            municipio_map[int(row['CO_MUNICIPIO'])] = result.inserted_ids[i]

    print(f"✅ {len(municipio_map)} municípios migrados.")

    # --- Migração de Escolas ---
    print("\n🏫 Migrando Escolas...")
    db.Escolas.drop()
    escolas_df = df_censo.drop_duplicates(subset='CO_ENTIDADE')
    escolas_docs = []
    dep_map = {1: 'Federal', 2: 'Estadual', 3: 'Municipal', 4: 'Privada'}
    loc_map = {1: 'Urbana', 2: 'Rural'}
    sit_map = {1: 'Ativa', 2: 'Inativa', 3: 'Paralisada'}

    for _, row in escolas_df.iterrows():
        id_municipio_mongo = municipio_map.get(int(row['CO_MUNICIPIO']))
        if not id_municipio_mongo:
            continue

        turmas = [
            {"nivel_ensino": "Infantil", "qt_turmas": int(row['QT_TUR_INF'])},
            {"nivel_ensino": "Fundamental", "qt_turmas": int(row['QT_TUR_FUND'])},
            {"nivel_ensino": "Médio", "qt_turmas": int(row['QT_TUR_MED'])}
        ]
        turmas = [t for t in turmas if t['qt_turmas'] > 0]

        niveis_ofertados = [n for f, n in [(row['IN_INF'], "Infantil"), (row['IN_FUND_AI'] or row['IN_FUND_AF'], "Fundamental"), (row['IN_MED'], "Médio")] if f]

        matriculas = [{
            "ano_referencia": int(row['NU_ANO_CENSO']),
            "niveis_ofertados": niveis_ofertados,
            "qt_matriculas_total": int(row['QT_MAT_BAS']),
            "qt_matriculas_indigenas": int(row['QT_MAT_BAS_INDIGENA'])
        }] if niveis_ofertados else []

        doc = {
            "nome_escola": row['NO_ENTIDADE'],
            "municipio_id": id_municipio_mongo,
            "uf_sigla": row['SG_UF'],
            "regiao_nome": row['NO_REGIAO'],
            "tipo_dependencia": dep_map.get(row['TP_DEPENDENCIA'], 'NI'),
            "tipo_localizacao": loc_map.get(row['TP_LOCALIZACAO'], 'NI'),
            "situacao_funcionamento": sit_map.get(row['TP_SITUACAO_FUNCIONAMENTO'], 'NI'),
            "indigena": bool(row['IN_EDUCACAO_INDIGENA']),
            "turmas": turmas,
            "matriculas": matriculas
        }
        escolas_docs.append(doc)

    if escolas_docs:
        db.Escolas.insert_many(escolas_docs)
    print(f"✅ {len(escolas_docs)} escolas migradas.")

    # --- Migração de Territórios Indígenas ---
    print("\n🏞️  Migrando Territórios Indígenas...")
    db.TerritoriosIndigenas.drop()
    df_territorios = df_censo[df_censo['TP_LOCALIZACAO_DIFERENCIADA'] == 1].copy().drop_duplicates(subset=['NO_MUNICIPIO', 'SG_UF'])
    territorios_docs = [{
        "nome_territorio": f"Território Indígena em {row['NO_MUNICIPIO']}",
        "uf_sigla": row['SG_UF'],
        "regiao_nome": row['NO_REGIAO']
    } for _, row in df_territorios.iterrows()]

    if territorios_docs:
        db.TerritoriosIndigenas.insert_many(territorios_docs)
    print(f"✅ {len(territorios_docs)} territórios indígenas migrados.")

    # --- Verificação final ---
    print("\n🔍 Verificação final dos dados inseridos:")

    # Contar municípios com dados de instrução no MongoDB
    municipios_com_dados = db.Municipios.count_documents({
        "indicadores_educacionais.nivel_instrucao": {"$ne": []}
    })

    print(f"📊 Municípios no MongoDB com dados de instrução: {municipios_com_dados}")

    # Mostrar exemplo de município com dados
    exemplo = db.Municipios.find_one({
        "indicadores_educacionais.nivel_instrucao": {"$ne": []}
    })

    if exemplo:
        print(f"📋 Exemplo - {exemplo['nome_municipio']} tem {len(exemplo['indicadores_educacionais']['nivel_instrucao'])} registros de instrução")
    else:
        print("❌ Nenhum município encontrado com dados de instrução!")

# --- 4. EXECUÇÃO ---
if __name__ == "__main__":
    try:
        run_migration()
        print("\n\n🎉 Migração DIRETA DOS ARQUIVOS concluída com sucesso!")
    except Exception as e:
        print(f"\n❌ Ocorreu um erro geral durante a migração: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if 'client' in locals() and client:
            client.close()
            print("\n🔌 Conexão com MongoDB fechada.")