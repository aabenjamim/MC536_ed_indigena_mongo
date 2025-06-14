from pymongo import MongoClient
from pprint import pprint

try:
    client = MongoClient('mongodb://localhost:27017/')
    db = client['educacao_indigena'] 
    db.command('ping')
    print("✅ Conexão com MongoDB estabelecida com sucesso.")
except Exception as e:
    print(f"❌ Falha na conexão com MongoDB. Erro: {e}")
    exit()

print("\n--- INICIANDO CONSULTAS TEMÁTICAS AVANÇADAS ---")

# --- Consulta 1: Painel da Educação Indígena na Região Norte (`$facet`) ---
print("\n\n[Consulta 1: Painel da Educação Indígena na Região Norte]")
pipeline1 = [
    { "$match": { "regiao_nome": "Norte" } },
    {
        "$facet": {
            "top_ufs_por_alunos_indigenas": [
                { "$unwind": "$matriculas" },
                { "$group": {
                    "_id": "$uf_sigla",
                    "total_alunos_indigenas": { "$sum": "$matriculas.qt_matriculas_indigenas" }
                }},
                { "$sort": { "total_alunos_indigenas": -1 } },
                { "$limit": 3 }
            ],
            "escolas_indigenas_por_dependencia": [
                { "$match": { "indigena": True } },
                { "$group": {
                    "_id": "$tipo_dependencia",
                    "quantidade": { "$sum": 1 }
                }},
                { "$sort": { "quantidade": -1 } }
            ]
        }
    }
]
pprint(list(db.Escolas.aggregate(pipeline1)))

# --- Consulta 2: Ranking de Municípios por Proporção de Alunos Indígenas (`$setWindowFields`) ---
print("\n\n[Consulta 2: Top 3 Municípios por UF com Maior Proporção de Alunos Indígenas]")
pipeline2 = [
    { "$unwind": "$matriculas" },
    { "$group": {
        "_id": "$municipio_id",
        "total_alunos": { "$sum": "$matriculas.qt_matriculas_total" },
        "total_alunos_indigenas": { "$sum": "$matriculas.qt_matriculas_indigenas" }
    }},
    { "$lookup": { "from": "Municipios", "localField": "_id", "foreignField": "_id", "as": "dados_municipio" } },
    { "$unwind": "$dados_municipio" },
    { "$addFields": {
        "proporcao_indigena": {
            "$cond": [{ "$eq": ["$total_alunos", 0] }, 0, { "$divide": ["$total_alunos_indigenas", "$total_alunos"] }]
        }
    }},
    { "$setWindowFields": {
        "partitionBy": "$dados_municipio.uf_sigla",
        "sortBy": { "proporcao_indigena": -1 },
        "output": { "ranking_no_estado": { "$rank": {} } }
    }},
    { "$match": { "ranking_no_estado": { "$lte": 3 } } },
    { "$sort": { "dados_municipio.uf_sigla": 1, "ranking_no_estado": 1 } },
    { "$project": { "_id": 0, "Município": "$dados_municipio.nome_municipio", "UF": "$dados_municipio.uf_sigla", "Proporção de Alunos Indígenas": "$proporcao_indigena", "Ranking no Estado": "$ranking_no_estado" }}
]
pprint(list(db.Escolas.aggregate(pipeline2)))

# --- Consulta 3: Municípios em Alerta: População Indígena Alta vs. Indicadores Críticos ---
print("\n\n[Consulta 3: Municípios com >5.000 Indígenas e Média de Estudo < 8 anos]")
pipeline3 = [
    {"$match": {"populacao_indigena": {"$gt": 5000},"indicadores_educacionais.anos_estudo": {"$elemMatch": {"faixa_etaria": "25 anos ou mais","media_anos": {"$lt": 8}}}}},
    {"$unwind": "$indicadores_educacionais.anos_estudo"},
    {"$match": {"indicadores_educacionais.anos_estudo.faixa_etaria": "25 anos ou mais"}},
    {"$project": {"_id": 0, "Município": "$nome_municipio", "UF": "$uf_sigla", "População Indígena": "$populacao_indigena", "Média Anos de Estudo (25+)": "$indicadores_educacionais.anos_estudo.media_anos"}},
    {"$sort": {"Média Anos de Estudo (25+)": 1}}
]
pprint(list(db.Municipios.aggregate(pipeline3)))

# --- Consulta 4 Otimizada: Correlação: % Pop. Indígena vs. Infraestrutura Escolar (`$bucketAuto`) ---
print("\n\n[Consulta 4 Otimizada: Proporção de Escolas Indígenas por Faixa de População Indígena do Município]")
pipeline4_otimizada = [
    # Etapa 1: Começa pelas escolas e agrupa por município para pré-calcular os totais
    {
        "$group": {
            "_id": "$municipio_id",
            "total_escolas": { "$sum": 1 },
            "escolas_indigenas": { "$sum": { "$cond": ["$indigena", 1, 0] } }
        }
    },
    # Etapa 2: Agora, com um conjunto de dados muito menor, busca as informações do município
    {
        "$lookup": {
            "from": "Municipios",
            "localField": "_id",
            "foreignField": "_id",
            "as": "info_municipio"
        }
    },
    { "$unwind": "$info_municipio" },
    # Etapa 3: Calcula as proporções necessárias
    {
        "$addFields": {
            "proporcao_pop_indigena": {
                "$cond": [{ "$eq": ["$info_municipio.populacao_total", 0] }, 0, { "$divide": ["$info_municipio.populacao_indigena", "$info_municipio.populacao_total"] }]
            },
            "proporcao_escolas_indigenas": {
                "$cond": [{ "$eq": ["$total_escolas", 0] }, 0, { "$divide": ["$escolas_indigenas", "$total_escolas"] }]
            }
        }
    },
    # Etapa 4: Agrupa em baldes (buckets) como antes
    {
        "$bucketAuto": {
            "groupBy": "$proporcao_pop_indigena",
            "buckets": 5,
            "output": {
                "total_municipios": { "$sum": 1 },
                "media_proporcao_escolas_indigenas": { "$avg": "$proporcao_escolas_indigenas" }
            }
        }
    },
    # Etapa 5: Formata a saída final
    {
        "$project": {
            "_id": 0,
            "faixa_proporcao_pop_indigena": {
                "min": { "$multiply": [ "$_id.min", 100 ] },
                "max": { "$multiply": [ "$_id.max", 100 ] }
            },
            "total_municipios_na_faixa": "$total_municipios",
            "media_de_escolas_indigenas_nesta_faixa (%)": { "$multiply": [ "$media_proporcao_escolas_indigenas", 100 ] }
        }
    }
]
pprint(list(db.Escolas.aggregate(pipeline4_otimizada)))

# --- Consulta 5: Score de "Polo Educacional Indígena" ---
print("\n\n[Consulta 5: Top 10 Municípios por Score de 'Polo Educacional Indígena']")
pipeline5 = [
    { "$match": { "indigena": True } }, # Começa apenas com escolas indígenas para otimizar
    { "$unwind": "$matriculas" },
    { "$group": {
        "_id": "$municipio_id",
        "total_escolas_indigenas": { "$sum": 1 },
        "total_alunos_indigenas": { "$sum": "$matriculas.qt_matriculas_indigenas" }
    }},
    { "$addFields": { "score": { "$add": [ { "$multiply": ["$total_escolas_indigenas", 10] }, { "$multiply": ["$total_alunos_indigenas", 0.5] } ] } } },
    { "$sort": { "score": -1 } },
    { "$limit": 10 },
    { "$lookup": { "from": "Municipios", "localField": "_id", "foreignField": "_id", "as": "dados_municipio" } },
    { "$unwind": "$dados_municipio" },
    { "$project": { "_id": 0, "Município": "$dados_municipio.nome_municipio", "UF": "$dados_municipio.uf_sigla", "Score": { "$round": ["$score", 2] }, "Escolas Indígenas": "$total_escolas_indigenas", "Alunos Indígenas": "$total_alunos_indigenas" }}
]
pprint(list(db.Escolas.aggregate(pipeline5)))

# --- Fim ---
print("\n--- CONSULTAS TEMÁTICAS AVANÇADAS CONCLUÍDAS ---")
client.close()