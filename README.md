# Projeto de Banco de Dados: Migração e Análise de Dados de Educação Indígena

Este projeto consiste na migração de um modelo de dados relacional sobre educação para um banco de dados não relacional (MongoDB), otimizado para um cenário de alta escalabilidade e flexibilidade de esquema. O objetivo final é criar um ambiente robusto que permita a execução de consultas analíticas complexas sobre o tema da educação indígena no Brasil.

---

## 💻 Cenário do Projeto

O desenvolvimento foi guiado pelo **Cenário B**, que apresenta os seguintes requisitos:

> "Seu desafio é desenvolver um sistema para armazenamento de dados semi-estruturados que podem variar bastante em suas propriedades. O modelo de dados deve permitir a inclusão de novos campos sem exigir alterações no esquema ou migrações. O volume de acessos simultâneos é alto, especialmente por APIs que manipulam entidades completas (com todas as suas informações agregadas). Há uma exigência de escalabilidade horizontal e suporte a replicação e particionamento automático."

A escolha pelo **MongoDB** foi uma resposta direta a esses requisitos, graças à sua natureza orientada a documentos, esquema flexível e arquitetura projetada para escalabilidade.

---

## 🚀 Tecnologias Utilizadas

| Tecnologia | Propósito |
| :--- | :--- |
| **MongoDB** | Banco de dados NoSQL orientado a documentos, escolhido para atender aos requisitos de flexibilidade e escalabilidade. |
| **Docker & Docker Compose** | Criação de um ambiente de desenvolvimento isolado, reproduzível e fácil de gerenciar. |
| **Python** | Linguagem principal para o script de migração e para as consultas analíticas. |
| **Pandas** | Biblioteca Python utilizada para a leitura e transformação dos dados a partir dos arquivos de origem (`.csv`, `.xlsx`). |
| **PyMongo** | Driver oficial para conectar e interagir com o MongoDB a partir do Python. |

---

## 📂 Estrutura do Projeto

O projeto está organizado da seguinte forma:

```markdown
/MC536_ED_INDIGENA_MONGO/
├── docker-compose.yml        # Orquestra os contêineres do MongoDB e do script.
├── Dockerfile                # Define como construir a imagem do nosso script de migração.
├── requirements.txt          # Lista as dependências Python (pymongo, pandas, etc.).
│
├── migracao.py  # Script principal que lê os arquivos e popula o MongoDB.
├── consultas.py # Script para executar as 5 consultas analíticas no banco já populado.
│
├── /datasets/                # Pasta contendo os arquivos de dados necessários.
│   ├── microdados_ed_basica_2023.csv
│   ├── frequencia_escolar.xlsx
│   ├── media_anos.xlsx
│   ├── nivel_instrucao.xlsx
```


## ▶️ Como Executar o Projeto

Para executar a migração e ter o banco de dados populado, siga os passos abaixo.

### Pré-requisitos
- Ter o **Docker** e o **Docker Compose** instalados ([Docker Desktop](https://www.docker.com/products/docker-desktop/) é a forma mais fácil).
- Ter a pasta `datasets` com todos os arquivos de dados necessários na raiz do projeto.

### Passos para Execução

1.  **Criar o Arquivo de Ambiente (`.env`)**
    Na raiz do projeto, crie um arquivo chamado `.env`. 

2.  **Construir e Iniciar os Contêineres**
    Abra um terminal na pasta raiz do projeto e execute um único comando. Ele irá construir a imagem do script Python, iniciar o contêiner do MongoDB e, em seguida, iniciar o contêiner do migrador que executará o script de população automaticamente.

    ```bash
    docker compose up --build
    ```
    Aguarde o processo terminar. Você verá os logs de "Migração concluída com sucesso!".

3.  **Executar as Consultas Analíticas**
    Após a migração ser concluída, o contêiner do migrador irá parar, mas o contêiner do MongoDB (`mongodb_final`) continuará rodando. Para executar as 5 consultas analíticas no banco de dados populado, rode o seguinte comando no seu terminal local:
    ```bash
    python3 consultas.py
    ```

4.  **Parar o Ambiente**
    Quando terminar de usar o projeto, você pode parar e remover os contêineres e volumes com o comando:
    ```bash
    docker compose down -v
    ```

---

## 📊 Modelo de Dados do MongoDB

O modelo foi projetado usando uma abordagem híbrida de incorporação e referência para otimizar as consultas e garantir a escalabilidade.

### Coleção: `Municipios`
Armazena dados demográficos e indicadores educacionais consolidados para cada município.

**Estrutura do Documento:**
```json
{
  "_id": ObjectId,
  "nome_municipio": String,
  "uf_sigla": String,
  "regiao_nome": String,
  "populacao_total": NumberInt,
  "populacao_indigena": NumberInt,
  "indicadores_educacionais": {
    "frequencia_escolar": [
      {
        "faixa_etaria": String,
        "taxa": NumberDecimal
      }
    ],
    "anos_estudo": [
      {
        "faixa_etaria": String,
        "media_anos": NumberDecimal
      }
    ],
    "nivel_instrucao": [
      {
        "faixa_etaria": String,
        "nivel": String,
        "qt_pessoas": NumberInt
      }
    ]
  }
}
```

### Coleção: `Escolas`
Armazena informações detalhadas para cada instituição de ensino, fazendo referência ao seu município.

Estrutura do Documento:
```json
{
  "_id": ObjectId,
  "nome_escola": String,
  "municipio_id": ObjectId,
  "tipo_dependencia": String,
  "tipo_localizacao": String,
  "situacao_funcionamento": String,
  "indigena": Boolean,
  "turmas": [
    {
      "nivel_ensino": String,
      "qt_turmas": NumberInt
    }
  ],
  "matriculas": [
    {
      "nivel_ensino": String,
      "qt_matriculas_total": NumberInt,
      "qt_matriculas_indigenas": NumberInt,
      "ano_referencia": NumberInt
    }
  ]
}
```

### Coleção: `TerritoriosIndigenas`
Lista os territórios indígenas identificados nos dados.

Estrutura do Documento:

```json
{
  "_id": ObjectId,
  "nome_territorio": String,
  "uf_sigla": String,
  "regiao_nome": String
}
```

### ❓ Consultas Analíticas

O script `consultas.py` executa 6 consultas complexas para responder a perguntas relevantes sobre o tema do projeto, como:

#### Painel da Região Norte: 
Qual o ranking de UFs por número de alunos indígenas e qual a distribuição de escolas indígenas por tipo de administração?
#### Ranking de Municípios: 
Dentro de cada estado, quais municípios têm a maior proporção de alunos indígenas?
#### Municípios em Alerta: 
Quais municípios de alta população indígena também apresentam baixos indicadores de anos de estudo?
#### Infraestrutura vs. Demografia: 
Municípios com maior % de população indígena possuem, de fato, uma maior proporção de escolas indígenas?
#### Polos Educacionais: 
Quais municípios são os maiores "polos" de educação indígena, com base em um score que considera escolas, alunos e turmas?
#### Cobertura Educacional por Território: 
Qual é o ranking dos estados com melhor cobertura educacional indígena, considerando quantas escolas e alunos indígenas existem por território identificado?
