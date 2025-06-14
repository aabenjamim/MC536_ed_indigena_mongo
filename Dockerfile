# Usa uma imagem oficial e leve do Python como base
FROM python:3.9-slim

# Define o diretório de trabalho dentro do contêiner
WORKDIR /app

# Copia o arquivo de requisitos para o contêiner
COPY requirements.txt .

# Instala as bibliotecas Python listadas no requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copia todos os outros arquivos do projeto (o script .py e a pasta datasets) para o contêiner
COPY . .

# Comando que será executado quando o contêiner iniciar
CMD ["python", "migracao.py"]