# Definindo versão do python (imagem base otimizada para tamanho)
FROM python:3.11-slim 

# Configura o diretório de trabalho dentro do container
WORKDIR /app

RUN apt-get update && apt-get install -y \
    libexpat1 \
    && rm -rf /var/lib/apt/lists/*

# Copia apenas o arquivo de dependências para otimizar camadas do Docker
COPY requirements.txt .

# Instala as dependências do Python (--no-cache-dir reduz o tamanho da imagem)
RUN pip install --no-cache-dir -r requirements.txt

# Copia todo o conteúdo do diretório atual para /app no container
COPY . .

# Define o comando padrão quando o container for executado
ENTRYPOINT [ "python" ] 