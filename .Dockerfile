# Usar uma imagem do Airflow com Python, que é compatível com as necessidades do projeto
FROM apache/airflow:2.1.0

# Definir argumentos de build que podem ser configurados durante o build do Docker
ARG EMBULK_VERSION=0.9.23

# Instalar dependências necessárias para o projeto e o Embulk
USER root
RUN apt-get update && apt-get install -y \
    default-jre \
    && rm -rf /var/lib/apt/lists/*

# Instalar Embulk
RUN curl -L https://dl.embulk.org/embulk-${EMBULK_VERSION}.jar -o /bin/embulk \
    && chmod +x /bin/embulk

# Mudar de volta para o usuário airflow para evitar problemas de permissão
USER airflow

# Copiar as configurações do Embulk e outros scripts necessários para o container
COPY ./embulk_config /opt/airflow/embulk_config

# (Opcional) Copiar outros arquivos necessários para o projeto, como scripts Python, DAGs, etc.
COPY ./dags /opt/airflow/dags
COPY ./plugins /opt/airflow/plugins

# Definir o diretório de trabalho
WORKDIR /opt/airflow

# Define um comando padrão que mantém o contêiner em execução
CMD ["tail", "-f", "/dev/null"]