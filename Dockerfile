# Defini qual imagem vamos usar do Airflow e a verssão do Python que será instalada
FROM apache/airflow:2.9.3-python3.12

# Copia o arquivo requirements.txt para o container
COPY requirements.txt .

# Instala as bibliotecas do requirements.txt com pip packages
RUN pip install --no-cache-dir -r requirements.txt
