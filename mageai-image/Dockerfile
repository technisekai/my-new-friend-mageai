# Use the official Apache Airflow image as the base
FROM mageai/mageai:0.9.76 

# Upgrade pip and install Python dependencies
COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir "mage-ai[mysql, postgres]" -r /requirements.txt