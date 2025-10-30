import requests
import pandas as pd
from sqlalchemy import create_engine

# Configuração MySQL
mysql_user = "root"
mysql_password = "31415"
mysql_host = "localhost"
mysql_port = 3306
mysql_db = "dados_ms"

engine = create_engine(f"mysql+mysqlconnector://{mysql_user}:{mysql_password}@{mysql_host}:{mysql_port}/{mysql_db}")

# URL da API CKAN do portal
api_url = "https://www.dados.ms.gov.br/api/3/action/package_show?id=compras"

r = requests.get(api_url)
r.raise_for_status()
data = r.json()

for resource in data["result"]["resources"]:
    if resource["format"].lower() == "csv":
        csv_url = resource["url"]
        filename = resource["name"].replace("/", "_")
        table_name = filename.replace(".csv", "").lower().replace(" ", "_").replace("-", "_")

        try:
            print(f"⬇️  Processando {filename}...")
            df = pd.read_csv(csv_url, encoding='utf-8', low_memory=False)

            # Normaliza nomes de colunas
            df.columns = [col.strip().replace(" ", "_").lower() for col in df.columns]

            # Insere no MySQL
            print(f"💾 Inserindo dados em {table_name}...")
            df.to_sql(name=table_name, con=engine, if_exists="replace", index=False)

        except Exception as e:
            print(f"⚠️ Erro ao processar {filename}: {e}")

print("✅ Downloads e importação para MySQL concluídos!")
