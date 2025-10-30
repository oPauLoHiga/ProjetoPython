import requests
import os

# URL da API CKAN do portal
api_url = "https://www.dados.ms.gov.br/api/3/action/package_show?id=compras"

output_folder = "compras_ms_csvs"
os.makedirs(output_folder, exist_ok=True)

# Chamada à API
r = requests.get(api_url)
r.raise_for_status()
data = r.json()

# Percorre todos os recursos do pacote "compras"
for resource in data["result"]["resources"]:
    if resource["format"].lower() == "csv":
        csv_url = resource["url"]
        filename = resource["name"].replace("/", "_") + ".csv"
        path = os.path.join(output_folder, filename)
        print(f"⬇️  Baixando {filename}...")

        resp = requests.get(csv_url, stream=True)
        with open(path, "wb") as f:
            for chunk in resp.iter_content(8192):
                f.write(chunk)

print("✅ Downloads concluídos!")
