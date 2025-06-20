import requests
import pandas as pd
import re
import os
import json
import sqlalchemy 
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import unicodedata
from urllib.parse import quote_plus

# Configurações Gerais
namenode = "172.21.0.142"
port = 9870
user = "hdoop"
hdfs_path = "/bronze/agrin/suprema/fat"

# Diretório de saída local
output_dir = r"C:\Users\caval\Projetos\COLIN_WS\csv_corrigidos"
os.makedirs(output_dir, exist_ok=True)

# Conexão com PostgreSQL
password = quote_plus("@Winover2024")
engine = create_engine(
    f"postgresql+psycopg2://wesley.carnauba:{password}@172.21.0.119/warin",
    connect_args={'client_encoding': 'utf8'}
)

# Funções Auxiliares
def listar_arquivos(prefix):
    url = f"http://{namenode}:{port}/webhdfs/v1{hdfs_path}?op=LISTSTATUS&user.name={user}"
    r = requests.get(url)
    r.raise_for_status()
    files = r.json()["FileStatuses"]["FileStatus"]
    return [f["pathSuffix"] for f in files if f["pathSuffix"].startswith(prefix)]

def abrir_json(path_suffix):
    url = f"http://{namenode}:{port}/webhdfs/v1{hdfs_path}/{path_suffix}?op=OPEN&user.name={user}"
    r1 = requests.get(url, allow_redirects=False)
    redirect = r1.headers.get("Location")
    if not redirect:
        raise Exception(f"Sem redirecionamento para: {path_suffix}")
    redirect = redirect.replace("trisk05", namenode).replace("trisk06", namenode).replace("trisk10", namenode)
    r2 = requests.get(redirect)
    r2.raise_for_status()
    return r2.json()

def extrair_equipamento(link):
    match = re.search(r"/Equipment/([^/]+)/", link)
    return match.group(1) if match else None

def extrair_valor_speed(val):
    if isinstance(val, str):
        try:
            val = json.loads(val)
        except:
            return 0
    if not val:
        return 0
    if isinstance(val, list) and isinstance(val[0], dict) and "Speed" in val[0]:
        return val[0]["Speed"]
    return 0


# ### Parte 2 - Telemetry Peak Daily Speed

# In[21]:


prefix = "telemetry_peak_daily_speed_"
output_csv = os.path.join(output_dir, "telemetry_peak_daily_speed.csv")
tabela_banco = "telemetry_peak_daily_speed"

# Coleta os arquivos do HDFS
arquivos = listar_arquivos(prefix)
todos_dados = []

for arq in arquivos:
    try:
        dados = abrir_json(arq)
        for item in dados:
            links = item.get("Links", [])
            hrefs = [l["href"] for l in links if l["rel"] == "self"]
            if hrefs:
                equipamento = extrair_equipamento(hrefs[0])
                todos_dados.append({
                    "Arquivo": arq,
                    "Equipamento": equipamento,
                    "Speed": item.get("Speed", [])
                })
    except Exception as e:
        print(f"⚠️ Erro ao processar {arq}: {e}")

# Cria DataFrame e trata colunas
df = pd.DataFrame(todos_dados)
df["Data"] = df["Arquivo"].str.extract(r"_(\d{4}-\d{2}-\d{2})_")
df["Speed"] = df["Speed"].apply(extrair_valor_speed)
df = df[["Equipamento", "Speed", "Data"]]

# Salva CSV
df.to_csv(output_csv, index=False)
print(f"✅ CSV salvo em: {output_csv}")

# Insere no banco, recriando a tabela
df.to_sql(tabela_banco, engine, schema="Fazenda_WS", if_exists="replace", index=False)
print(f"✅ Tabela recriada no banco: Fazenda_WS.{tabela_banco}")


# ### Parte 3 - Telemetry Location

# In[22]:


import os
import pandas as pd
from sqlalchemy import text

prefix = "telemetry_location_"
output_csv = os.path.join(output_dir, "telemetry_location.csv")
output_excel = os.path.join(output_dir, "telemetry_location.xlsx")
tabela_banco = "telemetry_location"

arquivos = listar_arquivos(prefix)
todos_dados = []

for arq in arquivos:
    try:
        dados = abrir_json(arq)
        for item in dados:
            links = item.get("Links", [])
            hrefs = [l["href"] for l in links if l["rel"] == "self"]
            equipamento = extrair_equipamento(hrefs[0]) if hrefs else None
            localizacoes = item.get("Location", [])
            for loc in localizacoes:
                todos_dados.append({
                    "Equipamento": equipamento,
                    "DataHora": loc.get("@datetime"),
                    "Latitude": loc.get("Latitude"),
                    "Longitude": loc.get("Longitude"),
                    "Altitude": loc.get("Altitude"),
                    "UnidadeAltitude": loc.get("AltitudeUnits"),
                    "Arquivo": arq
                })
    except Exception as e:
        print(f"⚠️ Erro ao processar {arq}: {e}")

# Criação do DataFrame
df = pd.DataFrame(todos_dados)

# Extrai a data do nome do arquivo
df["Data"] = df["Arquivo"].str.extract(r"_(\d{4}-\d{2}-\d{2})_")

# Organiza colunas e remove "Arquivo"
df = df[["Equipamento", "DataHora", "Latitude", "Longitude", "Altitude", "UnidadeAltitude", "Data"]]

# Salva CSV
df.to_csv(output_csv, index=False)
print(f"✅ CSV salvo em: {output_csv}")

# ============== ÚNICA ALTERAÇÃO FEITA ==============
# Substituição do DROP/REPLACE por TRUNCATE/APPEND
with engine.begin() as conn:
    # 1. Limpa a tabela sem dropar
    conn.execute(text(f'DELETE FROM "Fazenda_WS"."{tabela_banco}"'))

    df.to_sql(
        tabela_banco,
        engine,
        schema="Fazenda_WS",
        if_exists="append",
        index=False,
        method="multi"
    )

print(f"✅ Dados atualizados no banco! Total: {len(df)} registros.")


# ### Parte 4 - Telemetry Fuel Used Last 24h

# In[23]:


prefix = "telemetry_fuel_used_last_"
output_csv = os.path.join(output_dir, "telemetry_fuel_used_last.csv")
output_excel = os.path.join(output_dir, "telemetry_fuel_used_last.xlsx")
tabela_banco = "telemetry_fuel_used_last"

arquivos = listar_arquivos(prefix)
todos_dados = []

for arq in arquivos:
    try:
        dados = abrir_json(arq)
        for item in dados:
            links = item.get("Links", [])
            hrefs = [l["href"] for l in links if l["rel"] == "self"]
            equipamento = extrair_equipamento(hrefs[0]) if hrefs else None

            registros = item.get("FuelUsedLast24", [])
            if not registros:
                todos_dados.append({
                    "Equipamento": equipamento,
                    "FuelUsed": 0,
                    "DataHora": None,
                    "Unidade": None,
                    "Arquivo": arq
                })
            else:
                for dado in registros:
                    todos_dados.append({
                        "Equipamento": equipamento,
                        "FuelUsed": dado.get("FuelUsed", 0),
                        "DataHora": dado.get("@datetime"),
                        "Unidade": dado.get("FuelUnits"),
                        "Arquivo": arq
                    })
    except Exception as e:
        print(f"⚠️ Erro ao processar {arq}: {e}")

# Criação do DataFrame
df = pd.DataFrame(todos_dados)

# Extrai a data do nome do arquivo
df["Data"] = df["Arquivo"].str.extract(r"_(\d{4}-\d{2}-\d{2})_")

# Organiza colunas e remove "Arquivo"
df = df[["Equipamento", "FuelUsed", "Unidade", "DataHora", "Data"]]

# Salva CSV
df.to_csv(output_csv, index=False)
print(f"✅ CSV salvo em: {output_csv}")

# Insere no banco
df.to_sql(tabela_banco, engine, schema="Fazenda_WS", if_exists="replace", index=False)
print(f"✅ Tabela recriada no banco: Fazenda_WS.{tabela_banco}")


# ### Parte 5 - Telemetry Fuel Remaining Ratio

# In[24]:


prefix = "telemetry_fuel_remaining_ratio_"
output_csv = os.path.join(output_dir, "telemetry_fuel_remaining_ratio.csv")
output_excel = os.path.join(output_dir, "telemetry_fuel_remaining_ratio.xlsx")
tabela_banco = "telemetry_fuel_remaining_ratio"

arquivos = listar_arquivos(prefix)
todos_dados = []

for arq in arquivos:
    try:
        dados = abrir_json(arq)
        for item in dados:
            links = item.get("Links", [])
            hrefs = [l["href"] for l in links if l["rel"] == "self"]
            equipamento = extrair_equipamento(hrefs[0]) if hrefs else None
            niveis = item.get("FuelRemaining", [])
            for registro in niveis:
                todos_dados.append({
                    "Equipamento": equipamento,
                    "DataHora": registro.get("@datetime"),
                    "Percentual": registro.get("percent"),
                    "Arquivo": arq
                })
    except Exception as e:
        print(f"⚠️ Erro ao processar {arq}: {e}")

# Criação do DataFrame
df = pd.DataFrame(todos_dados)

# Extrai a data do nome do arquivo
df["Data"] = df["Arquivo"].str.extract(r"_(\d{4}-\d{2}-\d{2})_")

# Organiza colunas
df = df[["Equipamento", "Percentual", "DataHora", "Data"]]

# Salva CSV
df.to_csv(output_csv, index=False)
print(f"✅ CSV salvo em: {output_csv}")

# Salva no banco
with engine.begin() as conn:
    conn.execute(text(f'DELETE FROM "Fazenda_WS"."{tabela_banco}"'))
df.to_sql(tabela_banco, engine, schema="Fazenda_WS", if_exists="append", index=False)
print(f"✅ Tabela atualizada no banco: Fazenda_WS.{tabela_banco}")


# ### Parte 6 - Telemetry Faults

# In[25]:


prefix = "telemetry_faults_"
output_csv = os.path.join(output_dir, "telemetry_faults.csv")
tabela_banco = "telemetry_faults"

arquivos = listar_arquivos(prefix)
todos_dados = []

for arq in arquivos:
    try:
        dados = abrir_json(arq)
        for item in dados:
            links = item.get("Links", [])
            hrefs = [l["href"] for l in links if l["rel"] == "self"]
            equipamento = extrair_equipamento(hrefs[0]) if hrefs else None

            codigos = item.get("FaultCode", [])
            if not codigos:
                todos_dados.append({
                    "Equipamento": equipamento,
                    "FaultCode": None,
                    "DataHora": None,
                    "Severidade": None,
                    "Descricao": None,
                    "Arquivo": arq
                })
            else:
                for f in codigos:
                    todos_dados.append({
                        "Equipamento": equipamento,
                        "FaultCode": f.get("CodeIdentifier"),
                        "DataHora": f.get("@datetime"),
                        "Severidade": f.get("CodeSeverity"),
                        "Descricao": f.get("CodeDescription"),
                        "Arquivo": arq
                    })
    except Exception as e:
        print(f"⚠️ Erro ao processar {arq}: {e}")

# Cria DataFrame
df = pd.DataFrame(todos_dados)

# Extrai a data do nome do arquivo
df["Data"] = df["Arquivo"].str.extract(r"_(\d{4}-\d{2}-\d{2})_")

# Organiza colunas
df = df[["Equipamento", "FaultCode", "Severidade", "Descricao", "DataHora", "Data"]]

# Salva CSV
df.to_csv(output_csv, index=False)
print(f"✅ CSV salvo em: {output_csv}")

# Atualiza o banco: DELETE + INSERT
from sqlalchemy import text  # Certifique-se de importar isso

with engine.begin() as conn:
    conn.execute(text(f'DELETE FROM "Fazenda_WS"."{tabela_banco}"'))
    print(f"🧹 Tabela {tabela_banco} limpa com DELETE")

df.to_sql(
    tabela_banco,
    engine,
    schema="Fazenda_WS",
    if_exists="append",
    index=False,
    method="multi"  # melhora a performance em inserts grandes
)

print(f"✅ Dados atualizados no banco: Fazenda_WS.{tabela_banco} (Total: {len(df)} registros)")


# ### Parte 7 - Telemetry Engine Condition

# In[26]:


prefix = "telemetry_engine_condition_"
output_csv = os.path.join(output_dir, "telemetry_engine_condition.csv")
output_excel = os.path.join(output_dir, "telemetry_engine_condition.xlsx")
tabela_banco = "telemetry_engine_condition"

arquivos = listar_arquivos(prefix)
todos_dados = []

for arq in arquivos:
    try:
        dados = abrir_json(arq)
        for item in dados:
            links = item.get("Links", [])
            hrefs = [l["href"] for l in links if l["rel"] == "self"]
            equipamento = extrair_equipamento(hrefs[0]) if hrefs else None

            status = item.get("EngineStatus", [])
            for e in status:
                todos_dados.append({
                    "Equipamento": equipamento,
                    "DataHora": e.get("@datetime"),
                    "EngineNumber": e.get("EngineNumber"),
                    "Running": e.get("Running"),
                    "Arquivo": arq
                })
    except Exception as e:
        print(f"⚠️ Erro ao processar {arq}: {e}")

# DataFrame
df = pd.DataFrame(todos_dados)

# Extrai a data do nome do arquivo
df["Data"] = df["Arquivo"].str.extract(r"_(\d{4}-\d{2}-\d{2})_")

# Organiza colunas
df = df[["Equipamento", "EngineNumber", "Running", "DataHora", "Data"]]

# Salva CSV
df.to_csv(output_csv, index=False)
print(f"✅ CSV salvo em: {output_csv}")

# Salva no banco
df.to_sql(tabela_banco, engine, schema="Fazenda_WS", if_exists="replace", index=False)
print(f"✅ Tabela recriada no banco: Fazenda_WS.{tabela_banco}")


# ### Parte 8 - Telemetry Distance

# In[27]:


prefix = "telemetry_distance_"
output_csv = os.path.join(output_dir, "telemetry_distance.csv")
output_excel = os.path.join(output_dir, "telemetry_distance.xlsx")
tabela_banco = "telemetry_distance"

arquivos = listar_arquivos(prefix)
todos_dados = []

for arq in arquivos:
    try:
        dados = abrir_json(arq)
        for item in dados:
            links = item.get("Links", [])
            hrefs = [l["href"] for l in links if l["rel"] == "self"]
            equipamento = extrair_equipamento(hrefs[0]) if hrefs else None

            distancias = item.get("Distance", [])
            if not distancias:
                todos_dados.append({
                    "Equipamento": equipamento,
                    "Distance": 0,
                    "Unidade": None,
                    "DataHora": None,
                    "Arquivo": arq
                })
            else:
                for d in distancias:
                    todos_dados.append({
                        "Equipamento": equipamento,
                        "Distance": d.get("Distance", 0),
                        "Unidade": d.get("DistanceUnits"),
                        "DataHora": d.get("@datetime"),
                        "Arquivo": arq
                    })
    except Exception as e:
        print(f"⚠️ Erro ao processar {arq}: {e}")

# DataFrame
df = pd.DataFrame(todos_dados)

# Extrai a data do nome do arquivo
df["Data"] = df["Arquivo"].str.extract(r"_(\d{4}-\d{2}-\d{2})_")

# Organiza colunas
df = df[["Equipamento", "Distance", "Unidade", "DataHora", "Data"]]

# Salva CSV
df.to_csv(output_csv, index=False)
print(f"✅ CSV salvo em: {output_csv}")

# Salva no banco
df.to_sql(tabela_banco, engine, schema="Fazenda_WS", if_exists="replace", index=False)
print(f"✅ Tabela recriada no banco: Fazenda_WS.{tabela_banco}")


# ### Parte 9 - Telemetry DEF Remaining

# In[28]:


prefix = "telemetry_def_remaining_"
output_csv = os.path.join(output_dir, "telemetry_def_remaining.csv")
output_excel = os.path.join(output_dir, "telemetry_def_remaining.xlsx")
tabela_banco = "telemetry_def_remaining"

arquivos = listar_arquivos(prefix)
todos_dados = []

for arq in arquivos:
    try:
        dados = abrir_json(arq)
        for item in dados:
            links = item.get("Links", [])
            hrefs = [l["href"] for l in links if l["rel"] == "self"]
            equipamento = extrair_equipamento(hrefs[0]) if hrefs else None

            def_data = item.get("DEFRemaining", [])
            if not def_data:
                todos_dados.append({
                    "Equipamento": equipamento,
                    "DEFRemaining": 0,
                    "Unidade": None,
                    "DataHora": None,
                    "Arquivo": arq
                })
            else:
                for d in def_data:
                    todos_dados.append({
                        "Equipamento": equipamento,
                        "DEFRemaining": d.get("DEFRemaining", 0),
                        "Unidade": d.get("DEFUnits"),
                        "DataHora": d.get("@datetime"),
                        "Arquivo": arq
                    })
    except Exception as e:
        print(f"⚠️ Erro ao processar {arq}: {e}")

# Criação do DataFrame
df = pd.DataFrame(todos_dados)

# Extrai a data do nome do arquivo
df["Data"] = df["Arquivo"].str.extract(r"_(\d{4}-\d{2}-\d{2})_")

# Organiza colunas
df = df[["Equipamento", "DEFRemaining", "Unidade", "DataHora", "Data"]]

# Salva CSV
df.to_csv(output_csv, index=False)
print(f"✅ CSV salvo em: {output_csv}")

# Salva no banco
df.to_sql(tabela_banco, engine, schema="Fazenda_WS", if_exists="replace", index=False)
print(f"✅ Tabela recriada no banco: Fazenda_WS.{tabela_banco}")


# ### Parte 10 - Telemetry Cumulative Operating Hours

# In[29]:


prefix = "telemetry_cumul_op_hours_"
output_csv = os.path.join(output_dir, "telemetry_cumul_op_hours.csv")
output_excel = os.path.join(output_dir, "telemetry_cumul_op_hours.xlsx")
tabela_banco = "telemetry_cumul_op_hours"

arquivos = listar_arquivos(prefix)
todos_dados = []

for arq in arquivos:
    try:
        dados = abrir_json(arq)
        for item in dados:
            links = item.get("Links", [])
            hrefs = [l["href"] for l in links if l["rel"] == "self"]
            equipamento = extrair_equipamento(hrefs[0]) if hrefs else None

            horas = item.get("CumulativeOperatingHours", [])
            if not horas:
                todos_dados.append({
                    "Equipamento": equipamento,
                    "Hora": 0,
                    "DataHora": None,
                    "Arquivo": arq
                })
            else:
                for h in horas:
                    todos_dados.append({
                        "Equipamento": equipamento,
                        "Hora": h.get("Hour", 0),
                        "DataHora": h.get("@datetime"),
                        "Arquivo": arq
                    })
    except Exception as e:
        print(f"⚠️ Erro ao processar {arq}: {e}")

# Criação do DataFrame
df = pd.DataFrame(todos_dados)

# Extrai a data do nome do arquivo
df["Data"] = df["Arquivo"].str.extract(r"_(\d{4}-\d{2}-\d{2})_")

# Organiza colunas
df = df[["Equipamento", "Hora", "DataHora", "Data"]]

# Salva CSV
df.to_csv(output_csv, index=False)
print(f"✅ CSV salvo em: {output_csv}")


# Salva no banco
df.to_sql(tabela_banco, engine, schema="Fazenda_WS", if_exists="replace", index=False)
print(f"✅ Tabela recriada no banco: Fazenda_WS.{tabela_banco}")


# ### Parte 11 - Telemetry Cumulative Non-Productive Idle Hours

# In[30]:


prefix = "telemetry_cumul_non_prod_idle_hours_"
output_csv = os.path.join(output_dir, "telemetry_cumul_non_prod_idle_hours.csv")
output_excel = os.path.join(output_dir, "telemetry_cumul_non_prod_idle_hours.xlsx")
tabela_banco = "telemetry_cumul_non_prod_idle_hours"

arquivos = listar_arquivos(prefix)
todos_dados = []

for arq in arquivos:
    try:
        dados = abrir_json(arq)
        for item in dados:
            links = item.get("Links", [])
            hrefs = [l["href"] for l in links if l["rel"] == "self"]
            equipamento = extrair_equipamento(hrefs[0]) if hrefs else None

            horas = item.get("CumulativeNonProductiveIdleHours", [])
            if not horas:
                todos_dados.append({
                    "Equipamento": equipamento,
                    "Hora": 0,
                    "DataHora": None,
                    "Arquivo": arq
                })
            else:
                for h in horas:
                    todos_dados.append({
                        "Equipamento": equipamento,
                        "Hora": h.get("Hour", 0),
                        "DataHora": h.get("@datetime"),
                        "Arquivo": arq
                    })
    except Exception as e:
        print(f"⚠️ Erro ao processar {arq}: {e}")

# DataFrame
df = pd.DataFrame(todos_dados)

# Extrai a data do nome do arquivo
df["Data"] = df["Arquivo"].str.extract(r"_(\d{4}-\d{2}-\d{2})_")

# Organiza colunas
df = df[["Equipamento", "Hora", "DataHora", "Data"]]

# Salva CSV
df.to_csv(output_csv, index=False)
print(f"✅ CSV salvo em: {output_csv}")

# Salva no banco
df.to_sql(tabela_banco, engine, schema="Fazenda_WS", if_exists="replace", index=False)
print(f"✅ Tabela recriada no banco: Fazenda_WS.{tabela_banco}")


# ### Parte 12 - Telemetry Cumulative Idle Hours

# In[31]:


prefix = "telemetry_cumul_idle_hours_"
output_csv = os.path.join(output_dir, "telemetry_cumul_idle_hours.csv")
output_excel = os.path.join(output_dir, "telemetry_cumul_idle_hours.xlsx")
tabela_banco = "telemetry_cumul_idle_hours"

arquivos = listar_arquivos(prefix)
todos_dados = []

for arq in arquivos:
    try:
        dados = abrir_json(arq)
        for item in dados:
            links = item.get("Links", [])
            hrefs = [l["href"] for l in links if l["rel"] == "self"]
            equipamento = extrair_equipamento(hrefs[0]) if hrefs else None

            horas = item.get("CumulativeIdleHours", [])
            if not horas:
                todos_dados.append({
                    "Equipamento": equipamento,
                    "Hora": 0,
                    "DataHora": None,
                    "Arquivo": arq
                })
            else:
                for h in horas:
                    todos_dados.append({
                        "Equipamento": equipamento,
                        "Hora": h.get("Hour", 0),
                        "DataHora": h.get("@datetime"),
                        "Arquivo": arq
                    })
    except Exception as e:
        print(f"⚠️ Erro ao processar {arq}: {e}")

# Criação do DataFrame
df = pd.DataFrame(todos_dados)

# Extrai a data do nome do arquivo
df["Data"] = df["Arquivo"].str.extract(r"_(\d{4}-\d{2}-\d{2})_")

# Organiza colunas
df = df[["Equipamento", "Hora", "DataHora", "Data"]]

# Salva CSV
df.to_csv(output_csv, index=False)
print(f"✅ CSV salvo em: {output_csv}")

# Salva no banco
df.to_sql(tabela_banco, engine, schema="Fazenda_WS", if_exists="replace", index=False)
print(f"✅ Tabela recriada no banco: Fazenda_WS.{tabela_banco}")


# ### Parte 13 - Telemetry Cumulative Fuel Used

# In[32]:


prefix = "telemetry_cumul_fuel_used_"
output_csv = os.path.join(output_dir, "telemetry_cumul_fuel_used.csv")
output_excel = os.path.join(output_dir, "telemetry_cumul_fuel_used.xlsx")
tabela_banco = "telemetry_cumul_fuel_used"

arquivos = listar_arquivos(prefix)
todos_dados = []

for arq in arquivos:
    try:
        dados = abrir_json(arq)
        for item in dados:
            links = item.get("Links", [])
            hrefs = [l["href"] for l in links if l["rel"] == "self"]
            equipamento = extrair_equipamento(hrefs[0]) if hrefs else None

            combustivel = item.get("FuelUsed", [])
            if not combustivel:
                todos_dados.append({
                    "Equipamento": equipamento,
                    "FuelConsumed": 0,
                    "Unidade": None,
                    "DataHora": None,
                    "Arquivo": arq
                })
            else:
                for f in combustivel:
                    todos_dados.append({
                        "Equipamento": equipamento,
                        "FuelConsumed": f.get("FuelConsumed", 0),
                        "Unidade": f.get("FuelUnits"),
                        "DataHora": f.get("@datetime"),
                        "Arquivo": arq
                    })
    except Exception as e:
        print(f"⚠️ Erro ao processar {arq}: {e}")

# Criação do DataFrame
df = pd.DataFrame(todos_dados)

# Extrai a data do nome do arquivo
df["Data"] = df["Arquivo"].str.extract(r"_(\d{4}-\d{2}-\d{2})_")

# Organiza colunas
df = df[["Equipamento", "FuelConsumed", "Unidade", "DataHora", "Data"]]

# Salva CSV
df.to_csv(output_csv, index=False)
print(f"✅ CSV salvo em: {output_csv}")

# Salva no banco
df.to_sql(tabela_banco, engine, schema="Fazenda_WS", if_exists="replace", index=False)
print(f"✅ Tabela recriada no banco: Fazenda_WS.{tabela_banco}")


# ### Parte 14 - Telemetry Caution Codes

# In[33]:


prefix = "telemetry_caution_codes_"
output_csv = os.path.join(output_dir, "telemetry_caution_codes.csv")
output_excel = os.path.join(output_dir, "telemetry_caution_codes.xlsx")
tabela_banco = "telemetry_caution_codes"

arquivos = listar_arquivos(prefix)
todos_dados = []

for arq in arquivos:
    try:
        dados = abrir_json(arq)
        for item in dados:
            links = item.get("Links", [])
            hrefs = [l["href"] for l in links if l["rel"] == "self"]
            equipamento = extrair_equipamento(hrefs[0]) if hrefs else None

            alertas = item.get("CautionDescription", [])
            if not alertas:
                todos_dados.append({
                    "Equipamento": equipamento,
                    "DataHora": None,
                    "Identifier": None,
                    "Description": None,
                    "Severity": None,
                    "Active": None,
                    "Arquivo": arq
                })
            else:
                for alerta in alertas:
                    todos_dados.append({
                        "Equipamento": equipamento,
                        "DataHora": alerta.get("@datetime"),
                        "Identifier": alerta.get("Identifier"),
                        "Description": alerta.get("Description"),
                        "Severity": alerta.get("Severity"),
                        "Active": alerta.get("Active"),
                        "Arquivo": arq
                    })
    except Exception as e:
        print(f"⚠️ Erro ao processar {arq}: {e}")

# Criação do DataFrame
df = pd.DataFrame(todos_dados)

# Extrai a data do nome do arquivo
df["Data"] = df["Arquivo"].str.extract(r"_(\d{4}-\d{2}-\d{2})_")

# Organiza colunas
df = df[["Equipamento", "Identifier", "Description", "Severity", "Active", "DataHora", "Data"]]

# Salva CSV
df.to_csv(output_csv, index=False)
print(f"✅ CSV salvo em: {output_csv}")

# Atualiza o banco: DELETE + INSERT
from sqlalchemy import text  # Import necessário

with engine.begin() as conn:
    conn.execute(text(f'DELETE FROM "Fazenda_WS"."{tabela_banco}"'))
    print(f"🧹 Tabela {tabela_banco} limpa com DELETE")

df.to_sql(
    tabela_banco,
    engine,
    schema="Fazenda_WS",
    if_exists="append",
    index=False,
    method="multi"  # performance otimizada
)

print(f"✅ Dados atualizados no banco: Fazenda_WS.{tabela_banco} (Total: {len(df)} registros)")


# ### Parte 15 - Telemetry Average Load

# In[34]:


prefix = "telemetry_average_load_"
output_csv = os.path.join(output_dir, "telemetry_average_load.csv")
output_excel = os.path.join(output_dir, "telemetry_average_load.xlsx")
tabela_banco = "telemetry_average_load"

arquivos = listar_arquivos(prefix)
todos_dados = []

for arq in arquivos:
    try:
        dados = abrir_json(arq)
        for item in dados:
            links = item.get("Links", [])
            hrefs = [l["href"] for l in links if l["rel"] == "self"]
            equipamento = extrair_equipamento(hrefs[0]) if hrefs else None

            fatores = item.get("LoadFactor", [])
            if not fatores:
                todos_dados.append({
                    "Equipamento": equipamento,
                    "LoadFactor": 0,
                    "DataHora": None,
                    "Arquivo": arq
                })
            else:
                for f in fatores:
                    todos_dados.append({
                        "Equipamento": equipamento,
                        "LoadFactor": f.get("LoadFactor", 0),
                        "DataHora": f.get("@datetime"),
                        "Arquivo": arq
                    })
    except Exception as e:
        print(f"⚠️ Erro ao processar {arq}: {e}")

# DataFrame
df = pd.DataFrame(todos_dados)

# Extrai a data do nome do arquivo
df["Data"] = df["Arquivo"].str.extract(r"_(\d{4}-\d{2}-\d{2})_")

# Organiza colunas
df = df[["Equipamento", "LoadFactor", "DataHora", "Data"]]

# Salva CSV
df.to_csv(output_csv, index=False)
print(f"✅ CSV salvo em: {output_csv}")

# Salva no banco
df.to_sql(tabela_banco, engine, schema="Fazenda_WS", if_exists="replace", index=False)
print(f"✅ Tabela recriada no banco: Fazenda_WS.{tabela_banco}")


# ### Parte 16 - Operations By Vehicle

# In[35]:


# Parte 16 - Operations By Vehicle

prefix = "operations_by_vehicle_"
output_csv = os.path.join(output_dir, "operations_by_vehicle.csv")
output_excel = os.path.join(output_dir, "operations_by_vehicle.xlsx")
tabela_banco = "operations_by_vehicle"

arquivos = listar_arquivos(prefix)
todos_dados = []

for arq in arquivos:
    try:
        dados = abrir_json(arq)
        for item in dados:
            company_id = item.get("companyId")
            operacoes = item.get("operationsByVehicles", [])
            if not operacoes:
                todos_dados.append({
                    "companyId": company_id,
                    "VehicleId": None,
                    "OperationId": None,
                    "OperationName": None,
                    "Arquivo": arq
                })
            else:
                for op in operacoes:
                    todos_dados.append({
                        "companyId": company_id,
                        "VehicleId": op.get("vehicleId"),
                        "OperationId": op.get("operationId"),
                        "OperationName": op.get("operationName"),
                        "Arquivo": arq
                    })
    except Exception as e:
        print(f"⚠️ Erro ao processar {arq}: {e}")

# DataFrame
df = pd.DataFrame(todos_dados)

# Extrai a data do nome do arquivo
df["Data"] = df["Arquivo"].str.extract(r"_(\d{4}-\d{2}-\d{2})_")

# Organiza colunas
df = df[["companyId", "VehicleId", "OperationId", "OperationName", "Data"]]

# Salva CSV
df.to_csv(output_csv, index=False)
print(f"✅ CSV salvo em: {output_csv}")

# Salva no banco
df.to_sql(tabela_banco, engine, schema="Fazenda_WS", if_exists="replace", index=False)
print(f"✅ Tabela recriada no banco: Fazenda_WS.{tabela_banco}")


# ### Dimensões

# In[36]:


import requests
import pandas as pd
import re
import os
import json
import sqlalchemy 
from urllib.parse import quote_plus


# Configurações de diretório e HDFS
output_dir = r"C:\Users\caval\Projetos\COLIN_WS\csv_corrigidos"
os.makedirs(output_dir, exist_ok=True)

namenode = "172.21.0.142"
port = 9870
user = "hdoop"
hdfs_path = "/bronze/agrin/suprema/dim/equipment.json"

# Conexão com banco
password = quote_plus("@Winover2024")
engine = create_engine(
    f"postgresql+psycopg2://wesley.carnauba:{password}@172.21.0.119/warin",
    connect_args={'client_encoding': 'utf8'}
)

# Função para baixar JSON diretamente do HDFS
def ler_json_hdfs(caminho_hdfs):
    url = f"http://{namenode}:{port}/webhdfs/v1{caminho_hdfs}?op=OPEN&user.name={user}"
    r1 = requests.get(url, allow_redirects=False)
    redirect = r1.headers.get("Location")
    if not redirect:
        raise Exception("❌ Redirecionamento HDFS falhou.")
    redirect = redirect.replace("trisk05", namenode).replace("trisk06", namenode).replace("trisk10", namenode)
    r2 = requests.get(redirect)
    r2.raise_for_status()
    return r2.json()

# Lê do HDFS
dados = ler_json_hdfs(hdfs_path)

from sqlalchemy import text

from sqlalchemy import text

# -------------------------------------------------------------------
# dim_equipment

equipamentos = dados["equipment"]
df = pd.json_normalize(equipamentos)

for col in df.columns:
    if df[col].apply(lambda x: isinstance(x, (dict, list))).any():
        df[col] = df[col].apply(lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (dict, list)) else x)

csv_path = os.path.join(output_dir, "dim_equipment.csv")
df.to_csv(csv_path, index=False)
print(f"✅ CSV salvo em: {csv_path}")

with engine.begin() as conn:
    conn.execute(text('DELETE FROM "Fazenda_WS".dim_equipment'))
    df.to_sql("dim_equipment", conn, schema="Fazenda_WS", if_exists="append", index=False)
print("✅ Tabela dim_equipment atualizada no schema Fazenda_WS")


# -------------------------------------------------------------------
# dim_farm_setup_company_details

hdfs_path = "/bronze/agrin/suprema/dim/farm_setup_company_details.json"
dados = ler_json_hdfs(hdfs_path)
df = pd.json_normalize(dados["companies"])

csv_path = os.path.join(output_dir, "dim_farm_setup_company_details.csv")
df.to_csv(csv_path, index=False)
print(f"✅ CSV salvo em: {csv_path}")

with engine.begin() as conn:
    conn.execute(text('DELETE FROM "Fazenda_WS".dim_farm_setup_company_details'))
    df.to_sql("dim_farm_setup_company_details", conn, schema="Fazenda_WS", if_exists="append", index=False)
print("✅ Tabela dim_farm_setup_company_details atualizada no schema Fazenda_WS")


# -------------------------------------------------------------------
# dim_farm_setup_fields

hdfs_path = "/bronze/agrin/suprema/dim/farm_setup_fields.json"
dados = ler_json_hdfs(hdfs_path)

company_id = dados.get("companyId")
grower_id = dados.get("growerId")
farm_id = dados.get("farmId")

fields = dados.get("fields", [])
df = pd.json_normalize(fields)
df["companyId"] = company_id
df["growerId"] = grower_id
df["farmId"] = farm_id

csv_path = os.path.join(output_dir, "dim_farm_setup_fields.csv")
df.to_csv(csv_path, index=False)
print(f"✅ CSV salvo em: {csv_path}")

with engine.begin() as conn:
    conn.execute(text('DELETE FROM "Fazenda_WS".dim_farm_setup_fields'))
    df.to_sql("dim_farm_setup_fields", conn, schema="Fazenda_WS", if_exists="append", index=False)
print("✅ Tabela dim_farm_setup_fields atualizada no schema Fazenda_WS")


# -------------------------------------------------------------------
# dim_farm_setup_growers

hdfs_path = "/bronze/agrin/suprema/dim/farm_setup_growers.json"
dados = ler_json_hdfs(hdfs_path)

company_id = dados.get("companyId")
growers = dados.get("growers", [])
df = pd.json_normalize(growers)
df["companyId"] = company_id

csv_path = os.path.join(output_dir, "dim_farm_setup_growers.csv")
df.to_csv(csv_path, index=False)
print(f"✅ CSV salvo em: {csv_path}")

with engine.begin() as conn:
    conn.execute(text('DELETE FROM "Fazenda_WS".dim_farm_setup_growers'))
    df.to_sql("dim_farm_setup_growers", conn, schema="Fazenda_WS", if_exists="append", index=False)
print("✅ Tabela dim_farm_setup_growers atualizada no schema Fazenda_WS")


# -------------------------------------------------------------------
# dim_farm_setup_user_profile

hdfs_path = "/bronze/agrin/suprema/dim/farm_setup_user_profile.json"
dados = ler_json_hdfs(hdfs_path)

df = pd.DataFrame([dados])

csv_path = os.path.join(output_dir, "dim_farm_setup_user_profile.csv")
df.to_csv(csv_path, index=False)
print(f"✅ CSV salvo em: {csv_path}")

with engine.begin() as conn:
    conn.execute(text('DELETE FROM "Fazenda_WS".dim_farm_setup_user_profile'))
    df.to_sql("dim_farm_setup_user_profile", conn, schema="Fazenda_WS", if_exists="append", index=False)
print("✅ Tabela dim_farm_setup_user_profile atualizada no schema Fazenda_WS")


# -------------------------------------------------------------------
# dim_files

hdfs_path = "/bronze/agrin/suprema/dim/files.json"
nome_dim = "dim_files"

dados = ler_json_hdfs(hdfs_path)
df = pd.json_normalize(dados["files"])

csv_path = os.path.join(output_dir, f"{nome_dim}.csv")
df.to_csv(csv_path, index=False)
print(f"✅ CSV salvo em: {csv_path}")

with engine.begin() as conn:
    conn.execute(text(f'DELETE FROM "Fazenda_WS".{nome_dim}'))
    df.to_sql(nome_dim, conn, schema="Fazenda_WS", if_exists="append", index=False)
print(f"✅ Tabela {nome_dim} atualizada no schema Fazenda_WS")



# -------------------------------------------------------------------

# dim_rx_direct_to_vehicle_activity_types

hdfs_path = "/bronze/agrin/suprema/dim/rx_direct_to_vehicle_activity_types.json"
nome_dim = "dim_rx_direct_to_vehicle_activity_types"

dados = ler_json_hdfs(hdfs_path)
df = pd.json_normalize(dados["activityTypes"])

csv_path = os.path.join(output_dir, f"{nome_dim}.csv")
df.to_csv(csv_path, index=False)
print(f"✅ CSV salvo em: {csv_path}")

df.to_sql(nome_dim, engine, schema="Fazenda_WS", if_exists="replace", index=False)
print(f"✅ Tabela {nome_dim} criada/recriada no schema Fazenda_WS")

# -------------------------------------------------------------------

# dim_rx_direct_to_vehicle_chemical_categories

hdfs_path = "/bronze/agrin/suprema/dim/rx_direct_to_vehicle_chemical_categories.json"
nome_dim = "dim_rx_direct_to_vehicle_chemical_categories"

dados = ler_json_hdfs(hdfs_path)
df = pd.json_normalize(dados["categories"])

csv_path = os.path.join(output_dir, f"{nome_dim}.csv")
df.to_csv(csv_path, index=False)
print(f"✅ CSV salvo em: {csv_path}")

df.to_sql(nome_dim, engine, schema="Fazenda_WS", if_exists="replace", index=False)
print(f"✅ Tabela {nome_dim} criada/recriada no schema Fazenda_WS")

# -------------------------------------------------------------------

# dim_rx_direct_to_vehicle_chemicals

hdfs_path = "/bronze/agrin/suprema/dim/rx_direct_to_vehicle_chemicals.json"
nome_dim = "dim_rx_direct_to_vehicle_chemicals"

dados = ler_json_hdfs(hdfs_path)
df = pd.json_normalize(dados["chemicals"])

csv_path = os.path.join(output_dir, f"{nome_dim}.csv")
df.to_csv(csv_path, index=False)
print(f"✅ CSV salvo em: {csv_path}")

df.to_sql(nome_dim, engine, schema="Fazenda_WS", if_exists="replace", index=False)
print(f"✅ Tabela {nome_dim} criada/recriada no schema Fazenda_WS")

# -------------------------------------------------------------------

# dim_rx_direct_to_vehicle_crops

hdfs_path = "/bronze/agrin/suprema/dim/rx_direct_to_vehicle_crops.json"
nome_dim = "dim_rx_direct_to_vehicle_crops"

dados = ler_json_hdfs(hdfs_path)
df = pd.json_normalize(dados["crops"])

csv_path = os.path.join(output_dir, f"{nome_dim}.csv")
df.to_csv(csv_path, index=False)
print(f"✅ CSV salvo em: {csv_path}")

df.to_sql(nome_dim, engine, schema="Fazenda_WS", if_exists="replace", index=False)
print(f"✅ Tabela {nome_dim} criada/recriada no schema Fazenda_WS")

# -------------------------------------------------------------------

# dim_rx_direct_to_vehicle_element_types

hdfs_path = "/bronze/agrin/suprema/dim/rx_direct_to_vehicle_element_types.json"
nome_dim = "dim_rx_direct_to_vehicle_element_types"

dados = ler_json_hdfs(hdfs_path)
df = pd.json_normalize(dados["elementTypes"])

csv_path = os.path.join(output_dir, f"{nome_dim}.csv")
df.to_csv(csv_path, index=False)
print(f"✅ CSV salvo em: {csv_path}")

df.to_sql(nome_dim, engine, schema="Fazenda_WS", if_exists="replace", index=False)
print(f"✅ Tabela {nome_dim} criada/recriada no schema Fazenda_WS")

# -------------------------------------------------------------------

# dim_rx_direct_to_vehicle_genetic_types

hdfs_path = "/bronze/agrin/suprema/dim/rx_direct_to_vehicle_genetic_types.json"
nome_dim = "dim_rx_direct_to_vehicle_genetic_types"

dados = ler_json_hdfs(hdfs_path)
df = pd.json_normalize(dados["geneticTypes"])

csv_path = os.path.join(output_dir, f"{nome_dim}.csv")
df.to_csv(csv_path, index=False)
print(f"✅ CSV salvo em: {csv_path}")

df.to_sql(nome_dim, engine, schema="Fazenda_WS", if_exists="replace", index=False)
print(f"✅ Tabela {nome_dim} criada/recriada no schema Fazenda_WS")

# -------------------------------------------------------------------

# dim_rx_direct_to_vehicle_product_forms

hdfs_path = "/bronze/agrin/suprema/dim/rx_direct_to_vehicle_product_forms.json"
nome_dim = "dim_rx_direct_to_vehicle_product_forms"

dados = ler_json_hdfs(hdfs_path)
df = pd.json_normalize(dados["productForms"])

csv_path = os.path.join(output_dir, f"{nome_dim}.csv")
df.to_csv(csv_path, index=False)
print(f"✅ CSV salvo em: {csv_path}")

df.to_sql(nome_dim, engine, schema="Fazenda_WS", if_exists="replace", index=False)
print(f"✅ Tabela {nome_dim} criada/recriada no schema Fazenda_WS")

# -------------------------------------------------------------------

# dim_rx_direct_to_vehicle_seasons

hdfs_path = "/bronze/agrin/suprema/dim/rx_direct_to_vehicle_seasons.json"
nome_dim = "dim_rx_direct_to_vehicle_seasons"

dados = ler_json_hdfs(hdfs_path)
df = pd.json_normalize(dados["seasons"])

csv_path = os.path.join(output_dir, f"{nome_dim}.csv")
df.to_csv(csv_path, index=False)
print(f"✅ CSV salvo em: {csv_path}")

df.to_sql(nome_dim, engine, schema="Fazenda_WS", if_exists="replace", index=False)
print(f"✅ Tabela {nome_dim} criada/recriada no schema Fazenda_WS")

# -------------------------------------------------------------------

# dim_rx_direct_to_vehicle_seed_brands

hdfs_path = "/bronze/agrin/suprema/dim/rx_direct_to_vehicle_seed_brands.json"
nome_dim = "dim_rx_direct_to_vehicle_seed_brands"

dados = ler_json_hdfs(hdfs_path)
df = pd.json_normalize(dados["brands"])

csv_path = os.path.join(output_dir, f"{nome_dim}.csv")
df.to_csv(csv_path, index=False)
print(f"✅ CSV salvo em: {csv_path}")

df.to_sql(nome_dim, engine, schema="Fazenda_WS", if_exists="replace", index=False)
print(f"✅ Tabela {nome_dim} criada/recriada no schema Fazenda_WS")

# -------------------------------------------------------------------

# dim_rx_direct_to_vehicle_seed_manufacturers

hdfs_path = "/bronze/agrin/suprema/dim/rx_direct_to_vehicle_seed_manufacturers.json"
nome_dim = "dim_rx_direct_to_vehicle_seed_manufacturers"

dados = ler_json_hdfs(hdfs_path)
df = pd.json_normalize(dados["manufacturers"])

csv_path = os.path.join(output_dir, f"{nome_dim}.csv")
df.to_csv(csv_path, index=False)
print(f"✅ CSV salvo em: {csv_path}")

df.to_sql(nome_dim, engine, schema="Fazenda_WS", if_exists="replace", index=False)
print(f"✅ Tabela {nome_dim} criada/recriada no schema Fazenda_WS")

# -------------------------------------------------------------------

# dim_rx_direct_to_vehicle_seeds

hdfs_path = "/bronze/agrin/suprema/dim/rx_direct_to_vehicle_seeds.json"
nome_dim = "dim_rx_direct_to_vehicle_seeds"

dados = ler_json_hdfs(hdfs_path)
df = pd.json_normalize(dados["seeds"])

csv_path = os.path.join(output_dir, f"{nome_dim}.csv")
df.to_csv(csv_path, index=False)
print(f"✅ CSV salvo em: {csv_path}")

df.to_sql(nome_dim, engine, schema="Fazenda_WS", if_exists="replace", index=False)
print(f"✅ Tabela {nome_dim} criada/recriada no schema Fazenda_WS")

# -------------------------------------------------------------------

# dim_rx_direct_to_vehicle_units_of_measure

hdfs_path = "/bronze/agrin/suprema/dim/rx_direct_to_vehicle_units_of_measure.json"
nome_dim = "dim_rx_direct_to_vehicle_units_of_measure"

dados = ler_json_hdfs(hdfs_path)
df = pd.json_normalize(dados["unitsOfMeasure"])

csv_path = os.path.join(output_dir, f"{nome_dim}.csv")
df.to_csv(csv_path, index=False)
print(f"✅ CSV salvo em: {csv_path}")

df.to_sql(nome_dim, engine, schema="Fazenda_WS", if_exists="replace", index=False)
print(f"✅ Tabela {nome_dim} criada/recriada no schema Fazenda_WS")

# -------------------------------------------------------------------

# dim_rx_direct_to_vehicle_units_of_measure_types

hdfs_path = "/bronze/agrin/suprema/dim/rx_direct_to_vehicle_units_of_measure_types.json"
nome_dim = "dim_rx_direct_to_vehicle_units_of_measure_types"

dados = ler_json_hdfs(hdfs_path)
df = pd.json_normalize(dados["unitOfMeasureTypes"])

csv_path = os.path.join(output_dir, f"{nome_dim}.csv")
df.to_csv(csv_path, index=False)
print(f"✅ CSV salvo em: {csv_path}")

df.to_sql(nome_dim, engine, schema="Fazenda_WS", if_exists="replace", index=False)
print(f"✅ Tabela {nome_dim} criada/recriada no schema Fazenda_WS")



# In[37]:


import requests
import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from datetime import datetime

# Configurações
API_KEY = 'AIzaSyC0C0TPNokotbZCDwV-0c5DZ8gS4lf49aY'
URL_FORECAST = 'https://weather.googleapis.com/v1/forecast/days:lookup'
latitude = -12.5420
longitude = -55.7211

# Conexão com PostgreSQL
password = quote_plus("@Winover2024")
engine = create_engine(
    f"postgresql+psycopg2://wesley.carnauba:{password}@172.21.0.119/warin",
    connect_args={'client_encoding': 'utf8'}
)

# Dicionários de tradução
TRADUCOES = {
    'weather_conditions': {
        'Mostly cloudy': 'predominantemente nublado',
        'Clear': 'céu limpo',
        'Sunny': 'ensolarado',
        'Partly sunny': 'parcialmente ensolarado',
        'Mostly sunny': 'predominantemente ensolarado',
        'Clear with periodic clouds': 'céu limpo com nuvens periódicas'
    },
    'precipitation_types': {
        'RAIN': 'chuva',
        'SNOW': 'neve',
        'ICE': 'gelo',
        'NONE': 'nenhuma'
    }
}

def safe_get_value(obj, *keys):
    """Percorre os níveis do dicionário e retorna o valor se existir"""
    for key in keys:
        if not isinstance(obj, dict):
            return None
        obj = obj.get(key)
    return obj

def get_weather_forecast():
    """Busca a previsão do tempo da API Google"""
    params = {
        'location.latitude': latitude,
        'location.longitude': longitude,
        'key': API_KEY
    }
    response = requests.get(URL_FORECAST, params=params)
    response.raise_for_status()
    return response.json()

def translate_values(data_dict):
    """Traduz os valores para português"""
    if data_dict.get('condicao_climatica') in TRADUCOES['weather_conditions']:
        data_dict['condicao_climatica'] = TRADUCOES['weather_conditions'][data_dict['condicao_climatica']]
    if data_dict.get('tipo_precipitacao') in TRADUCOES['precipitation_types']:
        data_dict['tipo_precipitacao'] = TRADUCOES['precipitation_types'][data_dict['tipo_precipitacao']]
    return data_dict

def create_forecast_df(forecast_data):
    """Cria DataFrame com os dados da previsão"""
    table_data = []

    # Temperatura atual (se existir)
    current_conditions = forecast_data.get('currentConditions', {})
    current_temp = safe_get_value(current_conditions, 'temperature', 'value')
    current_time = safe_get_value(current_conditions, 'asOf')

    if current_temp is not None and current_time is not None:
        table_data.append({
            'data_previsao': current_time[:10],
            'periodo': 'atual',
            'condicao_climatica': safe_get_value(current_conditions, 'weatherCondition', 'description', 'text'),
            'probabilidade_chuva': None,
            'tipo_precipitacao': None,
            'cobertura_nuvens': safe_get_value(current_conditions, 'cloudCover'),
            'umidade_relativa': safe_get_value(current_conditions, 'relativeHumidity'),
            'temperatura_maxima': current_temp,
            'temperatura_minima': None,
            'velocidade_vento': safe_get_value(current_conditions, 'wind', 'speed', 'value')
        })

    for day in forecast_data.get('forecastDays', []):
        daytime = day.get('daytimeForecast', {})
        nighttime = day.get('nighttimeForecast', {})
        date = safe_get_value(daytime, 'interval', 'startTime')
        date = date[:10] if date else None

        if not date:
            continue

        max_temp = day.get('maxTemperature', {}).get('degrees')
        min_temp = day.get('minTemperature', {}).get('degrees')

        day_data = {
            'data_previsao': date,
            'periodo': 'diurno',
            'condicao_climatica': safe_get_value(daytime, 'weatherCondition', 'description', 'text'),
            'probabilidade_chuva': safe_get_value(daytime, 'precipitation', 'probability', 'percent'),
            'tipo_precipitacao': safe_get_value(daytime, 'precipitation', 'probability', 'type'),
            'cobertura_nuvens': daytime.get('cloudCover'),
            'umidade_relativa': daytime.get('relativeHumidity'),
            'temperatura_maxima': max_temp,
            'temperatura_minima': min_temp,  # Aplicado aqui também
            'velocidade_vento': safe_get_value(daytime, 'wind', 'speed', 'value')
        }

        night_data = {
            'data_previsao': date,
            'periodo': 'noturno',
            'condicao_climatica': safe_get_value(nighttime, 'weatherCondition', 'description', 'text'),
            'probabilidade_chuva': safe_get_value(nighttime, 'precipitation', 'probability', 'percent'),
            'tipo_precipitacao': safe_get_value(nighttime, 'precipitation', 'probability', 'type'),
            'cobertura_nuvens': nighttime.get('cloudCover'),
            'umidade_relativa': nighttime.get('relativeHumidity'),
            'temperatura_maxima': max_temp,  # Também aqui
            'temperatura_minima': min_temp,
            'velocidade_vento': safe_get_value(nighttime, 'wind', 'speed', 'value')
        }

        table_data.extend([translate_values(day_data), translate_values(night_data)])

    return pd.DataFrame(table_data).dropna(subset=['data_previsao'])

def get_existing_dates():
    """Obtém as datas já existentes na tabela"""
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT DISTINCT data_previsao 
            FROM "Fazenda_WS".forecast
            WHERE data_previsao >= CURRENT_DATE
        """))
        return {row[0] for row in result}

def update_forecast_data():
    """Atualiza os dados de previsão na tabela"""
    try:
        print("📡 Obtendo dados atualizados da API...")
        api_data = get_weather_forecast()
        forecast_df = create_forecast_df(api_data)

        if forecast_df.empty:
            print("⚠️ Nenhum dado válido recebido da API.")
            return False

        forecast_df['data_previsao'] = pd.to_datetime(forecast_df['data_previsao'])
        forecast_df = forecast_df[forecast_df['data_previsao'] >= pd.to_datetime(datetime.now().date())]

        if forecast_df.empty:
            print("⚠️ Nenhum dado futuro ou atual disponível.")
            return False

        existing_dates = get_existing_dates()
        new_dates = set(forecast_df['data_previsao'].dt.date) - existing_dates

        with engine.begin() as conn:
            # Atualiza registros existentes
            for _, row in forecast_df[forecast_df['data_previsao'].dt.date.isin(existing_dates)].iterrows():
                conn.execute(text("""
                    UPDATE "Fazenda_WS".forecast 
                    SET condicao_climatica = :condicao,
                        probabilidade_chuva = :prob_chuva,
                        tipo_precipitacao = :tipo_precip,
                        cobertura_nuvens = :nuvens,
                        umidade_relativa = :umidade,
                        temperatura_maxima = COALESCE(:temp_max, temperatura_maxima),
                        temperatura_minima = COALESCE(:temp_min, temperatura_minima),
                        velocidade_vento = :vento
                    WHERE data_previsao = :data AND periodo = :periodo
                """), {
                    'data': row['data_previsao'],
                    'periodo': row['periodo'],
                    'condicao': row['condicao_climatica'],
                    'prob_chuva': row['probabilidade_chuva'],
                    'tipo_precip': row['tipo_precipitacao'],
                    'nuvens': row['cobertura_nuvens'],
                    'umidade': row['umidade_relativa'],
                    'temp_max': row['temperatura_maxima'],
                    'temp_min': row['temperatura_minima'],
                    'vento': row['velocidade_vento']
                })

            # Insere novos registros
            new_data = forecast_df[forecast_df['data_previsao'].dt.date.isin(new_dates)]
            if not new_data.empty:
                new_data.to_sql(
                    name='forecast',
                    con=conn,
                    schema='Fazenda_WS',
                    if_exists='append',
                    index=False
                )

        print(f"✅ Atualização concluída! {len(existing_dates)} datas atualizadas, {len(new_dates)} novas datas adicionadas.")
        return True

    except Exception as e:
        print(f"❌ Erro durante a atualização: {str(e)}")
        return False

if __name__ == "__main__":
    update_forecast_data()


# In[38]:


# Leitura do CSV ignorando a segunda linha
df = pd.read_csv(r"C:\Users\caval\Projetos\COLIN_WS\files\report_weather.csv", sep=";", skiprows=[1])

# Normalização dos nomes das colunas
def normalize_column(name):
    name = name.strip().lower()
    name = unicodedata.normalize('NFKD', name).encode('ASCII', 'ignore').decode('ASCII')  # Remove acentos
    name = re.sub(r'[^\w\s]', '', name)  # Remove caracteres especiais
    name = re.sub(r'\s+', '_', name)     # Substitui espaços por underscores
    return name

df.columns = [normalize_column(col) for col in df.columns]

# Corrigir separadores decimais (vírgula para ponto) em colunas de texto
for col in df.columns:
    if df[col].dtype == object:
        df[col] = df[col].str.replace(',', '.', regex=False)

# Tentar converter colunas para numérico
for col in df.columns:
    try:
        df[col] = pd.to_numeric(df[col])
    except:
        pass

# Conexão com PostgreSQL
password = quote_plus("@Winover2024")
engine = create_engine(
    f"postgresql+psycopg2://wesley.carnauba:{password}@172.21.0.119/warin",
    connect_args={'client_encoding': 'utf8'}
)

# Subir os dados para o schema "Fazenda_WS"
df.to_sql("clima_fazenda_ws", engine, schema="Fazenda_WS", if_exists="replace", index=False)

print("Dados importados com sucesso para o schema Fazenda_WS.")

