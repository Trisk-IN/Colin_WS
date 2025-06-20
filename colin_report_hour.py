import os
import base64
import requests
import pandas as pd
import geopandas as gpd
import contextily as ctx
import matplotlib.pyplot as plt
import matplotlib.image as mpimg
from sqlalchemy import create_engine, text
from io import StringIO
from urllib.parse import quote_plus
import time

# ====================== CONFIG ======================
padrao_alertas_path = "./files/padrao_alertas.txt"
last_hour_path = "./files/last_hour.txt"
imagem_mapa = "mapa_maquinas_icones_legenda.png"
tractor_icon_path = r"./icons/Trator.png"
harvester_icon_path = r"./icons/Colheitadeira.png"
JD_524K_icon_path = r"./icons/524K.png"
JD_4040DN_icon_path = r"./icons/4040DN.png"
JD_7200J_icon_path = r"./icons/7200J.png"
JD_7225J_icon_path = r"./icons/7225J.png"
JD_PV4730_icon_path = r"./icons/PV 4730.png"
JD_PVM4025_icon_path = r"./icons/PV M4025.png"
JD_S68002_icon_path = r"./icons/S680 02.png"
JD_S680_icon_path = r"./icons/S680.png"
JD_S780_icon_path = r"./icons/S780.png"
JD_S78002_icon_path = r"./icons/S780 02.png"
JD_TR6115J_icon_path = r"./icons/TR 6115J.png"
JD_TR9640RX_icon_path = r"./icons/TR 9640RX.png"
spr_cnh_icon_path = r"./icons/spr_cnh.png"
har_cnh_icon_path = r"./icons/har_cnh.png"
tra_cnh_icon_path = r"./icons/tra_cnh.png"
telefones_csv = "./files/Telefones_Envio.csv"

password = quote_plus("@Winover2024")
engine = create_engine(
    f"postgresql+psycopg2://wesley.carnauba:{password}@172.21.0.119/warin",
    connect_args={'client_encoding': 'utf8'}
)

link2go_login = "Triskin"
link2go_senha = "Wesley@123"
link2go_carteira = 3381
link2go_source_id = 1
link2go_channel = "WHATSAPP"

# ✅ Configurável: Delay entre imagem e texto
delay_envio = 15  # segundos

# ====================== FUNÇÕES ======================

def fazer_login_link2go():
    url = "https://zap2go-api.link2go.com.br/v1/User/Login"
    payload = {"login": link2go_login, "password": link2go_senha}
    headers = {"Content-Type": "application/json"}
    try:
        res = requests.post(url, json=payload, headers=headers)
        res.raise_for_status()
        token = res.json().get("data", {}).get("token")
        print("✅ Login Link2Go realizado com sucesso.") if token else print("❌ Falha ao obter token.")
        return token
    except Exception as e:
        print(f"❌ Erro ao autenticar na Link2Go: {e}")
        return None

def read_file(path):
    try:
        with open(path, 'r', encoding='utf-8') as file:
            return file.read()
    except:
        return ""

def salvar_last_hour(mensagem):
    try:
        with open(last_hour_path, 'w', encoding='utf-8') as file:
            file.write(mensagem)
        print("\n💾 Novo relatório horário salvo com sucesso em last_hour.txt")
    except Exception as e:
        print(f"❌ Erro ao salvar relatório horário: {e}")

def gerar_analise(prompt_final):
    api_key = "sk-493ebdc65d8e4a60a8dd91310dab9b6d"
    url = "https://api.deepseek.com/v1/chat/completions"
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    payload = {
        "model": "deepseek-chat",
        "messages": [
            {"role": "system", "content": "Você é Colin, analista digital"},
            {"role": "user", "content": prompt_final}
        ],
        "temperature": 0.7,
        "top_p": 1,
        "max_tokens": 1000
    }
    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        return response.json()['choices'][0]['message']['content']
    except Exception as e:
        return f"Erro ao gerar análise: {e}"

from PIL import Image
import numpy as np

def gerar_mapa():
    try:
        # Mapeamento de ícones e tamanhos
        icon_mapping = {
            "524K": JD_524K_icon_path,
            "4040DN": JD_4040DN_icon_path,
            "7200J": JD_7200J_icon_path,
            "7225J": JD_7225J_icon_path,
            "PV 4730": JD_PV4730_icon_path,
            "PV M4025": JD_PVM4025_icon_path,
            "S680 - 02": JD_S68002_icon_path,
            "S680": JD_S680_icon_path,
            "S780": JD_S780_icon_path,
            "S780 02": JD_S78002_icon_path,
            "TR 6115J": JD_TR6115J_icon_path,
            "6115J": JD_TR6115J_icon_path,
            "TR 9640RX": JD_TR9640RX_icon_path,
            "TR": tractor_icon_path,
            "COLHEITADEIRA": harvester_icon_path,
            "Patriot® 350": spr_cnh_icon_path,
            "MAGNUM™ 340 AFS CONNECT™": tra_cnh_icon_path,
            "AXIAL-FLOW® 8250": har_cnh_icon_path,
            "AXIAL-FLOW® 8230": har_cnh_icon_path,
            "FROTA 219": spr_cnh_icon_path,
            "FROTA 213": tra_cnh_icon_path,
            "FROTA 214": har_cnh_icon_path,
            "FROTA 217": har_cnh_icon_path,
            "FROTA 215": har_cnh_icon_path
        }

        icon_sizes = {
            "DEFAULT": (40, 40),  # pixels
            **{k: (40, 40) for k in icon_mapping if k != "DEFAULT"}
        }

        # Pré-carregar e redimensionar os ícones
        loaded_icons = {}
        for key, path in icon_mapping.items():
            if os.path.exists(path):
                img = Image.open(path).convert("RGBA").resize(icon_sizes.get(key, icon_sizes["DEFAULT"]))
                loaded_icons[key.upper()] = np.asarray(img)

        query = """
        SELECT DISTINCT
            A.machine_id,
            A.equip_name,
            A.lon,
            A.lat
        FROM "Fazenda_WS".machine_alerts_view A;
        """
        df_machines = pd.read_sql(query, engine)
        if df_machines.empty:
            print("Nenhuma máquina encontrada para gerar mapa!")
            return False

        machines = [
            {"modelo": str(row["equip_name"]), "lat": float(row["lat"]), "lon": float(row["lon"])}
            for _, row in df_machines.iterrows()
        ]

        gdf = gpd.GeoDataFrame(
            machines,
            geometry=gpd.points_from_xy(
                [m["lon"] for m in machines],
                [m["lat"] for m in machines]
            ),
            crs="EPSG:4326"
        ).to_crs(epsg=3857)

        fig, ax = plt.subplots(figsize=(12, 6))
        buffer_x = 2200
        buffer_y = 1000
        ax.set_xlim(gdf.geometry.x.min() - buffer_x, gdf.geometry.x.max() + buffer_x)
        ax.set_ylim(gdf.geometry.y.min() - buffer_y, gdf.geometry.y.max() + buffer_y)

        ctx.add_basemap(ax, source=ctx.providers.Esri.WorldImagery, zoom=16, reset_extent=False)

        for idx, row in gdf.iterrows():
            x, y = row.geometry.x, row.geometry.y
            modelo = row["modelo"]

            icon_key = "DEFAULT"
            for key in loaded_icons:
                if key in modelo.upper():
                    icon_key = key
                    break

            img_array = loaded_icons.get(icon_key)
            if img_array is not None:
                ax.imshow(img_array, extent=[x - 200, x + 200, y - 200, y + 200], alpha=0.85)

            ax.text(
                x, y + 250, modelo,
                fontsize=4,
                color="white",
                fontweight="bold",
                ha="center",
                bbox=dict(facecolor="black", alpha=0.6, edgecolor="none", pad=0.5)
            )

        ax.set_axis_off()
        plt.savefig(imagem_mapa, dpi=150, bbox_inches="tight")
        plt.close()
        print(f"✅ Mapa gerado e salvo em: {imagem_mapa}")
        return True

    except Exception as e:
        print(f"❌ Erro ao gerar mapa: {e}")
        return False



def montar_dados_alerta():
    try:
        last_hour = read_file(last_hour_path)
        padrao_alertas = read_file(padrao_alertas_path)
        query = '''
        SELECT DISTINCT
            ma.machine_id,
            ma.lon,
            ma.lat,
			ma.alert_registered_at,
			ma.alert_color,
			ma.alert_description,
			ma.alert_severity,
			ma.alert_hour,
			ma.equip_name
			
        FROM "Fazenda_WS".machine_alerts_view ma;
        '''
        df = pd.read_sql_query(text(query), engine)
        return padrao_alertas, last_hour, df.to_string(index=False) if not df.empty else ""
    except Exception as e:
        print(f"❌ Erro ao montar dados: {e}")
        return "", "", ""

def deve_enviar_alerta_por_qtd(status_atual, qtd_alertas):
    try:
        query = text("""
        SELECT COUNT(*) 
        FROM "Fazenda_WS".alerta_logs 
        WHERE status = :status 
        AND qtd_alertas = :qtd_alertas
        """)
        params = {'status': status_atual, 'qtd_alertas': qtd_alertas}
        with engine.connect() as conn:
            total_ocorrencias = conn.execute(query, params).scalar()
        return total_ocorrencias == 0 or (total_ocorrencias % 5 == 0)
    except Exception as e:
        print(f"❌ Erro ao verificar histórico de logs: {e}")
        return True

def registrar_log_alerta(status, qtd_alertas):
    try:
        query = text("""
        INSERT INTO "Fazenda_WS".alerta_logs  (data, status, qtd_alertas)
        VALUES (NOW(), :status, :qtd_alertas)
        """)
        params = {'status': status, 'qtd_alertas': qtd_alertas}
        with engine.begin() as conn:
            conn.execute(query, params)
        print("📝 Log registrado no banco de dados")
    except Exception as e:
        print(f"❌ Erro ao registrar log no banco: {e}")

# ✅ Função com delay configurável
def enviar_link2go(token, imagem_mapa, mensagem, numeros_destino):
    url = "https://zap2go-api.link2go.com.br/v1/Ativo"
    try:
        with open(imagem_mapa, "rb") as f:
            imagem_base64 = base64.b64encode(f.read()).decode('utf-8')

        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

        for numero in numeros_destino:
            # ✅ Enviar a imagem primeiro
            payload_img = {
                "CarteiraId": link2go_carteira,
                "Address": numero,
                "ClientDocument": "123456",
                "ClientName": "Wesley",
                "Caption": "Mapa Atualizado",
                "FileName": os.path.basename(imagem_mapa),
                "MessageText": f"data:image/png;base64,{imagem_base64}",
                "MessageType": 2,
                "channelCode": "WHATSAPP",
                "SourceId": 1
            }
            res = requests.post(url, json=payload_img, headers=headers)
            print(f"✅ Imagem enviada para {numero} - Status: {res.status_code}")

            # ✅ Delay configurável
            time.sleep(delay_envio)

            # ✅ Depois enviar o texto
            payload_txt = {
                "CarteiraId": link2go_carteira,
                "Address": numero,
                "ClientDocument": "123456",
                "ClientName": "Wesley",
                "MessageText": mensagem,
                "MessageType": 1,
                "channelCode": "WHATSAPP",
                "SourceId": 1
            }
            res_txt = requests.post(url, json=payload_txt, headers=headers)
            print(f"✅ Texto enviado para {numero} - Status: {res_txt.status_code}")

    except Exception as e:
        print(f"❌ Erro ao enviar via Link2Go: {e}")

# ====================== EXECUÇÃO ======================
if __name__ == "__main__":
    token = fazer_login_link2go()
    if not token:
        print("❌ Abortando: não foi possível autenticar na Link2Go.")
        exit()

    try:
        df_telefones = pd.read_csv(telefones_csv, encoding="latin1", sep=";", dtype=str)
        numeros_destino = df_telefones["VALOR_DO_REGISTRO"].dropna().tolist()
    except Exception as e:
        print(f"❌ Erro ao ler CSV de telefones: {e}")
        numeros_destino = []

    padrao_alertas, last_hour, alert_data = montar_dados_alerta()
    status_atual = "SEM_ALERTA" if not alert_data.strip() else "COM_ALERTA"
    qtd_alertas = alert_data.count('\n') if alert_data.strip() else 0

    if not deve_enviar_alerta_por_qtd(status_atual, qtd_alertas):
        print("🚫 NÃO DEVE ENVIAR AGORA.")
        registrar_log_alerta(status_atual, qtd_alertas)
    else:
        if status_atual == "SEM_ALERTA":
            mensagem = "📢 Relatório de Alertas – Nenhum alerta registrado no último período."
        else:
            conteudo_dinamico = f"""
Você é o Colin, o COLaborador de INteligência Artificial do projeto DataAgrin. Minha função principal é auxiliar na modelagem, análise e desenvolvimento de soluções estratégicas voltadas ao agronegócio.

Gere um relatório horário de alertas referente à operação de máquinas.

O relatório deve conter **no máximo 1000 caracteres**.
Use o nome das máquinas (equip_name), data (alert_registered_at), hora (alert_hour).
Traduza os alertas para português.
Classifique o risco por cores: verde = baixo, amarelo = médio, vermelho = alto.

*não enviar esse tipo de informação : "(Total: 499 caracteres)" ! Você esta enviando relatório um informativo para o usuário, não para mim! Esses detalhes tecnicos não fazem sentido *

*não informar o horário que foi enviado, pois ele vai receber o whatsapp, ele sabe quando recebeu!*

### Padrão (*Usar como exemplo*):
{padrao_alertas}

### Último relatório:
{last_hour}

### Novos alertas:
{alert_data}
"""
            mensagem = gerar_analise(conteudo_dinamico)

        salvar_last_hour(mensagem)
        registrar_log_alerta(status_atual, qtd_alertas)

        if gerar_mapa():
            enviar_link2go(token, imagem_mapa, mensagem, numeros_destino)
        else:
            print("❌ Não foi possível gerar o mapa.")