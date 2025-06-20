import os
import base64
import requests
import pandas as pd
import geopandas as gpd
import contextily as ctx
import matplotlib.pyplot as plt
import matplotlib.image as mpimg
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
import time
from tabulate import tabulate
from PIL import Image
import numpy as np
 

# ====================== CONFIG ======================
padrao_report_daily_path = "./files/padrao_report_daily.txt"
last_report_path = "./files/last_report.txt"
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

## Leitura de arquivos

def read_file(path):
    try:
        with open(path, 'r', encoding='utf-8') as file:
            return file.read()
    except:
        return ""

## Last Report

def salvar_last_report(mensagem):
    try:
        with open(last_report_path, 'w', encoding='utf-8') as file:
            file.write(mensagem)
        print("\n💾 Novo relatório salvo com sucesso em last_report.txt")
    except Exception as e:
        print(f"❌ Erro ao salvar relatório: {e}")

## Gerar Análise

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

## Gerar Mapa

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
            "AXIAL-FLOW® 7250": har_cnh_icon_path,
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
               SELECT 

               machine_id,
               machine_model as equip_name,
               lon_location as lon,
               lat_location as lat
                
               FROM "Fazenda_WS".machine_summary_view;
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


# ✅ API DE CLIMA
def get_weather_data():
    try:
        API_KEY = 'AIzaSyC0C0TPNokotbZCDwV-0c5DZ8gS4lf49aY'
        latitude = -9.8756
        longitude = -56.0861

        current_url = 'https://weather.googleapis.com/v1/currentConditions:lookup'
        forecast_url = 'https://weather.googleapis.com/v1/forecast/days:lookup'

        current_params = {
            'location.latitude': latitude,
            'location.longitude': longitude,
            'key': API_KEY,
            'languageCode': 'pt'
        }

        forecast_params = current_params.copy()
        forecast_params['days'] = 5

        current_response = requests.get(current_url, params=current_params)
        current_response.raise_for_status()
        current_data = current_response.json()

        forecast_response = requests.get(forecast_url, params=forecast_params)
        forecast_response.raise_for_status()
        forecast_data = forecast_response.json()

        return process_weather_data(current_data, forecast_data)

    except Exception as e:
        print(f"❌ Erro na API de Clima: {str(e)}")
        return "⚠️ Dados climáticos indisponíveis"

def process_weather_data(current_data, forecast_data):
    def translate_condition(condition_en):
        translations = {
            'PARTLY_CLOUDY': 'Parcialmente nublado',
            'MOSTLY_SUNNY': 'Predominantemente ensolarado',
            'MOSTLY_CLEAR': 'Predominantemente limpo',
            'CLEAR': 'Céu limpo',
            'SUNNY': 'Ensolarado',
            'PARTLY_SUNNY': 'Parcialmente ensolarado',
            'RAIN': 'Chuva',
            'SHOWERS': 'Chuva',
            'SCATTERED_SHOWERS': 'Chuvas esparsas',
            'THUNDERSTORMS': 'Tempestades',
            'SCATTERED_THUNDERSTORMS': 'Tempestades esparsas'
        }
        return translations.get(condition_en, condition_en)

    current = current_data.get('current', {})
    current_info = {
        'Temperatura': f"{current.get('temperature', {}).get('degrees', 'N/A')}°C",
        'Sensação': f"{current.get('feelsLikeTemperature', {}).get('degrees', 'N/A')}°C",
        'Condição': translate_condition(current.get('weatherCondition', {}).get('type', 'N/A')),
        'Umidade': f"{current.get('relativeHumidity', 'N/A')}%",
        'Vento': f"{current.get('wind', {}).get('speed', {}).get('value', 'N/A')} km/h",
        'Direção': current.get('wind', {}).get('direction', {}).get('cardinal', 'N/A')
    }

    forecast_days = forecast_data.get('forecastDays', [])
    forecast_info = []

    for day in forecast_days[:3]:
        date = f"{day['displayDate']['day']}/{day['displayDate']['month']}"
        forecast_info.append({
            'Data': date,
            'Condição': translate_condition(day['daytimeForecast']['weatherCondition']['type']),
            'Max/Min': f"{day['maxTemperature']['degrees']}°C/{day['minTemperature']['degrees']}°C",
            'Chuva': f"{day['daytimeForecast']['precipitation']['probability']['percent']}%"
        })

    current_table = tabulate([current_info], headers='keys', tablefmt='grid')
    forecast_table = tabulate(forecast_info, headers='keys', tablefmt='grid')

    return f"""
=== DADOS ATUAIS DO CLIMA ===
{current_table}

=== PREVISÃO PARA OS PRÓXIMOS DIAS ===
{forecast_table}
"""

# ========== CONSULTA AO BANCO ==========

### Mecanização

def read_machine_summary():
    schema = "Fazenda_WS"
    table = "machine_summary_view"
    descricao = "Resumo operacional / Máquinas em ação"

    with engine.connect() as conn:
        try:
            full_table = f'"{schema}"."{table}"'
            with conn.begin():
                query = text(f"SELECT * FROM {full_table}")
                df = pd.read_sql_query(query, conn)

                print(f"\n✅ {descricao} ({schema}.{table})")
                print(df.head(1).to_string(index=False))

                result = f"\n\n=== {descricao} ({schema}.{table}) ===\n{df.to_string(index=False)}"
                return result
        except Exception as e:
            print(f"❌ Erro ao acessar {schema}.{table}: {str(e)}")
            conn.rollback()
            return ""

## Alertas

def read_machine_alerts():
    schema = "Fazenda_WS"
    table = "machine_alerts_view"
    descricao = "Alertas das máquinas"

    with engine.connect() as conn:
        try:
            full_table = f'"{schema}"."{table}"'
            with conn.begin():
                query = text(f"SELECT * FROM {full_table}")
                df = pd.read_sql_query(query, conn)

                print(f"\n✅ {descricao} ({schema}.{table})")
                print(df.head(1).to_string(index=False))

                result = f"\n\n=== {descricao} ({schema}.{table}) ===\n{df.to_string(index=False)}"
                return result
        except Exception as e:
            print(f"❌ Erro ao acessar {schema}.{table}: {str(e)}")
            conn.rollback()
            return ""

### QTDE DE ALERTAS

with engine.connect() as conn:
    query_alerts = 'SELECT * FROM "Fazenda_WS"."machine_alerts_view"'
    df_alerts = pd.read_sql_query(query_alerts, conn)

    alert_counts = df_alerts.groupby('machine_model').size().reset_index(name='qtd_alertas')
    alert_counts = alert_counts.sort_values(by='qtd_alertas', ascending=False)

    alert_summary = tabulate(alert_counts, headers='keys', tablefmt='grid')

### Dados de Máquina

with engine.connect() as conn:
    query_summary = 'SELECT * FROM "Fazenda_WS"."machine_summary_view"'
    df_summary = pd.read_sql_query(query_summary, conn)

    def seconds_to_hhmm(seconds):
        hours = int(seconds) // 3600
        minutes = (int(seconds) % 3600) // 60
        return f"{hours:02d}:{minutes:02d}"

    df_summary['working_time_hhmm'] = df_summary['working_time'].apply(seconds_to_hhmm)
    df_summary['idle_time_hhmm'] = df_summary['idle_time'].apply(seconds_to_hhmm)
    df_summary['status'] = df_summary['machine_state'].apply(lambda x: 'Inativa' if x == 'Inative' else 'Ativa')

    result = df_summary[['machine_model', 'working_time_hhmm', 'idle_time_hhmm', 'status']].drop_duplicates()

    resumo_tempos = tabulate(result, headers='keys', tablefmt='grid')
    

### Tipo de Máquinas
    
with engine.connect() as conn:
    query_type = 'SELECT * FROM "Fazenda_WS"."machine_summary_view"'
    df_type = pd.read_sql_query(query_type, conn)
    
    # Agrupa os modelos por tipo de máquina e calcula a quantidade
    result = df_type.groupby('machine_type').agg(
        modelos_agrupados=('machine_model', lambda x: ', '.join(sorted(set(x)))),
        quantidade=('machine_model', 'count')
    ).reset_index()
    
    tipo_maquinas = tabulate(result, 
                           headers=['Tipo de Máquina', 'Modelos', 'Quantidade'],
                           tablefmt='grid')

# Link2Go

def enviar_link2go(token, imagem_mapa, mensagem, numeros_destino):
    url = "https://zap2go-api.link2go.com.br/v1/Ativo"

    try:
        with open(imagem_mapa, "rb") as f:
            imagem_base64 = base64.b64encode(f.read()).decode('utf-8')

        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

        # ✅ Verificação da mensagem
        print(f"📝 Tamanho da mensagem: {len(mensagem)} caracteres")
        print("📝 Mensagem original (repr):")
        print(repr(mensagem))

        count_asterisk = mensagem.count('*')
        if count_asterisk % 2 != 0:
            print(f"⚠️ CUIDADO! Número ímpar de asteriscos: {count_asterisk}. Pode quebrar formatação no WhatsApp.")

        if len(mensagem) > 1000:
            print("⚠️ Mensagem muito grande! Será cortada para 1000 caracteres.")
            mensagem = mensagem[:1000]

        for numero in numeros_destino:
            print(f"📨 Enviando para {numero}")

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
            print(f"📩 Resposta da API (imagem): {res.text}")

            # ✅ Delay configurável
            time.sleep(delay_envio)

            # ✅ Enviar texto original
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
            print(f"📩 Resposta da API (texto): {res_txt.text}")

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

    padrao_report_daily = read_file(padrao_report_daily_path)
    last_report = read_file(last_report_path)
    weather_data = get_weather_data()
    mecanizacao_content = read_machine_summary()
    alertas_content = read_machine_alerts()
    
    conteudo_dinamico = f"""
 Você é o Colin, o Colaborador de Inteligência Artificial do projeto DataAgrin, inspirado no personagem Colin Lawson da série Territory. Minha função principal é auxiliar na modelagem, análise e desenvolvimento de soluções estratégicas voltadas ao agronegócio, sempre com foco na eficiência, inovação e clareza.

    Suas Características:
    Especialista no Agro:

    Suas respostas são objetivas e resumidas de forma inteligente e nunca escreve textos complexos.

    Suas Respostas vem com o dialeto do campo, exemplo: "AOOO PARCEIRO".

*Você esta disparando um relatório para um cliente! Nâo esta falando diretamente comigo,*

### Dados da operação
{mecanizacao_content}

### A parte de clima, temperatura, umidade e etc (🌦 *Clima em Alta Floresta/MT*)... pegar do {weather_data}

### pegar nível de combustível na {mecanizacao_content}, coluna chamada fuel_level

### modelo das máquinas, também na {mecanizacao_content}, coluna machine_model e o tipo na coluna machine_type

### QUANTIDADE DE ALERTAS POR MÁQUINAS ESTA AQUI: {alert_summary}, a descrição de que alertas são esses, esta na {alertas_content}

### Modelo da máquinas da resumo de tempos esta na {resumo_tempos}, coluna chamada machine_model, para que você consiga cruzar com os dados da {alertas_content}, {mecanizacao_content} e etc....

### Tempo trabalhado esta na {resumo_tempos}, coluna chamada Trabalhado

### Tempo Ocioso esta na {resumo_tempos}, coluna chamada Ocioso

### Status das máquinas estão na {resumo_tempos}, coluna chamada Status, aqui se tiver "Inativo" é que a máquina não trabalhou no dia, se tiver "Ativo" quer dizer que ela trabalhou

### Manter o padrão utilizado na {last_report}, daqui não se pode pegar nenhum nado, somente usar como base, pra você entender a estrutura, icones utilizados, linguajar e etc....

*não manda esse tipo de coisas (Relatório objetivo, dentro do limite de caracteres e com informações acionáveis, conforme solicitado.) é um relatório enviado para o cliente, se mandarmos isso ele não vai tentender nada*

*Não informar o horário de envio*

* você esta errando muito no tipo de máquina, trazendo nos campos errados! Consulta os tipos de máquina aqui: {tipo_maquinas}

* quantidade de máquinas por tipo aqui: {tipo_maquinas}

* você esta errando a quantidade total de máquinas ativas! Soma a coluna quantidade daqui: {tipo_maquinas}

*nunca ultrapassar 990 caracteres! Manter a análise dentro dos 990 caracteres*

*dar dicas do que pode fazer hoje*

*não mandara nenhum comentário, como total de caracteres e etc...! Se mantenha a 990 caracteres!*

* as dicas são pra amanhã, esse relatório vai sair ás 18h*

"""

    mensagem = gerar_analise(conteudo_dinamico)
    salvar_last_report(mensagem)

    if gerar_mapa():
        enviar_link2go(token, imagem_mapa, mensagem, numeros_destino)
    else:
        print("❌ Não foi possível gerar o mapa.")


