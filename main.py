from psycopg2 import sql
from datetime import datetime, timedelta
from google.oauth2 import service_account
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from core.config import GA4_PROPERTY_ID, CREDENTIALS_JSON
from core.conexao import salvar_no_postgres, conectar_postgres
from datetime import datetime
import traceback
from core.config import INFO_EMAIL

# Autenticação
credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_JSON)
client = BetaAnalyticsDataClient(credentials=credentials)
REMETENTE = INFO_EMAIL["REMETENTE"]
SENHA = INFO_EMAIL["SENHA"]
DESTINATARIOS = INFO_EMAIL["DESTINATARIOS"]

# Datas
hoje = datetime.today().date()
ano = hoje.year
antesontem = hoje - timedelta(days=2)


try:
    def gerar_data_range(tabela):
        # Começa sempre em 1º de janeiro do ano atual
        inicio = datetime(ano, 1, 1).date()
        fim = antesontem
        return inicio.strftime("%Y-%m-%d"), fim.strftime("%Y-%m-%d")

    def apagar_dados_ano_atual(conn, tabela, ano):
        """Apaga os dados do ano atual da tabela"""
        with conn.cursor() as cur:
            query = sql.SQL(
                "DELETE FROM {} WHERE EXTRACT(YEAR FROM data) = %s"
            ).format(sql.Identifier(tabela))
            cur.execute(query, (ano,))
        conn.commit()

    # --------------------------------------------------------------- #
    conn = conectar_postgres()

    blocos = [
        ("ga4_aquisicao_conversao", "aquisicao_conversao", "run_bloco1"),
        ("ga4_engajamento_qualidade", "engajamento_qualidade", "run"),
        ("ga4_eventos_comportamento", "eventos_comportamento", "run"),
        ("ga4_tecnologia_geolocalizacao", "tecnologia_geolocalizacao", "run"),
        ("ga4_produtos_ecommerce", "produtos_ecommerce", "run"),
    ]

    for tabela, modulo_nome, funcao in blocos:
        start, end = gerar_data_range(tabela)
        print(f"🔄 Processando {tabela}: {start} → {end}")

        modulo = __import__(f"blocos.{modulo_nome}", fromlist=[""])
        metodo = getattr(modulo, funcao)
        df = metodo(client, GA4_PROPERTY_ID, start, end)

        apagar_dados_ano_atual(conn, tabela, ano)
        salvar_no_postgres(df, tabela, conn)
        print(f"✅ {tabela} atualizado com {len(df)} registros.")

    conn.close()

except Exception as e:
    mensagem = f"""
❌ Erro ao executar o script GA4
📅 Data/Hora: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}
📌 Tipo de erro: {type(e).__name__}
💬 Mensagem: {str(e)}
🔍 Traceback:
{traceback.format_exc()}
"""
    print(mensagem)
    
    try:
        conn.close()
    except:
        pass  # ignora erro se conexão já estiver fechada

    raise