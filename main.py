from psycopg2 import sql
from datetime import datetime, timedelta
from google.oauth2 import service_account
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from core.config import GA4_PROPERTY_ID, CREDENTIALS_JSON
from core.conexao import salvar_no_postgres, conectar_postgres,enviar_email_log
from datetime import datetime
import traceback
from core.config import INFO_EMAIL
 
# Autenticação
credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_JSON)
client = BetaAnalyticsDataClient(credentials=credentials)
REMETENTE = INFO_EMAIL["REMETENTE"]
SENHA = INFO_EMAIL["SENHA"]
DESTINATARIOS = INFO_EMAIL["DESTINATARIOS"]
PORTA = INFO_EMAIL["port"]
 
# Datas
hoje = datetime.today().date()
antesontem = hoje - timedelta(days=2)

# Corrigir o mês (evitar mês 0 ou negativo)
mes = hoje.month - 2
ano = hoje.year

if mes <= 0:
    mes += 12
    ano -= 1

try:
    def gerar_data_range(tabela):
        # Gera um intervalo de datas de 1º dia do mês até antes de ontem
        inicio = datetime(ano, mes, 1).date()
        fim = antesontem
        return inicio.strftime("%Y-%m-%d"), fim.strftime("%Y-%m-%d")

    def apagar_dados_mes_ano(conn, tabela, ano, mes):
        # Apaga os dados da tabela a partir do mês e ano especificados
        with conn.cursor() as cur:
            query = sql.SQL("""
                DELETE FROM {tabela}
                WHERE data >= %s
            """).format(tabela=sql.Identifier(tabela))

            data_inicial = datetime(ano, mes, 1).date()
            cur.execute(query, (data_inicial,))
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
 
        apagar_dados_mes_ano(conn, tabela, ano, mes)
        salvar_no_postgres(df, tabela, conn)
        print(f"✅ {tabela} atualizado com {len(df)} registros.")
    
    mensagem = f"""
    ✅ Execução do script GA4 finalizada com atenção
    📅 Data/Hora: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}
    📌 {tabela} atualizado com {len(df)} registros.
    """
 
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
    enviar_email_log(
        assunto = "STATUS SCRIPT GA4",
        mensagem = mensagem,
        remetente = REMETENTE,
        senha = SENHA,
        destinatarios = DESTINATARIOS,
        port = PORTA  # <- Aqui está o ponto principal
    )
    print("📧 Email de confirmação enviado com sucesso.")
except Exception as email_erro:
    print("❌ Falha ao tentar enviar o e-mail de erro:", email_erro)

try:
    conn.close()
except:
    pass
    