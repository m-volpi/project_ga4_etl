import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from core.config import POSTGRES_CONN
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
 
 
def get_ultima_data_postgres(tabela):
    conn = psycopg2.connect(**POSTGRES_CONN)
    query = f"SELECT MAX(data) FROM {tabela}"
    df = pd.read_sql(query, conn)
    conn.close()
    return df.iloc[0, 0]
 
def salvar_no_postgres(df, tabela, conn=None):
    from sqlalchemy import create_engine
    from core.config import POSTGRES_CONN
 
    if conn is None:
        engine_str = f"postgresql+psycopg2://{POSTGRES_CONN['user']}:{POSTGRES_CONN['password']}@{POSTGRES_CONN['host']}:{POSTGRES_CONN['port']}/{POSTGRES_CONN['dbname']}"
        engine = create_engine(engine_str)
    else:
        engine = create_engine("postgresql+psycopg2://", creator=lambda: conn)
 
    df.to_sql(tabela, engine, if_exists="append", index=False)
 
def conectar_postgres():
    return psycopg2.connect(**POSTGRES_CONN)
 
def enviar_email_log(assunto, mensagem, remetente, senha, destinatarios,port):
    try:
        # Se for string separada por ; ou ,, transforma em lista
        if isinstance(destinatarios, str):
            destinatarios = [email.strip() for email in destinatarios.replace(";", ",").split(",")]
 
        # Criação da mensagem
        msg = MIMEMultipart()
        msg["From"] = remetente
        msg["To"] = ", ".join(destinatarios)  # visualmente no email
        msg["Subject"] = assunto
 
        msg.attach(MIMEText(mensagem, "plain"))
 
        with smtplib.SMTP("smtp.office365.com", port) as server:
            server.starttls()
            server.login(remetente, senha)
            server.sendmail(remetente, destinatarios, msg.as_string())
 
        print("📧 Email de log enviado com sucesso.")
    except Exception as e:
        print(f"❌ Falha ao enviar email de log: {e}")