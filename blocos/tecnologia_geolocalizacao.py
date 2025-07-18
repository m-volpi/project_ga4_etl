from google.analytics.data_v1beta.types import RunReportRequest, DateRange, Dimension, Metric
import pandas as pd

def run(client, property_id, start, end):
    request = RunReportRequest(
        property=f"properties/{property_id}",
        dimensions=[
            Dimension(name="date"),
            Dimension(name="deviceCategory"),
            Dimension(name="browser"),
            Dimension(name="country"),
            Dimension(name="region")
        ],
        metrics=[
            Metric(name="sessions"),
            Metric(name="engagedSessions"),
            Metric(name="bounceRate"),
            Metric(name="averageSessionDuration"),
            Metric(name="transactions"),
            Metric(name="totalRevenue")
        ],
        date_ranges=[DateRange(start_date=start, end_date=end)]
    )

    rows = client.run_report(request).rows
    dados = []

    for row in rows:
        dims = row.dimension_values
        mets = row.metric_values
        dados.append({
            "data": dims[0].value,
            "dispositivo": dims[1].value,
            "navegador": dims[2].value,
            "pais": dims[3].value,
            "regiao": dims[4].value,
            "sessoes": mets[0].value,
            "sessoes_engajadas": mets[1].value,
            "taxa_rejeicao": mets[2].value,
            "duracao_media": mets[3].value,
            "transacoes": mets[4].value,
            "receita": mets[5].value
        })

    df = pd.DataFrame(dados)
    df["data"] = pd.to_datetime(df["data"])

    colunas_numericas = [
        "sessoes", "sessoes_engajadas", "taxa_rejeicao",
        "duracao_media", "transacoes", "receita"
    ]
    df[colunas_numericas] = df[colunas_numericas].apply(pd.to_numeric, errors="coerce")

    return df