from google.analytics.data_v1beta.types import RunReportRequest, DateRange, Dimension, Metric
import pandas as pd

def run(client, property_id, start, end):
    request = RunReportRequest(
        property=f"properties/{property_id}",
        dimensions=[
            Dimension(name="date"),
            Dimension(name="sessionCampaignName"),
            Dimension(name="sessionSource"),
            Dimension(name="sessionMedium")
        ],
        metrics=[
            Metric(name="sessions"),
            Metric(name="newUsers"),
            Metric(name="totalUsers"),
            Metric(name="engagedSessions"),
            Metric(name="bounceRate"),
            Metric(name="averageSessionDuration")
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
            "campanha": dims[1].value,
            "fonte": dims[2].value,
            "meio": dims[3].value,
            "sessoes": mets[0].value,
            "novos_usuarios": mets[1].value,
            "usuarios_totais": mets[2].value,
            "sessoes_engajadas": mets[3].value,
            "taxa_rejeicao": mets[4].value,
            "duracao_media": mets[5].value
        })

    df = pd.DataFrame(dados)
    df["data"] = pd.to_datetime(df["data"])

    colunas_numericas = [
        "sessoes", "novos_usuarios", "usuarios_totais",
        "sessoes_engajadas", "taxa_rejeicao", "duracao_media"
    ]
    df[colunas_numericas] = df[colunas_numericas].apply(pd.to_numeric, errors="coerce")

    return df