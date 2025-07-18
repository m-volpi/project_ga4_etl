from google.analytics.data_v1beta.types import RunReportRequest, DateRange, Dimension, Metric
import pandas as pd

def run(client, property_id, start, end):
    request = RunReportRequest(
        property=f"properties/{property_id}",
        dimensions=[
            Dimension(name="date"),
            Dimension(name="eventName"),
            Dimension(name="sessionSource"),
            Dimension(name="sessionMedium")
        ],
        metrics=[
            Metric(name="eventCount"),
            Metric(name="activeUsers"),
            Metric(name="engagedSessions"),
            Metric(name="transactions")
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
            "evento": dims[1].value,
            "fonte": dims[2].value,
            "meio": dims[3].value,
            "eventos": mets[0].value,
            "usuarios_ativos": mets[1].value,
            "sessoes_engajadas": mets[2].value,
            "transacoes": mets[3].value
        })

    df = pd.DataFrame(dados)
    df["data"] = pd.to_datetime(df["data"])

    colunas_numericas = ["eventos", "usuarios_ativos", "sessoes_engajadas", "transacoes"]
    df[colunas_numericas] = df[colunas_numericas].apply(pd.to_numeric, errors="coerce")

    return df