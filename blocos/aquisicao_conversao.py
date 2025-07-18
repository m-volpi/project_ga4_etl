from google.analytics.data_v1beta.types import RunReportRequest, DateRange, Dimension, Metric
import pandas as pd

def run_bloco1(client, property_id, start, end):
    request = RunReportRequest(
        property=f"properties/{property_id}",
        dimensions=[
            Dimension(name="date"),
            Dimension(name="sessionCampaignName"),
            Dimension(name="sessionSource"),
            Dimension(name="sessionMedium")
        ],
        metrics=[
            Metric(name="activeUsers"),
            Metric(name="advertiserAdClicks"),
            Metric(name="advertiserAdImpressions"),
            Metric(name="advertiserAdCost"),
            Metric(name="purchaseRevenue"),
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
            "campanha": dims[1].value,
            "fonte": dims[2].value,
            "meio": dims[3].value,
            "usuarios": mets[0].value,
            "cliques": mets[1].value,
            "impressoes": mets[2].value,
            "custo": mets[3].value,
            "receita": mets[4].value,
            "transacoes": mets[5].value
        })

    df = pd.DataFrame(dados)
    df["data"] = pd.to_datetime(df["data"])
    num_cols = ["usuarios", "cliques", "impressoes", "custo", "receita", "transacoes"]
    df[num_cols] = df[num_cols].apply(pd.to_numeric, errors="coerce")
    return df