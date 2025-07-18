from google.analytics.data_v1beta.types import RunReportRequest, DateRange, Dimension, Metric
import pandas as pd

def run(client, property_id, start, end):
    request = RunReportRequest(
        property=f"properties/{property_id}",
        dimensions=[
            Dimension(name="date"),
            Dimension(name="itemName"),
            Dimension(name="itemCategory"),
            Dimension(name="sessionSource"),
            Dimension(name="sessionMedium")
        ],
        metrics=[
            Metric(name="itemRevenue"),
            Metric(name="itemPurchaseQuantity"),
            Metric(name="cartToViewRate"),
            Metric(name="purchaseToViewRate")
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
            "produto": dims[1].value,
            "categoria": dims[2].value,
            "fonte": dims[3].value,
            "meio": dims[4].value,
            "receita_item": mets[0].value,
            "quantidade_vendida": mets[1].value,
            "taxa_carrinho": mets[2].value,
            "taxa_compra": mets[3].value
        })

    df = pd.DataFrame(dados)
    df["data"] = pd.to_datetime(df["data"])

    colunas_numericas = [
        "receita_item", "quantidade_vendida", "taxa_carrinho", "taxa_compra"
    ]
    df[colunas_numericas] = df[colunas_numericas].apply(pd.to_numeric, errors="coerce")

    return df