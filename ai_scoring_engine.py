"""
ai_scoring_engine.py

The "Intelligence Tier" of the UNDP streaming architecture.

What it does:
1. Pulls project-level data from ClickHouse (undp.master_project_summary)
2. Trains a small regression model that predicts a project's EXPECTED
   impact efficiency (beneficiaries per $1M) based on its sector, region,
   and funding size.
3. Compares each project's ACTUAL efficiency to the model's prediction.
   A big gap (positive or negative) flags it as a statistical outlier --
   either surprisingly effective, or underperforming for its context.
4. Uses SHAP to explain WHY the model predicted what it did for each
   project -- e.g. "sector contributed +40 to predicted efficiency,
   region contributed -10".
5. Writes everything back into a new ClickHouse table: undp.impact_scores

USAGE:
    pip install pandas numpy scikit-learn shap clickhouse-connect
    python ai_scoring_engine.py

CONFIG:
    Edit CLICKHOUSE_CONFIG below if your ClickHouse setup differs.
"""

import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import LabelEncoder
import shap
import clickhouse_connect
import json
import os

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
CLICKHOUSE_CONFIG = {
    "host": os.environ.get("CLICKHOUSE_HOST", "localhost"),
    "port": int(os.environ.get("CLICKHOUSE_PORT", "8124")),
    "username": "default",
    "password": __import__("os").environ.get("CLICKHOUSE_PASSWORD", ""),
}

OUTLIER_THRESHOLD_STD = 1.5  # projects whose residual is beyond this many
                             # standard deviations are flagged as outliers


def fetch_project_data(client):
    """Pull the denormalized project summary table from ClickHouse."""
    query = """
        SELECT
            project_id,
            project_name,
            sector,
            region_name,
            total_funding_usd,
            total_beneficiaries,
            beneficiaries_per_million_usd
        FROM undp.master_project_summary
        WHERE total_funding_usd > 0
    """
    df = client.query_df(query)
    return df


def train_model_and_score(df):
    """
    Train a model to predict expected efficiency from sector/region/funding,
    then compute how far each project's actual efficiency deviates from
    what was expected -- that deviation is the "outlier signal".
    """
    df = df.copy()

    # Encode categorical features as numbers for the model
    sector_encoder = LabelEncoder()
    region_encoder = LabelEncoder()
    df["sector_encoded"] = sector_encoder.fit_transform(df["sector"])
    df["region_encoded"] = region_encoder.fit_transform(df["region_name"])

    # Log-transform funding since disbursement amounts vary by orders of magnitude
    df["log_funding"] = np.log1p(df["total_funding_usd"])

    feature_cols = ["sector_encoded", "region_encoded", "log_funding"]
    X = df[feature_cols]
    y = df["beneficiaries_per_million_usd"]

    model = RandomForestRegressor(n_estimators=100, max_depth=6, random_state=42)
    model.fit(X, y)

    df["predicted_efficiency"] = model.predict(X)
    df["residual"] = df["beneficiaries_per_million_usd"] - df["predicted_efficiency"]

    # Flag outliers: residuals more than N standard deviations from the mean
    residual_std = df["residual"].std()
    residual_mean = df["residual"].mean()
    df["is_outlier"] = (
        (df["residual"] - residual_mean).abs() > OUTLIER_THRESHOLD_STD * residual_std
    )
    df["outlier_direction"] = np.where(
        df["residual"] > 0, "overperforming", "underperforming"
    )

    # SHAP explanations -- how much each feature contributed to the prediction
    explainer = shap.TreeExplainer(model)
    shap_values = explainer.shap_values(X)

    shap_explanations = []
    for i, row in enumerate(df.itertuples()):
        contributions = {
            "sector": round(float(shap_values[i][0]), 2),
            "region": round(float(shap_values[i][1]), 2),
            "funding_level": round(float(shap_values[i][2]), 2),
        }
        top_factor = max(contributions, key=lambda k: abs(contributions[k]))
        shap_explanations.append({
            "contributions": contributions,
            "top_factor": top_factor,
            "top_factor_contribution": contributions[top_factor],
        })

    df["shap_explanation"] = [json.dumps(exp) for exp in shap_explanations]
    df["top_factor"] = [exp["top_factor"] for exp in shap_explanations]

    return df


def write_scores_to_clickhouse(client, df):
    """Create (or replace) the impact_scores table and insert the results."""
    client.command("""
        CREATE TABLE IF NOT EXISTS undp.impact_scores
        (
            project_id String,
            project_name String,
            sector String,
            region_name String,
            total_funding_usd Float64,
            total_beneficiaries UInt64,
            actual_efficiency Float64,
            predicted_efficiency Float64,
            residual Float64,
            is_outlier UInt8,
            outlier_direction String,
            top_factor String,
            shap_explanation String,
            scored_at DateTime DEFAULT now()
        )
        ENGINE = MergeTree
        ORDER BY project_id
    """)

    # Clear previous scores before inserting fresh ones (simple full-refresh pattern)
    client.command("TRUNCATE TABLE undp.impact_scores")

    insert_df = df[[
        "project_id", "project_name", "sector", "region_name",
        "total_funding_usd", "total_beneficiaries",
        "beneficiaries_per_million_usd", "predicted_efficiency", "residual",
        "is_outlier", "outlier_direction", "top_factor", "shap_explanation",
    ]].rename(columns={"beneficiaries_per_million_usd": "actual_efficiency"})

    insert_df["is_outlier"] = insert_df["is_outlier"].astype(int)

    client.insert_df("undp.impact_scores", insert_df)


def main():
    print("Connecting to ClickHouse...")
    client = clickhouse_connect.get_client(**CLICKHOUSE_CONFIG)

    print("Fetching project data...")
    df = fetch_project_data(client)
    print(f"Loaded {len(df)} projects.")

    print("Training model and computing scores + SHAP explanations...")
    scored_df = train_model_and_score(df)

    outlier_count = scored_df["is_outlier"].sum()
    print(f"Flagged {outlier_count} projects as statistical outliers.")

    print("Writing results to undp.impact_scores...")
    write_scores_to_clickhouse(client, scored_df)

    print("Done. Sample results:")
    print(
        scored_df[[
            "project_id", "sector", "region_name", "beneficiaries_per_million_usd",
            "predicted_efficiency", "is_outlier", "outlier_direction", "top_factor"
        ]].head(10).to_string(index=False)
    )


if __name__ == "__main__":
    main()
