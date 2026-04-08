"""
Fit a K-means pitch cluster model on Statcast pitcher arsenal data.

Clusters are fit on 2023+ data only (post-pitch-clock era) to ensure cluster
boundaries reflect the current pitching environment. The fitted model is then
applied to all rows (2021+) to assign pitch_cluster values.

Saves:
  - models/pitch_clusters.pkl  — (KMeans, StandardScaler) tuple
  - Updates pitcher_arsenal.pitch_cluster for all rows
  - Updates batter_pitch_type_stats.pitch_cluster via pitch_type_cluster_map
  - Populates pitch_type_cluster_map table

Usage:
  python scripts/cluster_pitches.py [--db data/mlb_stats.db] [--n-clusters 40] [--dry-run]
"""

import argparse
import logging
import pickle
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

# Features used for clustering — all continuous physical pitch characteristics.
# p_throws is encoded as binary (R=1, L=0) so it participates in distance computation.
CLUSTER_FEATURES = [
    'avg_velocity',
    'avg_pfx_x',
    'avg_pfx_z',
    'avg_arm_angle',
    'avg_release_extension',
    'p_throws_encoded',
]

# Only fit clusters on pitch-clock era data
FIT_MIN_SEASON = '2023'


def load_arsenal(conn: sqlite3.Connection, min_season: str = None) -> pd.DataFrame:
    where = f"WHERE season >= '{min_season}'" if min_season else ""
    query = f"""
        SELECT pitcher_id, season, pitch_type, p_throws,
               n_pitches, usage_pct,
               avg_velocity, avg_pfx_x, avg_pfx_z,
               avg_arm_angle, avg_release_extension
        FROM pitcher_arsenal
        {where}
    """
    return pd.read_sql_query(query, conn)


def prepare_features(df: pd.DataFrame) -> np.ndarray:
    """Encode p_throws, impute NaN with median, return feature matrix."""
    df = df.copy()
    df['p_throws_encoded'] = (df['p_throws'] == 'R').astype(float)

    for col in CLUSTER_FEATURES:
        if col not in df.columns:
            df[col] = np.nan
        median = df[col].median()
        df[col] = df[col].fillna(median)

    return df[CLUSTER_FEATURES].values


def fit_clusters(df_fit: pd.DataFrame, n_clusters: int) -> tuple[KMeans, StandardScaler]:
    """Fit StandardScaler + KMeans on the fitting subset (2023+)."""
    X = prepare_features(df_fit)

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    logger.info(f"Fitting K-means with {n_clusters} clusters on {len(X)} rows (seasons >= {FIT_MIN_SEASON})...")
    kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
    kmeans.fit(X_scaled)

    inertia = kmeans.inertia_
    logger.info(f"K-means converged — inertia: {inertia:.1f}")

    return kmeans, scaler


def assign_clusters(df: pd.DataFrame, kmeans: KMeans, scaler: StandardScaler) -> np.ndarray:
    """Assign cluster IDs to any arsenal DataFrame."""
    X = prepare_features(df)
    X_scaled = scaler.transform(X)
    return kmeans.predict(X_scaled)


def build_pitch_type_cluster_map(
    df_all: pd.DataFrame,
    kmeans: KMeans,
    scaler: StandardScaler,
) -> pd.DataFrame:
    """
    For each (pitch_type, p_throws) combination, compute league-average physical
    features and assign to the nearest cluster. This map is used to assign
    batter_pitch_type_stats rows to clusters without raw pitch-level data.
    """
    league_avg = (
        df_all.groupby(['pitch_type', 'p_throws'])
        .agg(
            avg_velocity=('avg_velocity', 'mean'),
            avg_pfx_x=('avg_pfx_x', 'mean'),
            avg_pfx_z=('avg_pfx_z', 'mean'),
            avg_arm_angle=('avg_arm_angle', 'mean'),
            avg_release_extension=('avg_release_extension', 'mean'),
        )
        .reset_index()
    )

    clusters = assign_clusters(league_avg, kmeans, scaler)
    league_avg['pitch_cluster'] = clusters

    return league_avg[['pitch_type', 'p_throws', 'pitch_cluster']]


def update_pitcher_arsenal(conn: sqlite3.Connection, df: pd.DataFrame, clusters: np.ndarray) -> int:
    """Write pitch_cluster back to pitcher_arsenal rows."""
    cursor = conn.cursor()
    rows = [
        (int(c), int(r['pitcher_id']), r['season'], r['pitch_type'])
        for c, (_, r) in zip(clusters, df.iterrows())
    ]
    cursor.executemany(
        "UPDATE pitcher_arsenal SET pitch_cluster = ? WHERE pitcher_id = ? AND season = ? AND pitch_type = ?",
        rows,
    )
    conn.commit()
    return len(rows)


def update_batter_pitch_type_stats(conn: sqlite3.Connection, cluster_map: pd.DataFrame) -> int:
    """
    Write pitch_type_cluster_map to DB, then bulk-update batter_pitch_type_stats
    using that map.
    """
    cursor = conn.cursor()

    # Populate pitch_type_cluster_map
    cursor.execute("DELETE FROM pitch_type_cluster_map")
    cursor.executemany(
        "INSERT INTO pitch_type_cluster_map (pitch_type, p_throws, pitch_cluster) VALUES (?, ?, ?)",
        [(r['pitch_type'], r['p_throws'], int(r['pitch_cluster'])) for _, r in cluster_map.iterrows()],
    )

    # Bulk update batter_pitch_type_stats via the map
    cursor.execute("""
        UPDATE batter_pitch_type_stats
        SET pitch_cluster = (
            SELECT pitch_cluster
            FROM pitch_type_cluster_map
            WHERE pitch_type_cluster_map.pitch_type = batter_pitch_type_stats.pitch_type
              AND pitch_type_cluster_map.p_throws   = batter_pitch_type_stats.p_throws
        )
        WHERE pitch_cluster IS NULL
    """)

    updated = cursor.rowcount
    conn.commit()
    return updated


def print_cluster_summary(df_all: pd.DataFrame, clusters: np.ndarray, n_clusters: int) -> None:
    df_all = df_all.copy()
    df_all['pitch_cluster'] = clusters

    summary = (
        df_all.groupby('pitch_cluster')
        .agg(
            rows=('pitcher_id', 'count'),
            avg_velo=('avg_velocity', 'mean'),
            avg_pfx_x=('avg_pfx_x', 'mean'),
            avg_pfx_z=('avg_pfx_z', 'mean'),
            pct_rhp=('p_throws', lambda x: (x == 'R').mean()),
        )
        .round(1)
    )

    logger.info(f"\nCluster summary ({n_clusters} clusters):")
    logger.info(f"{'Cluster':>8} {'Rows':>6} {'Velo':>6} {'pfx_x':>7} {'pfx_z':>7} {'%RHP':>6}")
    logger.info("-" * 48)
    for cluster_id, row in summary.iterrows():
        logger.info(
            f"{cluster_id:>8} {int(row['rows']):>6} {row['avg_velo']:>6.1f} "
            f"{row['avg_pfx_x']:>7.1f} {row['avg_pfx_z']:>7.1f} {row['pct_rhp']:>6.0%}"
        )


def main():
    parser = argparse.ArgumentParser(description="Fit pitch clusters and write to DB")
    parser.add_argument('--db', default='data/mlb_stats.db', help='Path to SQLite DB')
    parser.add_argument('--n-clusters', type=int, default=40, help='Number of K-means clusters')
    parser.add_argument('--dry-run', action='store_true', help='Fit and print summary without writing to DB')
    args = parser.parse_args()

    models_dir = Path('models')
    models_dir.mkdir(exist_ok=True)
    cluster_model_path = models_dir / 'pitch_clusters.pkl'

    conn = sqlite3.connect(args.db)

    # Load all arsenal rows (for assignment) and 2023+ subset (for fitting)
    logger.info("Loading pitcher_arsenal...")
    df_all = load_arsenal(conn)
    df_fit = df_all[df_all['season'] >= FIT_MIN_SEASON].copy()

    if df_fit.empty:
        logger.error(f"No arsenal data for seasons >= {FIT_MIN_SEASON}. Run backfill first.")
        conn.close()
        return

    if df_all.empty:
        logger.error("No arsenal data found.")
        conn.close()
        return

    logger.info(f"Fit subset: {len(df_fit)} rows (seasons >= {FIT_MIN_SEASON})")
    logger.info(f"Full dataset: {len(df_all)} rows (all seasons)")

    # Fit
    kmeans, scaler = fit_clusters(df_fit, args.n_clusters)

    # Assign clusters to all rows
    clusters_all = assign_clusters(df_all, kmeans, scaler)

    # Build pitch_type → cluster map
    cluster_map = build_pitch_type_cluster_map(df_all, kmeans, scaler)

    print_cluster_summary(df_all, clusters_all, args.n_clusters)

    if args.dry_run:
        logger.info("\n[dry-run] No DB writes performed.")
        conn.close()
        return

    # Save model
    with open(cluster_model_path, 'wb') as f:
        pickle.dump((kmeans, scaler), f)
    logger.info(f"\nSaved cluster model to {cluster_model_path}")

    # Write to DB
    p_updated = update_pitcher_arsenal(conn, df_all, clusters_all)
    logger.info(f"Updated {p_updated} pitcher_arsenal rows with pitch_cluster")

    b_updated = update_batter_pitch_type_stats(conn, cluster_map)
    logger.info(f"Updated {b_updated} batter_pitch_type_stats rows with pitch_cluster")
    logger.info(f"Wrote {len(cluster_map)} rows to pitch_type_cluster_map")

    conn.close()
    logger.info("Done.")


if __name__ == '__main__':
    main()
