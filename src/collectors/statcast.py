"""Statcast pitch-level data collector using pybaseball."""

import logging
import sqlite3
from datetime import date, datetime, timedelta

import pandas as pd

from src.config import CURRENT_SEASON

logger = logging.getLogger(__name__)

# Pitch types to exclude — pitchouts and unknown
_EXCLUDE_PITCH_TYPES = {'UN', 'PO', 'IN', 'AB'}

# Descriptions that count as a swing
_SWING_DESCRIPTIONS = {
    'swinging_strike',
    'swinging_strike_blocked',
    'foul',
    'foul_tip',
    'foul_bunt',
    'missed_bunt',
    'hit_into_play',
    'hit_into_play_no_out',
    'hit_into_play_score',
}

# Descriptions that count as a whiff (swing and miss)
_WHIFF_DESCRIPTIONS = {
    'swinging_strike',
    'swinging_strike_blocked',
}

# Events that count as a hit
_HIT_EVENTS = {'single', 'double', 'triple', 'home_run'}

# Events that count as a ball in play (for BA computation)
_BIP_EVENTS = {
    'single', 'double', 'triple', 'home_run',
    'field_out', 'grounded_into_double_play', 'force_out',
    'double_play', 'triple_play', 'field_error',
    'fielders_choice', 'fielders_choice_out',
    'sac_fly', 'sac_bunt', 'sac_fly_double_play',
}

# Total bases mapping per event
_TB_MAP = {'single': 1, 'double': 2, 'triple': 3, 'home_run': 4}


def _enable_pybaseball_cache() -> None:
    """Enable pybaseball disk cache (idempotent)."""
    try:
        from pybaseball import cache
        cache.enable()
    except Exception:
        pass  # Cache setup is best-effort


def _load_cluster_model(models_dir: str = 'models'):
    """
    Load the fitted (KMeans, StandardScaler) tuple from models/pitch_clusters.pkl.
    Returns None if the file does not exist yet (first run before clustering).
    """
    import pickle
    from pathlib import Path
    model_path = Path(models_dir) / 'pitch_clusters.pkl'
    if not model_path.exists():
        return None
    with open(model_path, 'rb') as f:
        return pickle.load(f)


def _assign_cluster_to_row(row: dict, kmeans, scaler) -> int | None:
    """
    Assign a pitch cluster to a single pitcher_arsenal row using the fitted model.
    Returns None if any required feature is missing after imputation.
    """
    import numpy as np
    features = [
        row.get('avg_velocity'),
        row.get('avg_pfx_x'),
        row.get('avg_pfx_z'),
        row.get('avg_arm_angle'),
        row.get('avg_release_extension'),
        1.0 if row.get('p_throws') == 'R' else 0.0,
    ]
    # Use cluster centroid medians for NaN imputation — impute with 0 in scaled space
    # (StandardScaler centers features, so 0 in scaled space ≈ median)
    features = [0.0 if (v is None or (isinstance(v, float) and v != v)) else v for v in features]
    X = scaler.transform([features])
    return int(kmeans.predict(X)[0])


class StatcastCollector:
    """
    Collect Statcast pitch-level data and aggregate into:
      - pitcher_arsenal: per-pitcher pitch mix with velocity/movement metrics
      - batter_pitch_type_stats: per-batter performance vs each pitch type
    """

    # Season start/end dates for backfill
    SEASON_DATES = {
        '2021': ('2021-04-01', '2021-10-03'),
        '2022': ('2022-04-07', '2022-10-05'),
        '2023': ('2023-03-30', '2023-10-01'),
        '2024': ('2024-03-20', '2024-09-29'),
        '2025': ('2025-03-27', '2025-09-28'),
        '2026': ('2026-03-26', '2026-10-04'),
    }

    def __init__(self, db_path: str, season: str = None, models_dir: str = 'models'):
        self.db_path = db_path
        self.season = season or CURRENT_SEASON
        _enable_pybaseball_cache()
        cluster_model = _load_cluster_model(models_dir)
        if cluster_model is not None:
            self._kmeans, self._scaler = cluster_model
        else:
            self._kmeans = None
            self._scaler = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def collect_date(self, date_str: str) -> tuple[int, int]:
        """
        Fetch all Statcast pitches for a single date, upsert both tables.

        Returns:
            (pitcher_rows_updated, batter_rows_updated)
        """
        logger.info(f"[statcast] Fetching pitches for {date_str}...")
        df = self._fetch_statcast(date_str, date_str)
        if df.empty:
            logger.info(f"[statcast] No pitches found for {date_str} — skipping")
            return 0, 0
        return self._process_and_upsert(df)

    def collect_date_range(self, start_dt: str, end_dt: str) -> tuple[int, int]:
        """
        Fetch Statcast data for a date range, chunked in 7-day windows.

        Returns:
            (total_pitcher_rows, total_batter_rows)
        """
        start = datetime.strptime(start_dt, '%Y-%m-%d').date()
        end = datetime.strptime(end_dt, '%Y-%m-%d').date()

        total_p, total_b = 0, 0
        chunk_start = start
        while chunk_start <= end:
            chunk_end = min(chunk_start + timedelta(days=6), end)
            logger.info(f"[statcast] Fetching {chunk_start} → {chunk_end}...")
            try:
                df = self._fetch_statcast(
                    chunk_start.strftime('%Y-%m-%d'),
                    chunk_end.strftime('%Y-%m-%d'),
                )
                if not df.empty:
                    p, b = self._process_and_upsert(df)
                    total_p += p
                    total_b += b
                    logger.info(f"[statcast] Chunk done — {p} pitcher rows, {b} batter rows")
            except Exception as e:
                logger.warning(f"[statcast] Chunk {chunk_start}→{chunk_end} failed: {e}")
            chunk_start = chunk_end + timedelta(days=1)

        return total_p, total_b

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _fetch_statcast(self, start_dt: str, end_dt: str) -> pd.DataFrame:
        """Fetch raw Statcast pitch data via pybaseball."""
        from pybaseball import statcast
        try:
            df = statcast(start_dt=start_dt, end_dt=end_dt, verbose=False)
        except Exception as e:
            logger.error(f"[statcast] pybaseball.statcast() failed: {e}")
            return pd.DataFrame()

        if df is None or df.empty:
            return pd.DataFrame()

        # Filter out excluded pitch types
        if 'pitch_type' in df.columns:
            df = df[~df['pitch_type'].isin(_EXCLUDE_PITCH_TYPES)]
            df = df[df['pitch_type'].notna()]

        return df

    def _process_and_upsert(self, df: pd.DataFrame) -> tuple[int, int]:
        """Aggregate raw pitch data and upsert into both DB tables."""
        pitcher_agg = self._aggregate_pitcher_arsenal(df)
        batter_agg = self._aggregate_batter_pitch_type(df)

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            p_count = self._upsert_pitcher_arsenal(cursor, pitcher_agg)
            b_count = self._upsert_batter_pitch_type(cursor, batter_agg)
            conn.commit()
        finally:
            conn.close()

        return p_count, b_count

    def _aggregate_pitcher_arsenal(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aggregate per-pitch rows into per-pitcher per-pitch-type season stats.
        """
        needed = ['pitcher', 'p_throws', 'pitch_type', 'release_speed',
                  'release_spin_rate', 'pfx_x', 'pfx_z', 'release_extension']
        df = df[[c for c in needed + ['arm_angle'] if c in df.columns]].copy()

        # Convert pfx from feet to inches
        if 'pfx_x' in df.columns:
            df['pfx_x'] = df['pfx_x'] * 12
        if 'pfx_z' in df.columns:
            df['pfx_z'] = df['pfx_z'] * 12

        # Group per pitcher + pitch type
        grp = df.groupby(['pitcher', 'p_throws', 'pitch_type'])

        agg = grp.agg(
            n_pitches=('release_speed', 'count'),
            avg_velocity=('release_speed', 'mean'),
            avg_spin_rate=('release_spin_rate', 'mean'),
            avg_pfx_x=('pfx_x', 'mean'),
            avg_pfx_z=('pfx_z', 'mean'),
            avg_release_extension=('release_extension', 'mean'),
        ).reset_index()

        # arm_angle — only if column exists
        if 'arm_angle' in df.columns:
            arm_avg = df.groupby(['pitcher', 'p_throws', 'pitch_type'])['arm_angle'].mean().reset_index()
            arm_avg.rename(columns={'arm_angle': 'avg_arm_angle'}, inplace=True)
            agg = agg.merge(arm_avg, on=['pitcher', 'p_throws', 'pitch_type'], how='left')
        else:
            agg['avg_arm_angle'] = None

        # Compute usage_pct per pitcher
        total_per_pitcher = agg.groupby('pitcher')['n_pitches'].transform('sum')
        agg['usage_pct'] = agg['n_pitches'] / total_per_pitcher

        agg['season'] = self.season
        agg.rename(columns={'pitcher': 'pitcher_id'}, inplace=True)

        return agg

    def _aggregate_batter_pitch_type(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aggregate per-pitch rows into per-batter per-pitch-type season stats.
        """
        needed = ['batter', 'p_throws', 'pitch_type', 'description', 'events',
                  'estimated_ba_using_speedangle', 'estimated_woba_using_speedangle']
        df = df[[c for c in needed if c in df.columns]].copy()

        # Swing / whiff flags
        df['is_swing'] = df['description'].isin(_SWING_DESCRIPTIONS).astype(int)
        df['is_whiff'] = df['description'].isin(_WHIFF_DESCRIPTIONS).astype(int)
        df['is_bip'] = df['events'].isin(_BIP_EVENTS).astype(int)
        df['is_hit'] = df['events'].isin(_HIT_EVENTS).astype(int)
        df['tb'] = df['events'].map(_TB_MAP).fillna(0)

        grp = df.groupby(['batter', 'p_throws', 'pitch_type'])

        agg = grp.agg(
            n_pitches=('description', 'count'),
            n_swings=('is_swing', 'sum'),
            n_whiffs=('is_whiff', 'sum'),
            n_bip=('is_bip', 'sum'),
            n_hits=('is_hit', 'sum'),
            total_bases=('tb', 'sum'),
        ).reset_index()

        # Derived rates
        agg['whiff_rate'] = agg.apply(
            lambda r: r['n_whiffs'] / r['n_swings'] if r['n_swings'] > 0 else None, axis=1
        )
        agg['swstr_rate'] = agg['n_whiffs'] / agg['n_pitches']
        agg['ba'] = agg.apply(
            lambda r: r['n_hits'] / r['n_bip'] if r['n_bip'] > 0 else None, axis=1
        )
        agg['slg'] = agg.apply(
            lambda r: r['total_bases'] / r['n_bip'] if r['n_bip'] > 0 else None, axis=1
        )

        # Expected stats from Statcast (mean over pitches where available)
        for col, out_col in [
            ('estimated_ba_using_speedangle', 'xba'),
            ('estimated_woba_using_speedangle', 'xwoba'),
        ]:
            if col in df.columns:
                exp = df.groupby(['batter', 'p_throws', 'pitch_type'])[col].mean().reset_index()
                exp.rename(columns={col: out_col}, inplace=True)
                agg = agg.merge(exp, on=['batter', 'p_throws', 'pitch_type'], how='left')
            else:
                agg[out_col] = None

        agg['season'] = self.season
        agg.rename(columns={'batter': 'batter_id'}, inplace=True)

        return agg[['batter_id', 'season', 'pitch_type', 'p_throws',
                     'n_pitches', 'ba', 'slg', 'whiff_rate', 'xba', 'xwoba', 'swstr_rate']]

    def _upsert_pitcher_arsenal(self, cursor: sqlite3.Cursor, df: pd.DataFrame) -> int:
        """INSERT OR REPLACE rows into pitcher_arsenal."""
        rows = []
        for _, r in df.iterrows():
            pitch_cluster = None
            if self._kmeans is not None:
                pitch_cluster = _assign_cluster_to_row(r.to_dict(), self._kmeans, self._scaler)
            rows.append((
                int(r['pitcher_id']),
                self.season,
                r['pitch_type'],
                r['p_throws'],
                int(r['n_pitches']),
                _safe_float(r.get('usage_pct')),
                _safe_float(r.get('avg_velocity')),
                _safe_float(r.get('avg_spin_rate')),
                _safe_float(r.get('avg_pfx_x')),
                _safe_float(r.get('avg_pfx_z')),
                _safe_float(r.get('avg_arm_angle')),
                _safe_float(r.get('avg_release_extension')),
                pitch_cluster,
            ))

        cursor.executemany('''
            INSERT OR REPLACE INTO pitcher_arsenal
                (pitcher_id, season, pitch_type, p_throws, n_pitches, usage_pct,
                 avg_velocity, avg_spin_rate, avg_pfx_x, avg_pfx_z,
                 avg_arm_angle, avg_release_extension, pitch_cluster, last_updated)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ''', rows)

        return len(rows)

    def _upsert_batter_pitch_type(self, cursor: sqlite3.Cursor, df: pd.DataFrame) -> int:
        """INSERT OR REPLACE rows into batter_pitch_type_stats."""
        # Load pitch_type → cluster map from DB (populated by cluster_pitches.py)
        cluster_map = {}
        try:
            for row in cursor.execute("SELECT pitch_type, p_throws, pitch_cluster FROM pitch_type_cluster_map"):
                cluster_map[(row[0], row[1])] = row[2]
        except Exception:
            pass  # Table may not exist yet on first run

        rows = []
        for _, r in df.iterrows():
            pitch_cluster = cluster_map.get((r['pitch_type'], r['p_throws']))
            rows.append((
                int(r['batter_id']),
                self.season,
                r['pitch_type'],
                r['p_throws'],
                int(r['n_pitches']),
                _safe_float(r.get('ba')),
                _safe_float(r.get('slg')),
                _safe_float(r.get('whiff_rate')),
                _safe_float(r.get('xba')),
                _safe_float(r.get('xwoba')),
                _safe_float(r.get('swstr_rate')),
                pitch_cluster,
            ))

        cursor.executemany('''
            INSERT OR REPLACE INTO batter_pitch_type_stats
                (batter_id, season, pitch_type, p_throws, n_pitches,
                 ba, slg, whiff_rate, xba, xwoba, swstr_rate, pitch_cluster, last_updated)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ''', rows)

        return len(rows)


def _safe_float(val) -> float | None:
    """Convert to float, returning None for NaN/None."""
    if val is None:
        return None
    try:
        f = float(val)
        return None if f != f else f  # NaN check
    except (TypeError, ValueError):
        return None
