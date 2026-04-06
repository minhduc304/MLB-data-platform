"""Evaluation metrics for classifier models."""

import logging
from typing import Dict, Optional

import numpy as np

logger = logging.getLogger(__name__)


def evaluate_classifier(
    y_true: np.ndarray,
    y_proba: np.ndarray,
    stat_type: str = '',
    threshold: float = 0.5,
    kelly_fraction: float = 0.25,
) -> Dict[str, float]:
    """
    Compute classification metrics for over/under prediction.

    Args:
        y_true: Binary labels (1 = hit over, 0 = missed)
        y_proba: Predicted probabilities of hitting over
        stat_type: Name of the stat (for logging)
        threshold: Decision threshold for binary predictions
        kelly_fraction: Fraction of Kelly criterion for ROI calculation

    Returns:
        Dict with accuracy, auc, brier_score, roi, avg_confidence
    """
    from sklearn.metrics import roc_auc_score, log_loss

    y_pred = (y_proba >= threshold).astype(int)

    accuracy = float(np.mean(y_pred == y_true))
    brier = float(np.mean((y_proba - y_true) ** 2))

    try:
        auc = float(roc_auc_score(y_true, y_proba))
    except ValueError:
        auc = 0.5  # Only one class in test set

    try:
        logloss = float(log_loss(y_true, y_proba))
    except ValueError:
        logloss = float('nan')

    # Simulated ROI using fractional Kelly sizing
    # Assumes standard -110 vig (implied odds 52.38%)
    # Bet on over when p_over > threshold, under when p_under > threshold
    roi = _simulate_roi(y_true, y_proba, threshold=threshold, kelly_fraction=kelly_fraction)

    avg_confidence = float(np.mean(np.maximum(y_proba, 1 - y_proba)))

    # Calibration: expected vs actual by decile
    calibration_error = _expected_calibration_error(y_true, y_proba, n_bins=10)

    metrics = {
        'accuracy': round(accuracy, 4),
        'auc': round(auc, 4),
        'brier_score': round(brier, 4),
        'log_loss': round(logloss, 4) if not np.isnan(logloss) else None,
        'roi': round(roi, 4),
        'avg_confidence': round(avg_confidence, 4),
        'ece': round(calibration_error, 4),
    }

    if stat_type:
        logger.info(
            f"[evaluator] {stat_type} classifier — "
            f"Accuracy={accuracy:.3f}, AUC={auc:.3f}, "
            f"Brier={brier:.4f}, ROI={roi:+.2%}, ECE={calibration_error:.4f}"
        )

    return metrics


def _simulate_roi(
    y_true: np.ndarray,
    y_proba: np.ndarray,
    threshold: float = 0.5,
    kelly_fraction: float = 0.25,
    vig_odds: float = -110,
) -> float:
    """
    Simulate betting ROI using fractional Kelly sizing.

    Bets on over when p > threshold, under when (1-p) > threshold.
    Uses standard American -110 odds on both sides.

    Returns:
        ROI as a fraction (e.g., 0.05 = 5% return)
    """
    # Convert -110 to decimal odds
    dec_odds = 100 / abs(vig_odds) + 1  # 1.909...

    total_wagered = 0.0
    total_profit = 0.0

    for p, actual in zip(y_proba, y_true):
        # Bet over if p_over > threshold
        if p > threshold:
            edge = p - (1 / dec_odds)
            if edge > 0:
                kelly = min(edge / (dec_odds - 1), 0.10)  # cap at 10% of bankroll
                bet = kelly * kelly_fraction
                total_wagered += bet
                if actual == 1:
                    total_profit += bet * (dec_odds - 1)
                else:
                    total_profit -= bet

        # Bet under if p_under > threshold
        elif (1 - p) > threshold:
            edge = (1 - p) - (1 / dec_odds)
            if edge > 0:
                kelly = min(edge / (dec_odds - 1), 0.10)
                bet = kelly * kelly_fraction
                total_wagered += bet
                if actual == 0:
                    total_profit += bet * (dec_odds - 1)
                else:
                    total_profit -= bet

    if total_wagered == 0:
        return 0.0

    return total_profit / total_wagered


def _expected_calibration_error(
    y_true: np.ndarray,
    y_proba: np.ndarray,
    n_bins: int = 10,
) -> float:
    """
    Expected Calibration Error (ECE).

    Bins predictions into n_bins equal-width buckets and measures
    |mean_confidence - fraction_positive| weighted by bin size.
    """
    bins = np.linspace(0, 1, n_bins + 1)
    ece = 0.0
    n = len(y_true)

    for i in range(n_bins):
        mask = (y_proba >= bins[i]) & (y_proba < bins[i + 1])
        if mask.sum() == 0:
            continue
        bin_conf = y_proba[mask].mean()
        bin_acc = y_true[mask].mean()
        ece += (mask.sum() / n) * abs(bin_conf - bin_acc)

    return float(ece)


def print_evaluation_report(results: Dict[str, Dict[str, dict]]) -> None:
    """Pretty-print a full training results report."""
    print("\n" + "=" * 70)
    print("MODEL EVALUATION REPORT")
    print("=" * 70)

    for stat_type, models in results.items():
        print(f"\n{stat_type.upper()}")
        print("-" * 40)

        if 'error' in models:
            print(f"  ERROR: {models['error']}")
            continue

        if 'classifier' in models:
            c = models['classifier']
            print(f"  Classifier:")
            print(f"    Accuracy:     {c.get('accuracy', 'N/A'):.3f}")
            print(f"    AUC:          {c.get('auc', 'N/A'):.3f}")
            print(f"    Brier Score:  {c.get('brier_score', 'N/A'):.4f}")
            print(f"    ECE:          {c.get('ece', 'N/A'):.4f}")
            print(f"    Sim ROI:      {c.get('roi', 0):+.2%}")
            print(f"    Test samples: {c.get('n_test', 'N/A')}")

    print("\n" + "=" * 70)
