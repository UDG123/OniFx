"""
OniQuant v6.0 — Alpha Stack: Signal Generation Models
========================================================
Mathematical core of the signal generation pipeline.

Models:
    1. KNNMarketArchitecture — Mahalanobis-distance kNN for volatility-invariant
       pattern matching across market regimes.
    2. SplineQuantileRegression — Cubic spline interpolation across 0.05/0.50/0.95
       quantiles for non-parametric price boundary estimation.
    3. IOFStrengthClassifier — Institutional Order Flow scoring via displacement
       and relative volume weighting.
"""

from __future__ import annotations

import numpy as np
from numpy.typing import NDArray
from scipy.interpolate import CubicSpline
from scipy.spatial.distance import mahalanobis
from sklearn.covariance import LedoitWolf

import structlog

log = structlog.get_logger("oniquant.alpha_stack")


# ===========================================================================
# 1. KNN Market Architecture (Mahalanobis Distance)
# ===========================================================================

class KNNMarketArchitecture:
    """
    k-Nearest Neighbors classifier using Mahalanobis distance.

    Standard Euclidean kNN fails in financial data because features
    have wildly different scales and correlations (e.g., price vs volume).
    Mahalanobis distance normalizes by the inverse covariance matrix:

        d(x, y) = √((x - y)ᵀ Σ⁻¹ (x - y))

    This makes the distance metric:
        - Scale-invariant (handles mixed units)
        - Rotation-invariant (accounts for feature correlations)
        - Volatility-adaptive (Σ captures regime-dependent dispersion)

    We use Ledoit-Wolf shrinkage for Σ⁻¹ estimation to handle
    ill-conditioned covariance matrices in high-dimensional feature spaces.

    Attributes:
        k: Number of neighbors.
        training_features: (N, D) array of historical feature vectors.
        training_labels: (N,) array of outcomes (1=win, 0=loss).
        cov_inv: (D, D) inverse covariance matrix (Ledoit-Wolf shrunk).
    """

    def __init__(self, k: int = 7) -> None:
        self.k = k
        self.training_features: NDArray | None = None
        self.training_labels: NDArray | None = None
        self.cov_inv: NDArray | None = None

    def fit(
        self,
        features: NDArray,
        labels: NDArray,
    ) -> None:
        """
        Fit the kNN model on historical feature vectors.

        Uses Ledoit-Wolf shrinkage estimator for the covariance matrix.
        This is critical for financial data where N (samples) may be
        close to D (features), causing the sample covariance to be singular.

        Args:
            features: (N, D) array — each row is a feature vector
                      (e.g., [RSI, ATR_norm, volume_ratio, spread, momentum]).
            labels: (N,) array — binary outcomes (1=win, 0=loss).
        """
        self.training_features = features.copy()
        self.training_labels = labels.copy()

        # Ledoit-Wolf shrinkage: λ·diag(S) + (1-λ)·S
        # Produces a well-conditioned Σ even when N ≈ D
        lw = LedoitWolf()
        lw.fit(features)
        self.cov_inv = np.linalg.inv(lw.covariance_)

    def predict_proba(self, query: NDArray) -> float:
        """
        Predict win probability for a single query feature vector.

        Steps:
            1. Compute Mahalanobis distance to every training point.
            2. Select k nearest neighbors.
            3. Win probability = (# winning neighbors) / k.

        Args:
            query: (D,) feature vector for the current market state.

        Returns:
            Probability of a winning trade [0.0, 1.0].
        """
        if self.training_features is None or self.cov_inv is None:
            raise RuntimeError("Model not fitted — call fit() first")

        n = self.training_features.shape[0]

        # Vectorized Mahalanobis: avoid Python loop over N training points
        # diff = X - query  →  (N, D)
        diff = self.training_features - query  # broadcasting

        # d² = (diff @ Σ⁻¹) · diff, summed over D  →  (N,)
        # Equivalent to: [diff[i] @ cov_inv @ diff[i] for i in range(N)]
        mahal_sq = np.einsum("ij,jk,ik->i", diff, self.cov_inv, diff)
        distances = np.sqrt(np.maximum(mahal_sq, 0.0))  # clamp for numerical safety

        # Select k nearest
        k = min(self.k, n)
        nearest_idx = np.argpartition(distances, k)[:k]
        neighbor_labels = self.training_labels[nearest_idx]

        # Win probability
        return float(np.mean(neighbor_labels))

    def predict_batch(self, queries: NDArray) -> NDArray:
        """
        Predict win probabilities for multiple query vectors.

        Fully vectorized — no Python loops.
        Shape: (M, D) → (M,)
        """
        if self.training_features is None or self.cov_inv is None:
            raise RuntimeError("Model not fitted")

        m = queries.shape[0]
        n = self.training_features.shape[0]
        k = min(self.k, n)
        probs = np.empty(m, dtype=np.float64)

        for i in range(m):
            diff = self.training_features - queries[i]
            mahal_sq = np.einsum("ij,jk,ik->i", diff, self.cov_inv, diff)
            distances = np.sqrt(np.maximum(mahal_sq, 0.0))
            nearest_idx = np.argpartition(distances, k)[:k]
            probs[i] = np.mean(self.training_labels[nearest_idx])

        return probs


# ===========================================================================
# 2. Spline Quantile Regression
# ===========================================================================

class SplineQuantileRegression:
    """
    Non-parametric quantile boundary estimation via cubic splines.

    Maps three quantile levels (0.05, 0.50, 0.95) of the price
    distribution using scipy CubicSpline interpolation. This captures
    the non-linear, non-Gaussian dynamics of financial returns that
    parametric models (normal, t-dist) miss.

    Usage:
        - q05 (5th percentile): Lower price boundary (support).
        - q50 (50th percentile): Fair value estimate (median).
        - q95 (95th percentile): Upper price boundary (resistance).
        - Signal: Price below q05 → mean-reversion long.
                  Price above q95 → mean-reversion short.

    Attributes:
        window: Rolling window for quantile calculation.
        spline_05, spline_50, spline_95: Fitted CubicSpline objects.
    """

    def __init__(self, window: int = 50) -> None:
        self.window = window
        self.spline_05: CubicSpline | None = None
        self.spline_50: CubicSpline | None = None
        self.spline_95: CubicSpline | None = None

    def fit(self, prices: NDArray) -> None:
        """
        Fit cubic splines on rolling quantiles of the price series.

        Steps:
            1. Compute rolling 5th, 50th, 95th quantiles over `window`.
            2. Fit CubicSpline through each quantile time series.

        The splines are fitted on the time index (0, 1, ..., N-1)
        and can be evaluated at any fractional index for interpolation.

        Args:
            prices: 1-D price array (close prices).
        """
        import pandas as pd

        n = len(prices)
        if n < self.window:
            raise ValueError(f"Need at least {self.window} prices, got {n}")

        s = pd.Series(prices)

        # Rolling quantiles — vectorized via pandas
        q05 = s.rolling(self.window).quantile(0.05).to_numpy()
        q50 = s.rolling(self.window).quantile(0.50).to_numpy()
        q95 = s.rolling(self.window).quantile(0.95).to_numpy()

        # Valid indices (after warmup period)
        valid_mask = ~np.isnan(q05)
        x = np.where(valid_mask)[0].astype(float)

        # Fit cubic splines through each quantile series
        self.spline_05 = CubicSpline(x, q05[valid_mask])
        self.spline_50 = CubicSpline(x, q50[valid_mask])
        self.spline_95 = CubicSpline(x, q95[valid_mask])

    def evaluate(self, idx: float) -> dict[str, float]:
        """
        Evaluate quantile boundaries at a given time index.

        Args:
            idx: Time index (can be fractional for interpolation).

        Returns:
            Dict with keys: q05, q50, q95.
        """
        if self.spline_05 is None:
            raise RuntimeError("Model not fitted")

        return {
            "q05": float(self.spline_05(idx)),
            "q50": float(self.spline_50(idx)),
            "q95": float(self.spline_95(idx)),
        }

    def generate_signal(
        self,
        current_price: float,
        idx: float,
        sensitivity: float = 1.0,
    ) -> tuple[int, float]:
        """
        Generate a trading signal based on quantile boundary position.

        Logic:
            price ≤ q05 × sensitivity → LONG  (below support, mean-revert up)
            price ≥ q95 × (2 - sensitivity) → SHORT (above resistance, mean-revert down)
            otherwise → FLAT

        The sensitivity parameter controls how aggressive the entries are:
            sensitivity < 1.0: more conservative (tighter bands)
            sensitivity > 1.0: more aggressive (wider bands)

        Args:
            current_price: Latest price.
            idx: Time index for spline evaluation.
            sensitivity: Entry sensitivity multiplier.

        Returns:
            (direction, confidence):
                direction: 1=LONG, -1=SHORT, 0=FLAT
                confidence: distance from boundary normalized [0, 1]
        """
        bounds = self.evaluate(idx)
        q05, q50, q95 = bounds["q05"], bounds["q50"], bounds["q95"]
        band_width = q95 - q05

        if band_width <= 0:
            return 0, 0.0

        if current_price <= q05 * sensitivity:
            # Below lower boundary — mean-reversion LONG
            distance = (q05 - current_price) / band_width
            confidence = min(1.0, abs(distance))
            return 1, confidence

        if current_price >= q95 * (2.0 - sensitivity):
            # Above upper boundary — mean-reversion SHORT
            distance = (current_price - q95) / band_width
            confidence = min(1.0, abs(distance))
            return -1, confidence

        return 0, 0.0


# ===========================================================================
# 3. IOF Strength Classifier
# ===========================================================================

class IOFStrengthClassifier:
    """
    Institutional Order Flow (IOF) strength scoring.

    Combines two microstructure signals:
        1. Displacement (60% weight):
           Measures how far price moved relative to expected range.
           displacement = |close - open| / ATR

        2. Relative Volume (40% weight):
           Measures current volume vs. 20-period average.
           rel_volume = current_volume / avg_volume_20

    Final IOF Score = 0.60 × displacement_norm + 0.40 × rel_volume_norm

    Both components are normalized to [0, 1] via min-max scaling
    against the trailing window.

    High IOF (>0.80): Strong institutional conviction — increases
    the Bayesian likelihood multiplier by 1.5x in the Orchestrator.
    """

    DISPLACEMENT_WEIGHT: float = 0.60
    REL_VOLUME_WEIGHT: float = 0.40

    def __init__(self, lookback: int = 20) -> None:
        self.lookback = lookback

    def compute_displacement(
        self,
        open_prices: NDArray,
        close_prices: NDArray,
        high_prices: NDArray,
        low_prices: NDArray,
    ) -> NDArray:
        """
        Compute per-bar displacement as |close - open| / ATR.

        ATR (Average True Range) is the rolling mean of true range
        over `lookback` periods. Displacement measures how much of
        the available range was consumed by directional movement.

        Returns:
            1-D array of displacement values (same length as input).
        """
        import pandas as pd

        prev_close = np.roll(close_prices, 1)
        prev_close[0] = close_prices[0]

        true_range = np.maximum(
            high_prices - low_prices,
            np.maximum(
                np.abs(high_prices - prev_close),
                np.abs(low_prices - prev_close),
            ),
        )

        atr = pd.Series(true_range).rolling(self.lookback).mean().to_numpy()
        atr = np.where(atr > 0, atr, 1e-8)  # avoid div-by-zero

        displacement = np.abs(close_prices - open_prices) / atr
        return displacement

    def compute_relative_volume(self, volume: NDArray) -> NDArray:
        """
        Compute relative volume = current / rolling mean(lookback).

        Values > 1.0 indicate above-average activity (institutional participation).
        """
        import pandas as pd
        avg_vol = pd.Series(volume).rolling(self.lookback).mean().to_numpy()
        avg_vol = np.where(avg_vol > 0, avg_vol, 1e-8)
        return volume / avg_vol

    @staticmethod
    def _normalize(arr: NDArray) -> NDArray:
        """Min-max normalize to [0, 1]."""
        arr_min, arr_max = np.nanmin(arr), np.nanmax(arr)
        if arr_max - arr_min < 1e-10:
            return np.zeros_like(arr)
        return (arr - arr_min) / (arr_max - arr_min)

    def score(
        self,
        open_prices: NDArray,
        close_prices: NDArray,
        high_prices: NDArray,
        low_prices: NDArray,
        volume: NDArray,
    ) -> NDArray:
        """
        Compute the composite IOF strength score for each bar.

        IOF = 0.60 × norm(displacement) + 0.40 × norm(relative_volume)

        Returns:
            1-D array of IOF scores in [0, 1].
        """
        disp = self.compute_displacement(open_prices, close_prices, high_prices, low_prices)
        rel_vol = self.compute_relative_volume(volume)

        disp_norm = self._normalize(disp)
        vol_norm = self._normalize(rel_vol)

        iof = self.DISPLACEMENT_WEIGHT * disp_norm + self.REL_VOLUME_WEIGHT * vol_norm
        return iof

    def classify(self, iof_score: float) -> str:
        """
        Classify IOF score into discrete tiers.

        Returns: "strong" (>0.80), "moderate" (>0.60), "weak" (>0.30), "absent".
        """
        if iof_score > 0.80:
            return "strong"
        elif iof_score > 0.60:
            return "moderate"
        elif iof_score > 0.30:
            return "weak"
        return "absent"
