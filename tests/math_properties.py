"""
OniQuant v6.0 — Property-Based Math Tests
============================================
Uses pytest + hypothesis to verify mathematical invariants.

Run: pytest tests/math_properties.py -v
"""

from __future__ import annotations

import numpy as np
import pytest
from hypothesis import given, settings, assume
from hypothesis import strategies as st

from app.quant.alpha_stack import KNNMarketArchitecture, SplineQuantileRegression
from app.services.orchestrator import BayesianArbiter


# ===========================================================================
# 1. kNN Mahalanobis Scale-Invariance
# ===========================================================================

@given(scale=st.floats(min_value=0.1, max_value=100.0))
@settings(max_examples=50)
def test_knn_scale_invariance(scale: float):
    """
    Mahalanobis distance must be scale-invariant:
    doubling all price inputs should NOT change neighbor ranking.

    d(cx, cy) should produce the same neighbor ordering as d(x, y)
    because Σ⁻¹ adjusts for the scale of the covariance.
    """
    rng = np.random.default_rng(42)
    n, d = 50, 3
    features = rng.standard_normal((n, d))
    labels = (rng.random(n) > 0.5).astype(float)
    query = rng.standard_normal(d)

    # Fit on original scale
    knn_orig = KNNMarketArchitecture(k=5)
    knn_orig.fit(features, labels)
    prob_orig = knn_orig.predict_proba(query)

    # Fit on scaled data
    knn_scaled = KNNMarketArchitecture(k=5)
    knn_scaled.fit(features * scale, labels)
    prob_scaled = knn_scaled.predict_proba(query * scale)

    # Probabilities should be identical (same neighbor ranking)
    assert abs(prob_orig - prob_scaled) < 1e-10, (
        f"Scale {scale}: prob_orig={prob_orig}, prob_scaled={prob_scaled}"
    )


# ===========================================================================
# 2. Bayesian Posterior Bounds
# ===========================================================================

@given(
    prior=st.floats(min_value=0.05, max_value=0.95),
    likelihood=st.floats(min_value=0.1, max_value=5.0),
)
@settings(max_examples=200)
def test_posterior_never_exceeds_bounds(prior: float, likelihood: float):
    """
    Posterior probability must always be in (0, 1), regardless of inputs.
    """
    posterior = BayesianArbiter._compute_posterior(prior, likelihood)
    assert 0.0 < posterior < 1.0, f"Posterior {posterior} out of bounds (prior={prior}, L={likelihood})"


@given(
    prior=st.floats(min_value=0.05, max_value=0.95),
)
@settings(max_examples=100)
def test_posterior_equals_prior_at_unit_likelihood(prior: float):
    """
    When likelihood = 1.0 (no evidence), posterior should equal prior.
    """
    posterior = BayesianArbiter._compute_posterior(prior, 1.0)
    assert abs(posterior - prior) < 1e-10, f"Expected {prior}, got {posterior}"


@given(
    prior=st.floats(min_value=0.05, max_value=0.95),
    L1=st.floats(min_value=0.1, max_value=5.0),
    L2=st.floats(min_value=0.1, max_value=5.0),
)
@settings(max_examples=100)
def test_posterior_monotonic_in_likelihood(prior: float, L1: float, L2: float):
    """
    Higher likelihood should produce higher posterior (monotonicity).
    """
    assume(abs(L1 - L2) > 0.01)
    p1 = BayesianArbiter._compute_posterior(prior, L1)
    p2 = BayesianArbiter._compute_posterior(prior, L2)
    if L1 > L2:
        assert p1 > p2, f"L1={L1}>L2={L2} but p1={p1}<=p2={p2}"
    else:
        assert p1 < p2, f"L1={L1}<L2={L2} but p1={p1}>=p2={p2}"


# ===========================================================================
# 3. Spline Quantile Ordering
# ===========================================================================

def test_spline_quantile_ordering():
    """
    The 50th quantile must always sit between q05 and q95.
    q05 ≤ q50 ≤ q95 for all time indices.
    """
    rng = np.random.default_rng(42)
    prices = 100 + np.cumsum(rng.standard_normal(500) * 0.5)

    spline = SplineQuantileRegression(window=50)
    spline.fit(prices)

    # Evaluate at multiple points
    for idx in range(60, len(prices), 10):
        bounds = spline.evaluate(float(idx))
        assert bounds["q05"] <= bounds["q50"], (
            f"idx={idx}: q05={bounds['q05']} > q50={bounds['q50']}"
        )
        assert bounds["q50"] <= bounds["q95"], (
            f"idx={idx}: q50={bounds['q50']} > q95={bounds['q95']}"
        )


def test_spline_band_width_positive():
    """Band width (q95 - q05) must always be non-negative."""
    rng = np.random.default_rng(123)
    prices = 200 + np.cumsum(rng.standard_normal(300) * 1.0)

    spline = SplineQuantileRegression(window=30)
    spline.fit(prices)

    for idx in range(40, len(prices), 5):
        bounds = spline.evaluate(float(idx))
        width = bounds["q95"] - bounds["q05"]
        assert width >= 0, f"idx={idx}: negative band width {width}"
