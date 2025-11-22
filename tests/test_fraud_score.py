# tests/test_fraud_score.py
import sys, os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from load_data import compute_fraud_score

def test_score_low():
    # amount lower than diag average -> low-ish
    score = compute_fraud_score(1000, 2000, 1)
    assert isinstance(score, int)
    assert 0 <= score <= 100

def test_score_high_ratio():
    # amount much higher than average
    score = compute_fraud_score(20000, 2000, 0)  # same-day adds 10
    assert score >= 80

def test_score_long_stay():
    score = compute_fraud_score(3000, 3000, 40)
    assert score >= 20
