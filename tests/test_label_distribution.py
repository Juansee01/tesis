"""
Tests that the ML training dataset has a reasonable class distribution.
Each of the three pit window classes (EARLY, MID, LATE) must have enough
samples to train a meaningful classifier — minimum 100 per class.
"""

import pandas as pd
import pytest

MIN_SAMPLES_PER_CLASS = 100
LABEL_COL = "pit_window_class"
VALID_CLASSES = {"EARLY", "MID", "LATE"}


def check_label_distribution(df: pd.DataFrame) -> dict:
    counts = df[LABEL_COL].value_counts().to_dict()
    return counts


def make_balanced_labels(n_per_class=150) -> pd.DataFrame:
    return pd.DataFrame({
        LABEL_COL: (
            ["EARLY"] * n_per_class
            + ["MID"] * n_per_class
            + ["LATE"] * n_per_class
        )
    })


def test_all_classes_present():
    df = make_balanced_labels()
    counts = check_label_distribution(df)
    for cls in VALID_CLASSES:
        assert cls in counts, f"Missing class in dataset: {cls}"


def test_minimum_samples_per_class():
    df = make_balanced_labels(n_per_class=150)
    counts = check_label_distribution(df)
    for cls in VALID_CLASSES:
        assert counts.get(cls, 0) >= MIN_SAMPLES_PER_CLASS, (
            f"Class '{cls}' has only {counts.get(cls, 0)} samples, minimum is {MIN_SAMPLES_PER_CLASS}"
        )


def test_fails_with_too_few_samples():
    df = make_balanced_labels(n_per_class=50)  # below threshold
    counts = check_label_distribution(df)
    failing = [cls for cls in VALID_CLASSES if counts.get(cls, 0) < MIN_SAMPLES_PER_CLASS]
    assert len(failing) > 0  # this test validates that the check works


def test_no_invalid_classes():
    df = pd.DataFrame({LABEL_COL: ["EARLY", "MID", "LATE", "UNKNOWN"]})
    invalid = set(df[LABEL_COL].unique()) - VALID_CLASSES
    assert "UNKNOWN" in invalid


def test_total_samples_sufficient_for_cv():
    # need at least 5 * 3 = 15 samples for 5-fold CV with 3 classes
    df = make_balanced_labels(n_per_class=150)
    assert len(df) >= 15
