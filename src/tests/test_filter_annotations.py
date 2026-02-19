import unittest
from datetime import datetime

import pandas as pd

from src.controller_utility import filter_annotations


class MinimalWBR:
    """Lightweight stand-in for WBR with only the attributes filter_events needs."""

    def __init__(self, week_ending: datetime, metric_names: list[str]):
        self.cy_week_ending = week_ending
        self.metrics = pd.DataFrame(columns=metric_names)


class TestFilterAnnotations_DuplicateMetric(unittest.TestCase):
    """WBR-206: Multiple annotations for the same metric must all be retained."""

    def test_multiple_annotations_for_same_metric_are_all_returned(self):
        wbr = MinimalWBR(
            week_ending=datetime(2021, 9, 18),
            metric_names=["Impressions", "Clicks"],
        )

        annotations_df = pd.DataFrame({
            "Date": pd.to_datetime(["2021-09-04", "2021-09-10"]),
            "MetricName": ["Impressions", "Impressions"],
            "EventDescription": ["Website outage", "SEM test  launched"],
        })

        errors = []
        result = filter_annotations(wbr, annotations_df, errors)

        self.assertIn("Impressions", result)
        self.assertEqual(len(result["Impressions"]), 2)

        descriptions = [e["description"] for e in result["Impressions"]]
        self.assertIn("Website outage", descriptions)
        self.assertIn("SEM test  launched", descriptions)

        dates = [e["date"] for e in result["Impressions"]]
        self.assertIn("September 04 2021", dates)
        self.assertIn("September 10 2021", dates)

        self.assertEqual(len(errors), 0)


class TestFilterAnnotations_InvalidMetricAlignment(unittest.TestCase):
    """WBR-207: Filtering invalid metrics must not misalign descriptions and dates."""

    def test_invalid_metric_filtered_without_misaligning_remaining_annotations(self):
        wbr = MinimalWBR(
            week_ending=datetime(2021, 9, 18),
            metric_names=["Impressions", "Clicks"],
        )

        annotations_df = pd.DataFrame({
            "Date": pd.to_datetime(["2021-09-04", "2021-09-06", "2021-09-08"]),
            "MetricName": ["Impressions", "NonExistentMetric", "Clicks"],
            "EventDescription": [
                "SEM budget  increase",
                "Should be filtered out",
                "Website redesign deployed",
            ],
        })

        errors = []
        result = filter_annotations(wbr, annotations_df, errors)

        # NonExistentMetric must not appear in output
        self.assertNotIn("NonExistentMetric", result)

        # Impressions keeps its own description and date
        self.assertEqual(len(result["Impressions"]), 1)
        self.assertEqual(result["Impressions"][0]["description"], "SEM budget  increase")
        self.assertEqual(result["Impressions"][0]["date"], "September 04 2021")

        # Clicks keeps its own description and date — not the removed row's values
        self.assertEqual(len(result["Clicks"]), 1)
        self.assertEqual(result["Clicks"][0]["description"], "Website redesign deployed")
        self.assertEqual(result["Clicks"][0]["date"], "September 08 2021")

        # The error list should mention the invalid metric
        self.assertEqual(len(errors), 1)
        self.assertIn("NonExistentMetric", errors[0])


if __name__ == "__main__":
    unittest.main()
