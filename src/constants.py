# SPDX-License-Identifier: Apache-2.0
"""
Named constants for the WBR computation engine.

These constants replace magic numbers throughout the codebase to make the
business logic of Amazon-style Weekly Business Reviews self-documenting.
"""

# ---------------------------------------------------------------------------
# Time periods
# ---------------------------------------------------------------------------
NUM_TRAILING_WEEKS = 6
NUM_TRAILING_MONTHS = 12
MONTHLY_DATA_START_INDEX = 7  # 6 weeks + 1 separator column in the merged frame

# ---------------------------------------------------------------------------
# Year-over-year offsets
# ---------------------------------------------------------------------------
PY_WEEKLY_OFFSET_DAYS = 364  # 52 weeks exactly — preserves weekday alignment
SIX_WEEKS_LOOKBACK_DAYS = 41  # 6 * 7 - 1

# ---------------------------------------------------------------------------
# Comparison scaling
# ---------------------------------------------------------------------------
BPS_MULTIPLIER = 10_000  # basis-point metrics: (CY - PY) * 10,000
PCT_MULTIPLIER = 100  # percent-change metrics: ((CY / PY) - 1) * 100

# ---------------------------------------------------------------------------
# Box totals row indices
#
# The box-totals DataFrame has exactly 9 rows.  Each row is a summary
# statistic shown below the 6-12 chart:
#
#   0  LastWk   — most recent full week (CY week 6)
#   1  WOW      — week-over-week change
#   2  YOY      — year-over-year change (weekly)
#   3  MTD      — month-to-date
#   4  YOY      — year-over-year change (MTD)
#   5  QTD      — quarter-to-date
#   6  YOY      — year-over-year change (QTD)
#   7  YTD      — year-to-date
#   8  YOY      — year-over-year change (YTD)
# ---------------------------------------------------------------------------
BOX_IDX_LAST_WK = 0
BOX_IDX_WOW = 1
BOX_IDX_YOY_WK = 2
BOX_IDX_MTD = 3
BOX_IDX_YOY_MTD = 4
BOX_IDX_QTD = 5
BOX_IDX_YOY_QTD = 6
BOX_IDX_YTD = 7
BOX_IDX_YOY_YTD = 8
NUM_BOX_TOTAL_ROWS = 9

# ---------------------------------------------------------------------------
# period_summary row indices
#
# The period_summary DataFrame has 10 rows — one for each data point
# needed to compute all the YOY comparisons in the box totals:
#
#   0  CY week 6       (most recent CY week)
#   1  CY week 5       (prior CY week, for WOW calculation)
#   2  PY week 6       (same week last year, for YOY)
#   3  PY week 5       (prior PY week)
#   4  CY MTD
#   5  PY MTD
#   6  CY QTD
#   7  PY QTD
#   8  CY YTD
#   9  PY YTD
# ---------------------------------------------------------------------------
YOY_IDX_CY_WK6 = 0
YOY_IDX_CY_WK5 = 1
YOY_IDX_PY_WK6 = 2
YOY_IDX_PY_WK5 = 3
YOY_IDX_CY_MTD = 4
YOY_IDX_PY_MTD = 5
YOY_IDX_CY_QTD = 6
YOY_IDX_PY_QTD = 7
YOY_IDX_CY_YTD = 8
YOY_IDX_PY_YTD = 9

WEEKS_PER_YEAR = 52
