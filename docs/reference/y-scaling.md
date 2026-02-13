# y_scaling Formats

Format: `##(.n)UNIT` where `n` is decimal places (0–3).

| Format | Operation | Example input | `##.2` | `##.1` | `##` |
|--------|-----------|---------------|--------|--------|------|
| `BB` | ÷ 1,000,000,000 | 12,637,800,000 | 12.64B | 12.6B | 13B |
| `MM` | ÷ 1,000,000 | 12,637,800 | 12.64M | 12.6M | 13M |
| `KK` | ÷ 1,000 | 1,263 | 1.26K | 1.3K | 1K |
| `%` | × 100 | 0.0264 | 2.64% | 2.6% | 3% |
| `bps` | × 10,000 | 0.026378 | 263.78bps | 263.8bps | 264bps |

Used in:

- `y_scaling` on `6_12Graph` blocks
- `y_scaling` on `6_WeeksTable` and `12_MonthsTable` rows
