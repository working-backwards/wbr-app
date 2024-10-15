from datetime import datetime

import pandas as pd

week_ending_date_format = '%d-%b-%Y'


def check_params(config):
    return 'function' not in config and \
        ("aggf" in config and ('column' in config or 'filter' in config))


class WBRValidator:
    def __init__(self, csv, cfg):
        self.daily_df = pd.read_csv(csv, parse_dates=['Date'], thousands=',').sort_values(by='Date')
        self.cfg = cfg

    def validate_yaml(self):
        self.check_week_ending()
        self.validate_aggf()

    def check_week_ending(self):
        """
        Checks the week ending date in the configuration.

        Raises:
            Exception: If the 'week_ending' key is missing in the 'setup' section of the configuration.
            ValueError: If the 'week_ending' date is not in the correct format.
        """
        if 'week_ending' not in self.cfg['setup']:
            raise Exception(f"Error in SETUP section for week_ending at line {self.cfg['setup']['__line__']}")
        try:
            # Validate the week ending date format
            datetime.strptime(self.cfg['setup']['week_ending'], week_ending_date_format)
        except ValueError:
            raise ValueError(f"week_ending is in an invalid format, example of correct format: 25-SEP-2012, at line: "
                             f"{self.cfg['setup']['__line__']}")

    def validate_aggf(self):
        """
        Validates the aggregate function (aggf) configuration for metrics.

        This method checks each metric's configuration for required parameters and validates
        the metric comparison method.

        Raises:
            KeyError: If required parameters for metric configuration are missing or if an invalid comparison method is
             found.
        """
        for metric, config in self.cfg['metrics'].items():
            # Skip the line indicator key
            if metric == '__line__':
                continue

            # Check for required metric config parameters
            if 'function' not in config and (
                    "aggf" not in config or ('column' not in config and 'filter' not in config)):
                raise KeyError(
                    f"One of the required metric config parameters from the list [aggf, column, filter] is missing for"
                    f" the metric {metric} at line: {config['__line__']}")

            # Validate the metric comparison method
            if 'metric_comparison_method' in config and config['metric_comparison_method'] != 'bps':
                raise KeyError(
                    f"Invalid value provided for metric_comparison_method {config['metric_comparison_method']} for"
                    f" the metric {metric} at line: {config['__line__']}")