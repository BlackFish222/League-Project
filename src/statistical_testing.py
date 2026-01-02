import json
import numpy
import pandas as pd
import numpy as np
from scipy import stats
import statsmodels.api as sm
import statsmodels.formula.api as smf

def bin_exposure(df: pd.DataFrame, method="tertiles") -> pd.DataFrame:
    df = df.copy()

    if method == "tertiles":
        df["exposure_bin"] = pd.qcut(
            df['exposure_meta'],
            q = 3,
            labels = ['low', 'medium', 'high']
        )
    elif method == 'quartiles':
        df['exposure_bin'] = pd.qcut(
            df['exposure_meta'],
            q = 4,
            labels = ['q1','q2', 'q3', 'q4']
        )
    else:
         raise ValueError('must be tertiles or quartiles')

    return df

def chi_square_give_up(df: pd.DataFrame) -> dict:
     table = pd.crosstab(df["exposure_bin"], df["gave_up"])
     chi2, p, dof, expected = stats.chi2_contingency(table)
     return{
         "test": "chi_square",
         "chi2": chi2,
         "p_value": p,
         "dof": dof,
         "table": table.to_dict(),
         "expected": expected.tolist()
     }

def performance_test(df: pd.DataFrame) -> dict:
    low = df[df['exposure_bin'] == "low"]['performance_delta']
    high = df[df['exposure_bin'] == "high"]['performance_delta']

    _, p_low = stats.shapiro(low.sample(min(len(low),500)))
    _, p_high = stats.shapiro(high.sample(min(len(high), 500)))

    if p_low > .05 and p_high> .05:
        stat, p = stats.ttest_ind(low, high, equal_var=False)
        test_name = "welch_ttest"
    else:
        stat, p = stats.mannwhitneyu(low, high, alternative="two-sided")
        test_name = "mann_whitney_u"

    effect = (high.mean() - low.mean())

    return{
        "test": test_name,
        "statisitic": stat,
        "p_vaule": p,
        "mean_low": low.mean(),
        "mean_high": high.mean(),
        "effect_size_raw": effect
    }

def run_all(df: pd.DataFrame, bin_method="tertiles") -> dict:
    df = bin_exposure(df, bin_method)

    results = {
        "n_players": int(len(df)),
        "bin_method": bin_method,
        "give_up_test": chi_square_give_up(df),
        "performance_test": performance_test(df)
    }

    return results

if __name__ == "__main__":
    df = pd.read_csv("Data/Processed/player_performance.csv")
    results = run_all(df, bin_method='tertiles')
    out = "Data/Processed/results.json"
    out.write_text(json.dumps(results, indent=2))