import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path

main_file_path = '/Users/johnathonsmith/Downloads/'
supporting_files_path = '/Users/johnathonsmith/Downloads/nurse_staffing_supporting_data/'

def load_csv_with_fallback(file_path, encodings=None, na_values=None):
    """
    Load a CSV using a sequence of possible encodings and normalize missing values.
    """
    if encodings is None:
        encodings = ["utf-8", "cp1252", "latin1"]

    if na_values is None:
        na_values = ["", " ", "NA", "N/A", "NULL", "null", "-", ".", "Unknown", "unknown"]

    last_error = None

    for encoding in encodings:
        try:
            df = pd.read_csv(
                file_path,
                encoding=encoding,
                na_values=na_values,
                keep_default_na=True
            )

            # Convert whitespace-only values to NaN
            df = df.replace(r"^\s*$", np.nan, regex=True)

            print(f"Loaded file successfully with encoding: {encoding}")
            return df

        except UnicodeDecodeError as e:
            print(f"Failed to load with encoding '{encoding}': {e}")
            last_error = e
        except pd.errors.EmptyDataError:
            raise ValueError("The CSV file is empty.")
        except pd.errors.ParserError as e:
            raise ValueError(f"Parsing error while reading the CSV: {e}")
        except FileNotFoundError:
            raise FileNotFoundError(f"File not found: {file_path}")
        except Exception as e:
            last_error = e

    raise ValueError(f"Unable to read file with provided encodings. Last error: {last_error}")


def detect_numeric_like_text(series, threshold=0.8):
    """
    Detect whether an object column appears to contain mostly numeric values stored as text.

    Returns True if the ratio of parseable numeric values exceeds the threshold.
    Returns False if detection fails or column is not suitable for evaluation.
    """

    try:
        # Ensure input is a pandas Series
        if not isinstance(series, pd.Series):
            return False

        # Only evaluate object/string columns
        if not pd.api.types.is_object_dtype(series):
            return False

        # Remove null values
        non_null = series.dropna()

        if non_null.empty:
            return False

        # Convert values to string and strip whitespace
        non_null = non_null.astype(str).str.strip()

        # Attempt numeric conversion
        numeric_converted = pd.to_numeric(non_null, errors="coerce")

        # Calculate ratio of numeric-like values
        numeric_ratio = numeric_converted.notna().mean()

        return numeric_ratio >= threshold

    except Exception as e:
        print(f"Warning: Could not evaluate column '{series.name}' for numeric-like text. Error: {e}")
        return False


def analyze_csv_data_quality(
    file_path,
    missing_warning_threshold=0.2,
    plot=True
):
    """
    Analyze CSV data quality and return a column-level summary dataframe.

    Parameters
    ----------
    file_path : str or Path
        Path to the CSV file.
    missing_warning_threshold : float
        Threshold above which missing percentage is flagged.
        Example: 0.2 means 20%.
    plot : bool
        Whether to show a bar chart of missing values by column.

    Returns
    -------
    profile_df : pandas.DataFrame
        Column-level data quality summary.
    numeric_summary : pandas.DataFrame
        Summary statistics for numeric columns.
    """
    try:
        file_path = Path(file_path)
        df = load_csv_with_fallback(file_path)

        total_rows, total_cols = df.shape

        print("\n" + "=" * 60)
        print("DATASET OVERVIEW")
        print("=" * 60)
        print(f"File: {file_path.name}")
        print(f"Rows: {total_rows}")
        print(f"Columns: {total_cols}")

        print("\n" + "=" * 60)
        print("COLUMN NAMES AND DATA TYPES")
        print("=" * 60)
        dtype_df = pd.DataFrame({
            "column_name": df.columns,
            "dtype": df.dtypes.astype(str).values
        })
        print(dtype_df.to_string(index=False))

        duplicate_row_count = df.duplicated().sum()
        duplicate_row_pct = (duplicate_row_count / total_rows * 100) if total_rows > 0 else 0

        print("\n" + "=" * 60)
        print("DATASET-LEVEL QUALITY METRICS")
        print("=" * 60)
        print(f"Duplicate rows: {duplicate_row_count} ({duplicate_row_pct:.2f}%)")

        summary_rows = []

        for col in df.columns:
            series = df[col]

            missing_count = series.isna().sum()
            missing_pct = (missing_count / total_rows) if total_rows > 0 else 0

            non_null_count = series.notna().sum()
            completeness_pct = non_null_count / total_rows if total_rows > 0 else 0

            unique_count = series.nunique(dropna=True)
            constant_column = unique_count <= 1

            inferred_issue = ""
            if detect_numeric_like_text(series):
                inferred_issue = "Possible numeric values stored as text"

            summary_rows.append({
                "column_name": col,
                "dtype": str(series.dtype),
                "row_count": total_rows,
                "non_null_count": non_null_count,
                "missing_count": missing_count,
                "missing_pct": round(missing_pct * 100, 2),
                "completeness_pct": round(completeness_pct * 100, 2),
                "unique_non_null_values": unique_count,
                "constant_column_flag": constant_column,
                "high_missing_flag": missing_pct >= missing_warning_threshold,
                "possible_type_issue": inferred_issue
            })

        profile_df = pd.DataFrame(summary_rows)

        print("\n" + "=" * 60)
        print("COLUMN-LEVEL DATA QUALITY SUMMARY")
        print("=" * 60)
        print(profile_df.to_string(index=False))

        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        if numeric_cols:
            numeric_summary = df[numeric_cols].describe().T.reset_index().rename(columns={"index": "column_name"})
        else:
            numeric_summary = pd.DataFrame()

        print("\n" + "=" * 60)
        print("NUMERIC COLUMN SUMMARY")
        print("=" * 60)
        if not numeric_summary.empty:
            print(numeric_summary.to_string(index=False))
        else:
            print("No numeric columns found.")

        # Optional: simple outlier counts using IQR
        if numeric_cols:
            print("\n" + "=" * 60)
            print("NUMERIC OUTLIER CHECK (IQR METHOD)")
            print("=" * 60)

            outlier_results = []
            for col in numeric_cols:
                s = df[col].dropna()
                if s.empty:
                    outlier_count = 0
                else:
                    q1 = s.quantile(0.25)
                    q3 = s.quantile(0.75)
                    iqr = q3 - q1
                    lower_bound = q1 - 1.5 * iqr
                    upper_bound = q3 + 1.5 * iqr
                    outlier_count = ((s < lower_bound) | (s > upper_bound)).sum()

                outlier_pct = (outlier_count / total_rows * 100) if total_rows > 0 else 0
                outlier_results.append({
                    "column_name": col,
                    "outlier_count": int(outlier_count),
                    "outlier_pct": round(outlier_pct, 2)
                })

            outlier_df = pd.DataFrame(outlier_results)
            print(outlier_df.to_string(index=False))

        if plot:
            print("\n" + "=" * 60)
            print("MISSING VALUE HISTOGRAM")
            print("=" * 60)

            missing_counts = profile_df.set_index("column_name")["missing_count"].sort_values(ascending=False)

            plt.figure(figsize=(12, 6))
            missing_counts.plot(kind="bar")
            plt.title(f"Missing Values by Column: {file_path.name}")
            plt.xlabel("Column")
            plt.ylabel("Missing Value Count")
            plt.xticks(rotation=45, ha="right")
            plt.tight_layout()
            plt.show()

        return df, profile_df, numeric_summary

    except Exception as e:
        print(f"Unexpected error while analyzing '{file_path}': {e}")
        return None, None

def check_impossible_numeric_values(df, column, min_value=0, max_value=None):
    """
    Detect impossible numeric values outside an expected range.
    """

    try:
        if column not in df.columns:
            print(f"Column '{column}' not found.")
            return

        series = pd.to_numeric(df[column], errors="coerce")

        invalid_rows = series < min_value

        if max_value is not None:
            invalid_rows |= series > max_value

        count = invalid_rows.sum()

        print(f"{column}: {count} values outside expected range")

        return df[invalid_rows]

    except Exception as e:
        print(f"Error checking numeric range for '{column}': {e}")

        
def check_date_continuity(df, date_column):
    """
    Identify missing dates in time series data.
    """

    try:
        df[date_column] = pd.to_datetime(df[date_column], errors="coerce")

        min_date = df[date_column].min()
        max_date = df[date_column].max()

        expected_dates = pd.date_range(min_date, max_date)

        missing_dates = expected_dates.difference(df[date_column].dropna().unique())

        print(f"Missing reporting days: {len(missing_dates)}")

        return missing_dates

    except Exception as e:
        print(f"Error checking date continuity: {e}")

        
def check_duplicate_keys(df, key_columns):
    """
    Detect duplicate records based on key columns.
    """

    try:
        duplicates = df[df.duplicated(subset=key_columns, keep=False)]

        print(f"Duplicate key rows: {len(duplicates)}")

        return duplicates

    except Exception as e:
        print(f"Error checking duplicate keys: {e}")