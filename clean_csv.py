import argparse
import logging
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(description="Clean a messy CSV and output a Parquet file")
    parser.add_argument("--input", required=True, help="Path to the input CSV file")
    parser.add_argument("--output", required=True, help="Path for the output Parquet file")
    return parser.parse_args()


def read_data(filepath):
    logger.info(f"Reading data from {filepath}")
    df = pd.read_csv(filepath)
    logger.info(f"Loaded {len(df)} rows and {len(df.columns)} columns")
    return df

# Standardise missing values
def standardise_missing(df):
    logger.info("Standardising missing values")
    df = df.replace(r'^\s*$', pd.NA, regex=True)
    df = df.replace(["nan", "NAN", "NaN", "N/A", "n/a", "NA"], pd.NA)
    df.columns = df.columns.str.strip()
    return df

# Convert word-numbers to numeric values
def convert_word_numbers(df):
    logger.info("Converting word-based numbers")
    word_to_num = {
        "thirty": 30,
        "SIXTY THOUSAND": 60000,
    }
    df["Age"] = df["Age"].replace(word_to_num)
    df["Salary"] = df["Salary"].replace(word_to_num)
    df["Age"] = pd.to_numeric(df["Age"], errors="coerce")
    df["Salary"] = pd.to_numeric(df["Salary"], errors="coerce")
    return df

# Parse dates
def parse_dates(df):
    logger.info("Parsing Joining Date column")
    df["Joining Date"] = pd.to_datetime(df["Joining Date"], format="mixed", dayfirst=False)
    return df

# Remove duplicates
def remove_duplicates(df):
    before = len(df)
    df = df.drop_duplicates()
    after = len(df)
    logger.info(f"Removed {before - after} duplicate rows")
    return df

def clean_names(df):
    logger.info("Cleaning Name column")
    df["Name"] = df["Name"].str.strip().str.title()
    return df

def write_parquet(df, filepath):
    logger.info(f"Writing {len(df)} rows to {filepath}")
    df.to_parquet(filepath, index=False)
    logger.info(f"Successfully wrote {filepath}")


def main():
    args = parse_args()
    
    df = read_data(args.input)
    df = standardise_missing(df)
    df = clean_names(df)
    df = convert_word_numbers(df)
    df = parse_dates(df)
    df = remove_duplicates(df)
    write_parquet(df, args.output)
    
    logger.info("Done!")

if __name__ == "__main__":
    main()