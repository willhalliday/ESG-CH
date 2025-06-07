import pandas as pd
from tqdm import tqdm

# 1. Load and clean enrichment data
basic_df = pd.read_csv(
    'BasicCompanyDataAsOneFile-2025-06-01.csv',
    dtype=str,
    skipinitialspace=True  # strips spaces after delimiters
)
# Strip any extra whitespace in column names
basic_df.columns = basic_df.columns.str.strip()
# Select and rename the key column
basic_df = basic_df[[
    'CompanyNumber',
    'CompanyName',
    'RegAddress.PostCode',
    'CompanyCategory',
    'CompanyStatus'
]].rename(columns={'CompanyNumber': 'company_number'})

# 2. Prepare for chunked processing of the large metrics file
chunk_size = 1_000_000  # adjust to available RAM
unmatched_ids = set()
first = True  # to write header only once

# 3. Process in chunks: join enrichment onto metrics
for chunk in tqdm(
    pd.read_csv('downloads/monthly/all_metrics.csv', dtype=str, chunksize=chunk_size),
    desc='Joining chunks'
):
    merged = chunk.merge(
        basic_df,
        on='company_number',
        how='left',
        indicator=True
    )
    # Track unmatched company_numbers
    left_only = merged.loc[merged['_merge'] == 'left_only', 'company_number']
    unmatched_ids.update(left_only.unique())

    # Drop the merge indicator and append to output CSV
    merged.drop(columns=['_merge']).to_csv(
        'merged_metrics.csv',
        mode='w' if first else 'a',
        index=False,
        header=first
    )
    first = False

# 4. Report unmatched count
print(f'Unique company_numbers with no match: {len(unmatched_ids)}')
