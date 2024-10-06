# account_processing.py

import pandas as pd

def extract_domain_root_and_suffix(domain):
    """
    Extracts the domain root and suffix.
    
    Parameters:
    domain (str): The domain of the account.

    Returns:
    tuple: (root, suffix) of the domain.
    """
    if pd.isna(domain):
        return None, None
    parts = domain.split('.')
    if len(parts) >= 2:
        return parts[-2], parts[-1]
    return None, None

def process_account_relationships(df):
    """
    Processes parent-child relationships, duplicates, merges, and deletions for a list of accounts.
    
    Parameters:
    df (DataFrame): The DataFrame containing account data.

    Returns:
    DataFrame: The updated DataFrame with parent-child relationships and outcome analysis.
    """
    # Apply domain root and suffix extraction
    df['Domain Root'], df['Domain Suffix'] = zip(*df['Domain'].apply(extract_domain_root_and_suffix))

    # Group by Domain Root to establish Parent-Child relationships
    grouped = df.groupby('Domain Root')

    # Initialize new columns for outcome and parent/merge targets
    df['Outcome'] = 'No Action'
    df['Proposed Parent ID'] = None
    df['Merge Target ID'] = None

    for _, group in grouped:
        if len(group) > 1:
            # Assign .com domain (without country suffix) as the parent if available
            parent_row = group[group['Domain'].str.match(r'^.*\.com$')]
            if parent_row.empty:
                # Assign USA entity as the parent if no .com domain
                parent_row = group[group['Billing Country'] == 'United States']
            if parent_row.empty:
                # Assign UK entity if in Europe and no .com or USA entity
                parent_row = group[(group['Billing Country'].isin(['United Kingdom', 'Europe']))]
            if not parent_row.empty:
                parent_id = parent_row.iloc[0]['Account ID']
                df.loc[group.index, 'Outcome'] = 'Child'
                df.loc[parent_row.index, 'Outcome'] = 'Parent'
                df.loc[group.index.difference(parent_row.index), 'Proposed Parent ID'] = parent_id

    # Handling Duplicates, Merges, and Deletions
    # Mark for Merge: Same account name but no domain, with another having a domain
    merge_condition = df['Domain'].isna() & df['Account Name'].notna()
    potential_merge = df.loc[merge_condition]

    # Create a dictionary to quickly look up merge targets by account name
    merge_target_dict = df[df['Domain'].notna()].set_index('Account Name')['Account ID'].to_dict()

    # Apply merge logic using vectorized approach
    df.loc[merge_condition, 'Merge Target ID'] = df.loc[merge_condition, 'Account Name'].map(merge_target_dict)
    df.loc[merge_condition & df['Merge Target ID'].notna(), 'Outcome'] = 'Merge'

    # Mark for Deletion: No domain, no website, no opportunities
    deletion_condition = (
        df['Domain'].isna() &
        df['Website'].isna() &
        (df['# of Closed Opportunities'] == 0) &
        (df['# of Open Opportunities'] == 0)
    )
    matching_accounts = df[df['Domain'].notna()].set_index('Account Name').index
    df.loc[deletion_condition & ~df['Account Name'].isin(matching_accounts), 'Outcome'] = 'Delete'

    return df

def main(input_file, output_file):
    """
    Main function to process accounts from an input CSV file and save the output to another CSV file.
    
    Parameters:
    input_file (str): Path to the input CSV file.
    output_file (str): Path to the output CSV file.
    """
    # Load the CSV file
    df = pd.read_csv(input_file, encoding='latin1')

    # Process the account relationships
    df_processed = process_account_relationships(df)

    # Save the processed DataFrame to a new CSV file
    df_processed.to_csv(output_file, index=False)

if __name__ == "__main__":
    # Example usage
    input_file = "input_accounts.csv"
    output_file = "output_accounts.csv"
    main(input_file, output_file)
