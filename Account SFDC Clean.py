# account_processing.py

import pandas as pd
from difflib import SequenceMatcher
import re

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

def clean_domain(domain):
    """
    Cleans the domain by removing special characters like hyphens.
    
    Parameters:
    domain (str): The domain of the account.

    Returns:
    str: The cleaned domain.
    """
    return re.sub(r'[-_]', '', domain) if isinstance(domain, str) else domain

def domain_similarity(domain1, domain2):
    """
    Calculate similarity between two domains.
    
    Parameters:
    domain1 (str): The first domain.
    domain2 (str): The second domain.

    Returns:
    float: The similarity ratio between the two domains.
    """
    return SequenceMatcher(None, domain1, domain2).ratio()

def fuzzy_name_match(name1, name2):
    """
    Calculate similarity between two account names.
    
    Parameters:
    name1 (str): The first account name.
    name2 (str): The second account name.

    Returns:
    float: The similarity ratio between the two account names.
    """
    return SequenceMatcher(None, name1, name2).ratio()

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
    df['Cleaned Domain'] = df['Domain'].apply(clean_domain)

    # Group by Domain Root to establish Parent-Child relationships
    grouped = df.groupby('Domain Root')

    # Initialize new columns for outcome and parent/merge targets
    df['Outcome'] = 'No Action'
    df['Proposed Parent ID'] = None
    df['Merge Target ID'] = None

    for _, group in grouped:
        if len(group) > 1:
            print(f"Processing group with Domain Root: {group['Domain Root'].iloc[0]}")
            # Assign .com domain (without country suffix) as the parent if available
            parent_row = group[group['Domain'].str.match(r'^.*\.com$')]
            if not parent_row.empty:
                print(f"Parent assigned based on .com domain: {parent_row['Domain'].iloc[0]}")
            if parent_row.empty:
                # Assign USA entity as the parent if no .com domain
                parent_row = group[group['Billing Country'] == 'United States']
                if not parent_row.empty:
                    print(f"Parent assigned based on USA entity: {parent_row['Account Name'].iloc[0]}")
            if parent_row.empty:
                # Assign UK entity if in Europe and no .com or USA entity
                parent_row = group[(group['Billing Country'].isin(['United Kingdom', 'Europe']))]
                if not parent_row.empty:
                    print(f"Parent assigned based on UK/Europe entity: {parent_row['Account Name'].iloc[0]}")
            if parent_row.empty:
                # Use tiebreaker logic based on Total Contacts or Age of Record
                parent_row = group.sort_values(by=['Total Contacts', 'Created Date'], ascending=[False, True]).iloc[:1]
                print(f"Parent assigned based on tiebreaker (Total Contacts or Age): {parent_row['Account Name'].iloc[0]}")
            if parent_row.empty:
                # Use domain similarity as the final tiebreaker, followed by oldest account
                similarity_scores = group['Cleaned Domain'].apply(lambda x: domain_similarity(group['Cleaned Domain'].iloc[0], x))
                most_similar_index = similarity_scores.idxmax()
                parent_row = group.loc[[most_similar_index]]
                print(f"Parent assigned based on domain similarity: {parent_row['Domain'].iloc[0]}")
            if parent_row.empty:
                # As a last resort, choose the oldest account by Created Date
                parent_row = group.sort_values(by=['Created Date']).iloc[:1]
                print(f"Parent assigned as the oldest account by Created Date: {parent_row['Account Name'].iloc[0]}")
            if not parent_row.empty:
                parent_id = parent_row.iloc[0]['Account ID']
                df.loc[group.index, 'Outcome'] = 'Child'
                df.loc[parent_row.index, 'Outcome'] = 'Parent'
                df.loc[group.index.difference(parent_row.index), 'Proposed Parent ID'] = parent_id
                print(f"Final parent for group: {parent_row['Account Name'].iloc[0]}\n")

    # Handling Duplicates, Merges, and Deletions
    # Mark for Merge: Same account name but no domain, with another having a domain
    merge_condition = df['Domain'].isna() & df['Account Name'].notna()
    potential_merge = df.loc[merge_condition]

    # Create a dictionary to quickly look up merge targets by account name using fuzzy matching
    merge_target_dict = {}
    for idx, row in potential_merge.iterrows():
        for account_name, account_id in df[df['Domain'].notna()][['Account Name', 'Account ID']].values:
            if fuzzy_name_match(row['Account Name'], account_name) > 0.8:
                merge_target_dict[row['Account Name']] = account_id
                break

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
