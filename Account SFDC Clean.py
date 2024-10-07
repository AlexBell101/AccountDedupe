import streamlit as st
import pandas as pd

# Title of the Streamlit app
st.title('Account Deduplication and Relationship Management')

# File uploader for users to upload their CSV file
uploaded_file = st.file_uploader("Upload Account CSV", type=["csv"])

if uploaded_file is not None:
    # Load the uploaded CSV file
    df = pd.read_csv(uploaded_file)

    # Extract domain root and suffix
    def extract_domain_root_and_suffix(domain):
        if pd.isna(domain):
            return None, None
        parts = domain.split('.')
        if len(parts) >= 2:
            return parts[0], '.'.join(parts[1:])
        return None, None

    # Apply domain extraction to the dataframe
    df['Root Domain'], df['Domain Suffix'] = zip(*df['Domain'].apply(extract_domain_root_and_suffix))

    # Initialize outcome columns
    df['Outcome'] = 'No Action'
    df['Proposed Parent ID'] = None
    df['Merge Target ID'] = None

    # Group by Root Domain to ensure strict matching within the same domain family
    grouped = df.groupby('Root Domain')

    # Parent-child relationship logic, strictly using root domain
    for root_domain, group in grouped:
        if len(group) > 1:
            # Prioritize the '.com' version of the root domain as the parent if available
            parent_row = group[group['Domain Suffix'].str.endswith('com')]

            if not parent_row.empty:
                # Assign the '.com' domain as the parent explicitly
                parent_id = parent_row.iloc[0]['Account ID']
                df.loc[group.index, 'Outcome'] = 'Child'
                df.loc[parent_row.index, 'Outcome'] = 'Parent'
                df.loc[group.index.difference(parent_row.index), 'Proposed Parent ID'] = parent_id
            else:
                # If no '.com' domain is available, select the oldest account as the parent
                parent_row = group.sort_values(by=['Created Date']).iloc[:1]
                if not parent_row.empty:
                    parent_id = parent_row.iloc[0]['Account ID']
                    df.loc[group.index, 'Outcome'] = 'Child'
                    df.loc[parent_row.index, 'Outcome'] = 'Parent'
                    df.loc[group.index.difference(parent_row.index), 'Proposed Parent ID'] = parent_id

    # Merge logic: Same account name but no domain, with another having a domain
    merge_condition = df['Domain'].isna() & df['Account Name'].notna()
    potential_merge = df.loc[merge_condition]
    domain_accounts = df[df['Domain'].notna() & df['Account Name'].notna()]

    # Create a dictionary to quickly look up merge targets by account name using exact match
    merge_target_dict = {}
    for idx, row in potential_merge.iterrows():
        match = domain_accounts[domain_accounts['Account Name'] == row['Account Name']]
        if not match.empty:
            merge_target_dict[row['Account ID']] = match.iloc[0]['Account ID']

    # Apply merge logic
    df.loc[merge_condition, 'Merge Target ID'] = df.loc[merge_condition, 'Account ID'].map(merge_target_dict)
    df.loc[merge_condition & df['Merge Target ID'].notna(), 'Outcome'] = 'Merge'

    # Mark for Deletion: No domain, no website, no opportunities
    deletion_condition = (
        df['Domain'].isna() &
        df['Website'].isna() &
        (df['# of Closed Opportunities'] == 0) &
        (df['# of Open Opportunities'] == 0)
    )
    df.loc[deletion_condition, 'Outcome'] = 'Delete'

    # Display the updated dataframe
    st.write("Processed Accounts:")
    st.dataframe(df)

    # Provide an option to download the processed dataframe
    @st.cache_data
    def convert_df_to_csv(df):
        return df.to_csv(index=False).encode('utf-8')

    csv = convert_df_to_csv(df)
    st.download_button(label="Download Processed Accounts CSV", data=csv, file_name='processed_accounts.csv', mime='text/csv')
