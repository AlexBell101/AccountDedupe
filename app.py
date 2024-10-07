import streamlit as st
import pandas as pd
from difflib import SequenceMatcher
import time

# Title of the Streamlit app
st.title('Account Deduplication and Relationship Management')

# File uploader for users to upload their CSV file
uploaded_file = st.file_uploader("Upload Account CSV", type=["csv"])

if uploaded_file is not None:
    # Allow user to specify column names for important fields
    st.sidebar.header("Column Mapping")
    df = pd.read_csv(uploaded_file)
    columns = df.columns.tolist()

    account_id_col = st.sidebar.selectbox("Select Account ID Column", options=columns, index=columns.index('Account ID') if 'Account ID' in columns else 0)
    account_name_col = st.sidebar.selectbox("Select Account Name Column", options=columns, index=columns.index('Account Name') if 'Account Name' in columns else 0)
    domain_col = st.sidebar.selectbox("Select Domain Column", options=columns, index=columns.index('Domain') if 'Domain' in columns else 0)
    website_col = st.sidebar.selectbox("Select Website Column", options=columns, index=columns.index('Website') if 'Website' in columns else 0)
    created_date_col = st.sidebar.selectbox("Select Created Date Column", options=columns, index=columns.index('Created Date') if 'Created Date' in columns else 0)
    closed_opps_col = st.sidebar.selectbox("Select Closed Opportunities Column", options=columns, index=columns.index('# of Closed Opportunities') if '# of Closed Opportunities' in columns else 0)
    open_opps_col = st.sidebar.selectbox("Select Open Opportunities Column", options=columns, index=columns.index('# of Open Opportunities') if '# of Open Opportunities' in columns else 0)
    contacts_col = st.sidebar.selectbox("Select Total Contacts Column", options=columns, index=columns.index('Total Contacts') if 'Total Contacts' in columns else 0)

    # Add a button to start the initial process
    if st.button("Start Processing"):
        # Show status message
        with st.spinner('Processing... Please wait...'):
            time.sleep(1)  # Simulating delay

            # Extract domain root and suffix
            def extract_domain_root_and_suffix(domain):
                if pd.isna(domain):
                    return None, None
                parts = domain.split('.')
                if len(parts) >= 2:
                    return parts[0], '.'.join(parts[1:])
                return None, None

            # Apply domain extraction to the dataframe
            df['Root Domain'], df['Domain Suffix'] = zip(*df[domain_col].apply(extract_domain_root_and_suffix))

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

                    if parent_row.empty:
                        # Use tiebreaker logic based on Total Contacts or Age of Record
                        parent_row = group.sort_values(by=[contacts_col, created_date_col], ascending=[False, True]).iloc[:1]
                    
                    if not parent_row.empty:
                        parent_id = parent_row.iloc[0][account_id_col]
                        df.loc[group.index, 'Outcome'] = 'Child'
                        df.loc[parent_row.index, 'Outcome'] = 'Parent'
                        df.loc[group.index.difference(parent_row.index), 'Proposed Parent ID'] = parent_id

            # Merge logic: Same account name but no domain, with another having a domain
            merge_condition = df[domain_col].isna() & df[account_name_col].notna()
            potential_merge = df.loc[merge_condition]
            domain_accounts = df[df[domain_col].notna() & df[account_name_col].notna()]

            # Apply merge logic
            df['Merge Target ID'] = None
            for idx, row in potential_merge.iterrows():
                # Exact match logic
                match = domain_accounts[domain_accounts[account_name_col] == row[account_name_col]]
                if not match.empty:
                    df.at[idx, 'Merge Target ID'] = match.iloc[0][account_id_col]
                    df.at[idx, 'Outcome'] = 'Merge'

            # Mark for Deletion: No domain, no website, no opportunities
            deletion_condition = (
                df[domain_col].isna() &
                df[website_col].isna() &
                (df[closed_opps_col] == 0) &
                (df[open_opps_col] == 0)
            )
            df.loc[deletion_condition, 'Outcome'] = 'Delete'

            # Display the updated dataframe
            st.success("Processing complete!")
            st.write("Processed Accounts:")
            st.dataframe(df)

            # Provide an option to download the processed dataframe
            @st.cache_data
            def convert_df_to_csv(df):
                return df.to_csv(index=False).encode('utf-8')

            csv = convert_df_to_csv(df)
            st.download_button(label="Download Processed Accounts CSV", data=csv, file_name='processed_accounts.csv', mime='text/csv')

            # Provide an option to run fuzzy matching as a separate step
            if st.button("Run Fuzzy Matching on Accounts with No Domain", key='fuzzy_matching'):
                with st.spinner('Running fuzzy matching... Please wait...'):
                    time.sleep(1)  # Simulating delay

                    # Create a dictionary to quickly look up merge targets by account name using fuzzy match
                    fuzzy_merge_target_dict = {}
                    for idx, row in potential_merge.iterrows():
                        # Fuzzy match logic to find similar account names
                        best_match = None
                        best_ratio = 0.8  # Set a threshold for fuzzy matching
                        for _, domain_row in domain_accounts.iterrows():
                            ratio = SequenceMatcher(None, row[account_name_col], domain_row[account_name_col]).ratio()
                            if ratio > best_ratio:
                                best_match = domain_row
                                best_ratio = ratio
                        if best_match is not None:
                            fuzzy_merge_target_dict[row[account_id_col]] = best_match[account_id_col]

                    # Apply fuzzy merge logic
                    for account_id, merge_target_id in fuzzy_merge_target_dict.items():
                        df.loc[df[account_id_col] == account_id, 'Merge Target ID'] = merge_target_id
                        df.loc[df[account_id_col] == account_id, 'Outcome'] = 'Merge'

                    st.success("Fuzzy matching complete!")
                    st.write("Updated Accounts after Fuzzy Matching:")
                    st.dataframe(df)

                    # Provide an option to download the updated dataframe
                    csv = convert_df_to_csv(df)
                    st.download_button(label="Download Updated Accounts CSV", data=csv, file_name='updated_accounts.csv', mime='text/csv')
