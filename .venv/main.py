"""
READ THIS! - Before running this script make sure you:
1. Edit 'project_path' and 'client'
2. Copy "Python UserID translation" from latest SAP project to the "Python Translation lists"-folder so it can be edited for the specific client later on
3. Create a file translating the ST03 extract filenames to a system
    - Use column names: "Filename" and "System". Remember to include ".txt" at the end of the filename
    - Save as "Python Filename to system translation {client}.xlsx" in "Python Translation lists"-folder
"""

import os
import sys
import pandas as pd


user = os.environ.get("OneDrive")
project_path = user + r'\Viking - Viking SAP 2025'
client = 'Viking'


def main():
    # Imports transaction data
    activity_data = import_activity_data()

    # Defines user type for each UserID
    activity_data_w_usertype = define_user_type(activity_data)

    # Defines Z-codes in temporary column
    activity_data_w_usertype['Custom code'] = activity_data_w_usertype['tcode'].apply(define_z_codes)

    # Adds system combination
    users_w_system_combination = create_users_system_combination(activity_data_w_usertype)
    activity_data_w_usertype = activity_data_w_usertype.merge(users_w_system_combination,
                                                              how='left',
                                                              on='UserID')

    # Adds modules from library
    activity_data_w_usertype = add_modules_from_library(activity_data_w_usertype)
    activity_data_w_usertype.loc[activity_data_w_usertype[
                                     'Custom code'] == 'Z - Custom', 'module_level_0'] = 'Z - Custom'  # Adds "Z - Custom" to the module_level_0 column for the custom codes
    activity_data_w_usertype = activity_data_w_usertype.drop(columns='Custom code')

    # Output for PBI
    activity_data_w_usertype.to_csv(
        project_path + f'/04 Calculations/Python/Output files/{client}_activity_data_w_usertype.csv', index=False)

    extract_mm_tcodes(activity_data_w_usertype)
    extract_bc_tcodes(activity_data_w_usertype)

    prYellow("Map MM and BC t-codes.\nWhen this is done, continue by entering 'yes' or 'y'. Alternatively, enter 'no' or 'n', and run the script again after mapping.")
    while True:
        proceed = input("\nContinue license mapping (yes/no)? ").strip().lower()
        if proceed in ['yes', 'y', 'no', 'n']:
            break
        else:
            print("Please enter a valid response: 'yes' or 'no'.")

    if proceed in ['no', 'n']:
        print("Script cancelled by user.")
        exit()

    # Adds LoB and corresponding S4 license
    module_to_license_by_lob = pd.read_excel(
        user + f'/Databases/2. SAP/2. Module Database/1. Master data/Python Module to license by assumed LoB - All clients.xlsx')
    activity_data_w_usertype = add_lob_and_s4_license(activity_data_w_usertype, module_to_license_by_lob[
        ['module_level_0', 'Assumed LoB', 'S4 license']])

    # Further license mapping (+ LoB for MM tcodes)
    activity_data_w_license_mapping = tcode_to_license(activity_data_w_usertype)

    # Checks that no rows have been lost in the activity data
    if activity_data.shape[0] != activity_data_w_license_mapping.shape[0]:
        print("NB! The number of rows has changed in activity data. Find error before proceeding")
        sys.exit(3)

    # Assign user to license
    users_w_s4_license_assignment = unique_users_licences_overview(activity_data_w_license_mapping)
    users_w_s4_license_assignment['Assumed license'] = users_w_s4_license_assignment.apply(
        assign_assumed_s4_license, axis=1)

    # Activity for unmapped users
    unmapped_user_license_activity = map_unmapped_user_license_activity(activity_data_w_license_mapping, users_w_s4_license_assignment)

    # Output for PBI
    activity_data_w_license_mapping.to_csv(
        project_path + f'/04 Calculations/Python/Output files/{client}_activity_data_w_license_mapping.csv',
        index=False)

    users_w_s4_license_assignment.to_excel(
        project_path + f'/04 Calculations/Python/Output files/{client}_users_w_s4_license_assignment.xlsx', index=False)

    unmapped_user_license_activity.to_excel(project_path + f'/04 Calculations/Python/Output files/{client}_unmapped_users_activity.xlsx')


def import_activity_data():
    file_to_system_translation = pd.read_excel(project_path + f'/04 Calculations/Python/Python Translation lists/Python Filename to system translation {client}.xlsx')
    file_names = file_to_system_translation['Filename'].to_list()
    systems = file_to_system_translation['System'].to_list()

    folder_path = project_path + '/03 Data collection/Extracts from SAP system/Activity data'
    activity_data = []
    activity_data_folder = os.listdir(folder_path)
    file: str
    for file in activity_data_folder:
        file_path = os.path.join(folder_path, file)
        extract = (pd.read_csv(file_path,
                              sep='\t',
                              names=['UniqueID', 'Period', 'Tasktype', 'tcode', 'UserID', 'Client User Type', 'Executions']).drop(columns='UniqueID'))
        extract['System'] = file_to_system_translation[file_to_system_translation['Filename'] == file]['System'].values[0]
        activity_data.append(extract)

    activity_data = pd.concat(activity_data, ignore_index=True)
    activity_data[['tcode', 'UserID', 'Client User Type']] = activity_data[['tcode', 'UserID', 'Client User Type']].apply(lambda x: x.str.strip())
    activity_data['tcode'].apply(lambda x: x.upper()) # Converts to uppercase to avoid missing values due to case sensitivity
    print(activity_data.head().to_string())

    # UNCOMMENT to filter out data by date if relevant
    #date = [202307, 202308, 202309] # EXAMPLE - change to relevant periods
    #activity_data = activity_data[~activity_data['Period'].isin(date)]

    return activity_data


def define_user_type(activity_data):
    unique_userids = activity_data['UserID'].unique()
    unique_userids = pd.DataFrame(unique_userids, columns=['UserID'])
    unique_userids.to_excel(project_path + '/04 Calculations/Python/Output files/unique_userIDs.xlsx')
    prYellow("Map userIDs that are non-users. You find the unique list of users in the output files. \nSave as Python UserID translation.xlsx in translation list folder.\nWhen this is done, continue by entering 'yes' or 'y'. Alternatively, enter 'no' or 'n', and run the script again after mapping.")
    while True:
        proceed = input("\nContinue license mapping (yes/no)? ").strip().lower()
        if proceed in ['yes', 'y', 'no', 'n']:
            break
        else:
            print("Please enter a valid response: 'yes' or 'no'.")

    if proceed in ['no', 'n']:
        print("Script cancelled by user.")
        exit()

    translation_table = pd.read_excel(project_path + '/04 Calculations/Python/Python Translation lists/Python UserID translation.xlsx')
    keyword_list = translation_table['UserID'].to_list()

    activity_data['User Type'] = activity_data['UserID'].apply(find_user_match, args=(keyword_list, ))

    return activity_data


def find_user_match(cell_value, keyword_list):
    for text in keyword_list:
        if str(text) == str(cell_value).upper(): # upper() changes the text to uppercase to avoid missing values due to case sensitivity
            return 'Non-user'
    return 'User'


def define_z_codes(cell_value):
    z_codes_list = ['Z', '{Z', 'Y'] # TODO: Consider extending to codes starting with "Y"
    for text in z_codes_list:
        if cell_value.startswith(text):
            return 'Z - Custom'
    return ''


def create_users_system_combination(activity_data):
    users = activity_data[activity_data['User Type'] == "User"]
    users = users.groupby('UserID')['System'].apply(lambda x: ', '.join(sorted(set(x)))).reset_index()
    users.rename({'System': 'System Combination'}, axis=1, inplace=True)

    return users


def add_modules_from_library(activity_data):
    tcode_library = pd.read_csv(user + f'/Databases/2. SAP/2. Module Database/1. Master data/master_table_modules.csv')

    tcode_library = tcode_library[
        ['saporg_tcode', 'module_level_0', 'description', 'module_level_1', 'module_level_2', 'module_level_3',
         'module_level_4', 'note']]
    tcode_library = tcode_library.apply(lambda x: x.str.strip())
    activity_data_with_modules = activity_data.merge(tcode_library,
                                                     how='left',
                                                     left_on='tcode',
                                                     right_on='saporg_tcode')
    activity_data_with_modules.drop({'saporg_tcode'}, axis=1, inplace=True)

    return activity_data_with_modules


def extract_mm_tcodes(activity_data):
    activity_data = activity_data[['tcode', 'description', 'module_level_0', 'module_level_1']]
    mm_tcodes = activity_data[activity_data['module_level_0'] == 'Materials Management']
    mm_tcodes = mm_tcodes.groupby(['tcode', 'description', 'module_level_1']).sum().drop(columns='module_level_0').reset_index() # sum() converts it back to a DataFrame from the DataFrameGroupBy

    mm_tcodes_all_clients = pd.read_excel(user + f'/Databases/2. SAP/2. Module Database/1. Master data/Python MM tcodes to license by LoB - All clients.xlsx', )
    mm_tcodes_all_clients = mm_tcodes_all_clients.drop(columns='description')
    mm_tcodes = mm_tcodes.merge(mm_tcodes_all_clients,
                                how='left',
                                on='tcode') # Adds mapping of MM-tcodes that have already been mapped for other clients before outputting the table

    mm_tcodes.to_excel(project_path + f'/04 Calculations/Python/Output files/mm_tcodes_{client}.xlsx', index=False)


def extract_bc_tcodes(activity_data):
    activity_data = activity_data[['tcode', 'description', 'module_level_0', 'module_level_1']]
    bc_tcodes = activity_data[activity_data['module_level_0'] == 'Basis Components']
    bc_tcodes = bc_tcodes.groupby(['tcode', 'description', 'module_level_1']).sum().drop(columns='module_level_0').reset_index() # sum() converts it back to a DataFrame from the DataFrameGroupBy

    bc_tcodes_all_clients = pd.read_excel(user + f'/Databases/2. SAP/2. Module Database/1. Master data/Python BC tcodes to license by LoB - All clients.xlsx')
    bc_tcodes_all_clients = bc_tcodes_all_clients.drop(columns='description')
    bc_tcodes = bc_tcodes.merge(bc_tcodes_all_clients,
                                how='left',
                                on='tcode')  # Adds mapping of BC-tcodes that have already been mapped for other clients before outputting the table
    bc_tcodes = bc_tcodes.drop(columns='module_level_1_y').rename(columns={'module_level_1_x': 'module_level_1'})

    bc_tcodes.to_excel(project_path + f'/04 Calculations/Python/Output files/bc_tcodes_{client}.xlsx', index=False)


def add_lob_and_s4_license(activity_data, module_to_lob):
    activity_data = activity_data.merge(module_to_lob, how='left', on='module_level_0')

    return activity_data


def tcode_to_license(activity_data):
    # Splits dataframe for efficiency
    mm_transactions = activity_data[activity_data['module_level_0'] == 'Materials Management']
    bc_transactions = activity_data[activity_data['module_level_0'] == 'Basis Components']
    other_activity_data = activity_data.loc[~((activity_data['module_level_0'] == 'Materials Management') | (activity_data['module_level_0'] == 'Basis Components'))]

    # Maps Materials Management tcodes to LoB and S4 license
    mm_transactions = map_mm_tcodes(mm_transactions)

    # Maps Basis Components tcodes to LoB and S4 license
    bc_transactions = map_bc_tcodes(bc_transactions)

    # Maps all tcodes (except MM and BC) starting with "Display" to Productivity Use-license. Replaces general mapping from LoB for these tcodes
    # other_activity_data['S4 license'] = other_activity_data.apply(lambda row: find_match_description_to_license(row), axis=1) # OLD method
    mask = other_activity_data['description'].astype(str).str.contains('Display', case=False, na=False, regex=False)
    other_activity_data.loc[mask, 'S4 license'] = 'Productivity Use'

    activity_data = pd.concat([mm_transactions, bc_transactions, other_activity_data])

    activity_data = map_singular_tcodes(activity_data)

    return activity_data


def map_mm_tcodes(mm_transactions):
    mm_tcodes_to_license = pd.read_excel(project_path + f'/04 Calculations/Python/Python Translation lists/Python MM tcodes to license by LoB {client}.xlsx')
    mm_tcodes = mm_tcodes_to_license['tcode'].to_list()
    mm_licenses = mm_tcodes_to_license['Minimum conservative S4 license'].to_list()
    mm_lob = mm_tcodes_to_license['LoB'].to_list()

    mm_transactions[['Assumed LoB', 'S4 license']] = mm_transactions.apply(lambda row: find_match_mm_tcodes_to_license(row, mm_tcodes, mm_licenses, mm_lob), result_type='expand', axis=1)

    return mm_transactions


def map_bc_tcodes(bc_transactions):
    bc_tcodes_to_license = pd.read_excel(project_path + f'/04 Calculations/Python/Python Translation lists/Python BC tcodes to license by LoB {client}.xlsx')
    bc_tcodes = bc_tcodes_to_license['tcode'].to_list()
    bc_licenses = bc_tcodes_to_license['Minimum conservative S4 license'].to_list()
    bc_lob = bc_tcodes_to_license['LoB'].to_list()

    bc_transactions[['Assumed LoB', 'S4 license']] = bc_transactions.apply(lambda row: find_match_bc_tcodes_to_license(row, bc_tcodes, bc_licenses, bc_lob), result_type='expand', axis=1)

    return bc_transactions


def find_match_mm_tcodes_to_license(row, mm_tcodes, mm_licenses, mm_lob):
    for index, tcode in enumerate(mm_tcodes):
        if tcode == row['tcode']:
            return mm_lob[index], mm_licenses[index]

    return row['Assumed LoB'], row['S4 license']  # Returns the license that is already assigned for all tcodes that are not MM


def find_match_bc_tcodes_to_license(row, bc_tcodes, bc_licenses, bc_lob):
    for index, tcode in enumerate(bc_tcodes):
        if tcode == row['tcode']:
            return bc_lob[index], bc_licenses[index]

    return row['Assumed LoB'], row['S4 license']  # Returns the license that is already assigned for all tcodes that are not BC


def find_match_description_to_license(row):
    description = str(row['description']) if pd.notna(row['description']) else ''  # Ensures 'description' is a string to avoid AttributeError
    if description.startswith('Display'):  # For all 'descriptions' starting with 'Display' a Productivity Use license is returned - TODO: consider changing to 'contains "Display"'
        return 'Productivity Use'

    return row['S4 license'] # Returns the license that is already assigned for all descriptions not starting with "Display"


def map_singular_tcodes(activity_data):
    singular_tcodes_to_license = pd.read_excel(project_path + f'/04 Calculations/Python/Python Translation lists/Python Singular tcodes to license {client}.xlsx')
    singular_tcodes = singular_tcodes_to_license['tcode'].to_list()
    singular_tcodes_licenses = singular_tcodes_to_license['Minimum conservative S4 license'].to_list()

    activity_data['S4 license'] = activity_data.apply(lambda row: find_match_singular_tcodes_to_license(row, singular_tcodes, singular_tcodes_licenses), axis=1)

    return activity_data


def find_match_singular_tcodes_to_license(row, singular_tcodes, singular_tcodes_licenses):
    for index, tcode in enumerate(singular_tcodes):
        if tcode == row['tcode']:
            return singular_tcodes_licenses[index]

    return row['S4 license']  # Returns the license that is already assigned for all tcodes that are not in the translation table


def unique_users_licences_overview(all_codes_w_license):
    # Filtering, only relevant to assign license to dialogue users and non-custom codes
    all_codes_w_license_filtered = all_codes_w_license[all_codes_w_license['User Type'] == 'User']
    all_codes_w_license_filtered.loc[(all_codes_w_license_filtered['module_level_0'] == 'Z - Custom'), 'S4 license'] = 'Unmapped - Z-code'
    all_codes_w_license_filtered.loc[all_codes_w_license_filtered['S4 license'] .isna() |(all_codes_w_license_filtered['S4 license'] == ''), 'S4 license'] = 'Unmapped'

    # Groups data
    transactions_aggregated_by_user_and_s4 = all_codes_w_license_filtered.groupby(['UserID', 'S4 license', 'System Combination'], dropna=False).agg({'Executions': 'sum'})  # Aggregating by user and S4 licenses

    # Creates overview of how many transactions each unique user has within each license type requirement
    users_aggregated_with_s4_licenses = transactions_aggregated_by_user_and_s4.pivot_table(index=['UserID', 'System Combination'], columns='S4 license', values='Executions').reset_index()  # Pivoting table

    # Converts column values to integer in order to load data properly into Excel
    cols_s4 = ['Productivity Use', 'Functional Use', 'Professional Use', 'Unmapped', 'Unmapped - Z-code']
    users_aggregated_with_s4_licenses[cols_s4] = users_aggregated_with_s4_licenses[cols_s4].fillna(0)
    users_aggregated_with_s4_licenses[cols_s4] = users_aggregated_with_s4_licenses[cols_s4].astype(int)

    return users_aggregated_with_s4_licenses


def assign_assumed_s4_license(row):
    if not row['Professional Use'] == 0:
        return 'Professional Use'
    elif not row['Functional Use'] == 0:
        return 'Functional Use'
    elif not row['Productivity Use'] == 0:
        return 'Productivity Use'
    elif not row['Unmapped'] == 0:
        return 'Unmapped'
    else:
        return 'Unmapped Z-code'


def map_unmapped_user_license_activity(activity_data_w_license_mapping, users_w_s4_license_assignment):
    users_w_s4_license_assignment = users_w_s4_license_assignment[users_w_s4_license_assignment['Assumed license']=='Unmapped']
    unmapped_users = users_w_s4_license_assignment['UserID'].to_list()

    activity_data_for_unmapped_user_license = activity_data_w_license_mapping[activity_data_w_license_mapping['UserID'].isin(unmapped_users)]

    return activity_data_for_unmapped_user_license



### --------- FORMATTING --------- ###
def prYellow(s): print("\033[93m {}\033[00m".format(s))


BOLD = '\033[1m'
RESET = '\033[0m'



### --------- RUN --------- ###

if __name__ == "__main__":
    main()
