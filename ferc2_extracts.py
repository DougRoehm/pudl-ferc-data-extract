'''
This file contains functions to extract FERC Form 2 schedules and statements from a 
PUDL ferc2_xbrl.sqlite database.
'''

import sqlite3
import pandas as pd


# Extract the Statement of Income
def get_ferc2_statement_of_income(db_file, subject_id):
    '''
    Extract the Statement of Income out of a PUDL FERC Form 2 SQLite file.
    See the SQLite datasette section at:
    https://catalystcoop-pudl.readthedocs.io/en/latest/data_access.html
    
    Parameters:
        db_file (object): Path to PUDL FERC Form 2 sqlite database file
        subject_id (string): the entity_id to filter the database by

    Returns:
        DataFrame (object): A Pandas DataFrame of the filtered Statement of Income
    '''
    # Connect to the sql database using sqlite3
    conn = sqlite3.connect(db_file)

    # Query the database file and assign to pandas dataframe
    sql_table = 'statement_of_income_114_duration'
    sql_query = f'SELECT * FROM {sql_table}'
    df = pd.read_sql(sql_query + " WHERE entity_id = ?", conn, params=(subject_id,))
    
    # Close the database connection
    conn.close()

    # Rearrange columns to match Statement of Income order
    statement_of_income_items = (
        'entity_id',
        'filing_name',
        'publication_time',
        'utility_type_axis',
        'start_date',
        'end_date',
        'operating_revenues',
        'operation_expense',
        'maintenance_expense',
        'depreciation_expense',
        'depreciation_expense_for_asset_retirement_costs',
        'amortization_and_depletion_of_utility_plant',
        'amortization_of_gas_plant_acquisition_adjustments',
        'amortization_of_property_losses_unrecovered_plant_and_regulatory_study_costs',
        'amortization_of_conversion_expenses',
        'regulatory_debits',
        'regulatory_credits',
        'taxes_other_than_income_taxes_utility_operating_income',
        'income_taxes_utility_operating_income',
        'income_taxes_utility_operating_income_other',
        'provisions_for_deferred_income_taxes_utility_operating_income',
        'provision_for_deferred_income_taxes_credit_utility_operating_income',
        'investment_tax_credit_adjustments',
        'gains_from_disposition_of_plant',
        'losses_from_disposition_of_utility_plant',
        'gains_from_disposition_of_allowances',
        'losses_from_disposition_of_allowances',
        'accretion_expense',
        'utility_operating_expenses',
        'net_utility_operating_income',
        'revenues_from_merchandising_jobbing_and_contract_work',
        'costs_and_expenses_of_merchandising_jobbing_and_contract_work',
        'revenues_from_nonutility_operations',
        'expenses_of_nonutility_operations',
        'nonoperating_rental_income',
        'equity_in_earnings_of_subsidiary_companies',
        'interest_and_dividend_income',
        'allowance_for_other_funds_used_during_construction',
        'miscellaneous_nonoperating_income',
        'gain_on_disposition_of_property',
        'other_income',
        'loss_on_disposition_of_property',
        'miscellaneous_amortization',
        'donations',
        'life_insurance',
        'penalties',
        'expenditures_for_certain_civic_political_and_related_activities',
        'other_deductions',
        'other_income_deductions',
        'taxes_other_than_income_taxes_other_income_and_deductions',
        'income_taxes_federal',
        'income_taxes_other',
        'provision_for_deferred_income_taxes_other_income_and_deductions',
        'provision_for_deferred_income_taxes_credit_other_income_and_deductions',
        'investment_tax_credit_adjustments_nonutility_operations',
        'investment_tax_credits',
        'taxes_on_other_income_and_deductions',
        'net_other_income_and_deductions',
        'interest_on_long_term_debt',
        'amortization_of_debt_discount_and_expense',
        'amortization_of_loss_on_reacquired_debt',
        'amortization_of_premium_on_debt_credit',
        'amortization_of_gain_on_reacquired_debt_credit',
        'interest_on_debt_to_associated_companies',
        'other_interest_expense',
        'allowance_for_borrowed_funds_used_during_construction_credit',
        'net_interest_charges',
        'income_before_extraordinary_items',
        'extraordinary_income',
        'extraordinary_deductions',
        'net_extraordinary_items',
        'income_taxes_extraordinary_items',
        'extraordinary_items_after_taxes',
        'net_income_loss'
    )
    
    df = df.loc[:, statement_of_income_items]
    
    return df