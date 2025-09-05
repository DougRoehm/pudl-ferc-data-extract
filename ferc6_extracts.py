'''
This file contains functions to extract FERC Form 6 schedules and statements from a 
PUDL ferc6_xbrl.sqlite database.
'''

import sqlite3
import pandas as pd


# Extract the Income Statement
def get_ferc6_income_statement(db_file, subject_id):
    '''
    Extract the Income Statement out of a PUDL FERC Form 6 SQLite file.
    See the SQLite datasette section at:
    https://catalystcoop-pudl.readthedocs.io/en/latest/data_access.html
    
    Parameters:
        db_file (object): Path to PUDL FERC Form 6 sqlite database file
        subject_id (string): the entity_id to filter the database by

    Returns:
        DataFrame (object): A Pandas DataFrame of the filtered Income Statement
    '''
    # Connect to the sql database using sqlite3
    conn = sqlite3.connect(db_file)

    # Query the database file and assign to pandas dataframe
    sql_table = 'income_statement_114_duration'
    sql_query = f'SELECT * FROM {sql_table}'
    df = pd.read_sql(sql_query + " WHERE entity_id = ?", conn, params=(subject_id,))
    
    # Close the database connection
    conn.close()

    # Rearrange columns to match income statement format
    income_statement_items = (
        'entity_id',
        'filing_name',
        'publication_time',
        'start_date',
        'end_date',
        'operating_revenues', 
        'operating_expenses',
        'net_carrier_operating_income',
        'other_income_and_deductions',
        'income_net_from_noncarrier_property',
        'interest_and_dividend_income',
        'miscellaneous_income',
        'unusual_or_infrequent_items_credit',
        'interest_expense',
        'miscellaneous_income_charges',
        'unusual_or_infrequent_items_debit',
        'dividend_income_equity_investments',
        'undistributed_earnings_losses',
        'equity_in_earnings_losses_of_affiliated_companies_including_dividend_income',
        'other_income_and_deductions',
        'ordinary_income_before_federal_income_taxes',
        'federal_income_taxes_on_income_from_continuing_operations',
        'provision_for_deferred_taxes',
        'income_loss_from_continuing_operations',
        'income_loss_from_operations_of_discontinued_segments_less_applicable_income_taxes',
        'gain_loss_from_disposition_of_discontinued_segments_less_applicable_income_taxes',
        'income_loss_from_discontinued_operations',
        'income_loss_before_extraordinary_items',
        'extraordinary_items_net',
        'income_taxes_on_extraordinary_items',
        'provision_for_deferred_taxes_extraordinary_items',
        'extraordinary_items',
        'cumulative_effect_of_changes_in_accounting_principles_less_applicable_income_taxes',
        'extraordinary_items_and_accounting_changes',
        'net_income_loss'
    )
    
    df = df.loc[:, income_statement_items]
    
    return df

# Extract the Balance Sheet
def get_ferc6_balance_sheet(db_file, subject_id):
    '''
    Extract the Balance Sheet out of a PUDL FERC Form 6 SQLite file.
    See the SQLite datasette section at:
    https://catalystcoop-pudl.readthedocs.io/en/latest/data_access.html

    Parameters:
        db_file (object): Path to PUDL FERC Form 6 sqlite database file
        subject_id (string): the entity_id to filter the database by

    Returns:
        DataFrame (object): A Pandas DataFrame of the filtered Balance Sheet
    '''
    # Connect to the sql database using sqlite3
    conn = sqlite3.connect(db_file)

    # Query the database file and assign to pandas dataframe
    sql_table = 'comparative_balance_sheet_110_instant'
    sql_query = f'SELECT * FROM {sql_table}'
    df = pd.read_sql(sql_query + " WHERE entity_id = ?", conn, params=(subject_id,))
    
    # Close the database connection
    conn.close()

    # Rearrange columns to match balance sheet format
    balance_sheet_items = (
        'entity_id',
        'filing_name',
        'publication_time',
        'date',
        'cash',
        'special_deposits',
        'temporary_investments',
        'notes_receivable',
        'receivables_from_affiliated_companies',
        'accounts_receivable',
        'accumulated_provision_for_uncollectible_accounts',
        'interest_and_dividends_receivable',
        'oil_inventory',
        'material_and_supplies',
        'prepayments',
        'other_current_assets',
        'deferred_income_tax_assets',
        'current_assets',
        'investments_in_affiliated_companies_stocks',
        'investment_in_affiliated_companies_bonds',
        'investments_in_affiliated_companies_other_secured_obligations',
        'investments_in_affiliated_companies_unsecured_notes',
        'investments_in_affiliated_companies_investment_advances',
        'investments_in_affiliated_companies_undistributed_earnings_from_certain_investments',
        'other_investments_stocks',
        'other_investments_bonds',
        'other_investments_other_secured_obligations',
        'other_investments_unsecured_notes',
        'other_investments_investment_advances',
        'sinking_and_other_funds',
        'investments_and_special_funds',
        'carrier_property',
        'accrued_depreciation_carrier_property',
        'accrued_amortization_carrier_property',
        'carrier_property_net',
        'operating_oil_supply',
        'noncarrier_property',
        'accrued_depreciation_noncarrier_property',
        'noncarrier_property_net',
        'tangible_property',
        'organization_costs_and_other_intangibles',
        'accrued_amortization_of_intangibles',
        'miscellaneous_other_assets',
        'other_deferred_charges',
        'accumulated_deferred_income_tax_assets',
        'derivative_instrument_assets',
        'derivative_instrument_assets_hedges',
        'other_assets_and_deferred_charges',
        'assets',
        'notes_payable',
        'payables_to_affiliated_companies',
        'accounts_payable',
        'salaries_and_wages_payable',
        'interest_payable',
        'dividends_payable',
        'taxes_payable',
        'long_term_debt_payable_within_one_year',
        'other_current_liabilities',
        'deferred_income_tax_liabilities',
        'current_liabilities',
        'long_term_debt_payable_after_one_year',
        'unamortized_premium_on_long_term_debt',
        'unamortized_discount_on_long_term_debt_debit',
        'other_noncurrent_liabilities',
        'accumulated_deferred_income_tax_liabilities',
        'derivative_instrument_liabilities',
        'derivative_instrument_liabilities_hedges',
        'asset_retirement_obligations',
        'noncurrent_liabilities',
        'liabilities',
        'capital_stock',
        'premiums_on_capital_stock',
        'capital_stock_subscriptions',
        'additional_paid_in_capital',
        'appropriated_retained_income',
        'unappropriated_retained_income_and_equity_in_undistributed_earnings_losses_of_affiliated_company',
        'treasury_stock',
        'accumulated_other_comprehensive_income',
        'stockholders_equity',
        'liabilities_and_stockholders_equity'
    )
    
    df = df.loc[:, balance_sheet_items]
    
    return df

# Extract the Cash Flow Statement
def get_ferc6_cash_flow_statement(db_file, subject_id):
    '''
    Extract the Statement of Cash FLows out of a PUDL FERC Form 6 SQLite file.
    See the SQLite datasette section at:
    https://catalystcoop-pudl.readthedocs.io/en/latest/data_access.html

    Parameters:
        db_file (object): Path to PUDL FERC Form 6 sqlite database file
        subject_id (string): the entity_id to filter the database by

    Returns:
        DataFrame (object): A Pandas DataFrame of the filtered Statement of Cash Flows
    '''
    # Connect to the sql database using sqlite3
    conn = sqlite3.connect(db_file)

    # Query the database file and assign to pandas dataframe
    sql_table = 'statement_of_cash_flows_120_duration'
    sql_query = f'SELECT * FROM {sql_table}'
    df = pd.read_sql(sql_query + " WHERE entity_id = ?", conn, params=(subject_id,))
    
    # Close the database connection
    conn.close()

    # Rearrange columns to match cash flow statement format
    cash_flow_statement_items = (
        'entity_id',
        'filing_name',
        'publication_time',
        'start_date',
        'end_date',
        'net_income_loss',
        'depreciation_and_depletion',
        'amortization',
        'noncash_adjustments_to_cash_flows_from_operating_activities',
        'deferred_income_taxes_net',
        'net_increase_decrease_in_receivables_operating_activities',
        'net_increase_decrease_in_inventory_operating_activities',
        'net_increase_decrease_in_payables_and_accrued_expenses_operating_activities',
        'other_adjustments_to_cash_flows_from_operating_activities',
        'net_cash_provided_by_used_in_operating_activities',
        'gross_additions_to_carrier_property_investment_activities',
        'gross_additions_to_noncarrier_property_investment_activities',
        'other_construction_and_acquisition_of_plant_investment_activities',
        'cash_outflows_for_plant',
        'acquisition_of_other_noncurrent_assets',
        'proceeds_from_disposal_of_noncurrent_assets',
        'investments_in_and_advances_to_associated_and_subsidiary_companies',
        'contributions_and_advances_from_associated_and_subsidiary_companies',
        'disposition_of_investments_in_and_advances_to_associated_and_subsidiary_companies',
        'purchase_of_investment_securities',
        'proceeds_from_sales_of_investment_securities',
        'loans_made_or_purchased',
        'collections_on_loans',
        'net_increase_decrease_in_receivables_investing_activities',
        'net_increase_decrease_in_inventory_investing_activities',
        'net_increase_decrease_in_payables_and_accrued_expenses_investing_activities',
        'other_adjustments_to_cash_flows_from_investment_activities',
        'cash_flows_provided_from_used_in_investment_activities',
        'proceeds_from_issuance_of_long_term_debt_financing_activities',
        'proceeds_from_issuance_of_capital_stock',
        'other_adjustments_by_outside_sources_to_cash_flows_from_financing_activities',
        'other_adjustment_by_short_term_debt_to_cash_flows_from_financing_activities',
        'cash_provided_by_outside_sources',
        'payments_for_retirement_of_long_term_debt_financing_activities',
        'payment_for_retirement_of_capital_stock',
        'other_retirements_of_balances_impacting_cash_flows_from_financing_activities',
        'net_decrease_in_short_term_debt',
        'dividends_on_capital_stock',
        'other_adjustments_to_cash_flows_from_financing_activities',
        'cash_flows_provided_from_used_in_financing_activities',
        'net_increase_decrease_in_cash_and_cash_equivalents')
    
    df = df.loc[:, cash_flow_statement_items]
    
    return df