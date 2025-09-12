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


# Extract the Balance Sheet Assets
def get_ferc2_balance_sheet_assets(db_file, subject_id):
    '''
    Extract Balance Sheet assets out of a PUDL FERC Form 2 SQLite file.
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
    sql_table = 'comparative_balance_sheet_assets_and_other_debits_110_instant'
    sql_query = f'SELECT * FROM {sql_table}'
    df = pd.read_sql(sql_query + " WHERE entity_id = ?", conn, params=(subject_id,))
    
    # Close the database connection
    conn.close()

    # Rearrange columns to match Statement of Income order
    balance_sheet_asset_items = (
        'entity_id',
        'filing_name',
        'publication_time',
        'date',
        'other_special_funds',
        'advances_for_gas',
        'accounts_receivable_from_associated_companies',
        'accumulated_provision_for_uncollectible_accounts_credit',
        'unamortized_loss_on_reacquired_debt',
        'extraordinary_property_losses',
        'other_property_and_investments',
        'prepayments',
        'utility_plant_and_construction_work_in_progress',
        'nonutility_property',
        'research_development_and_demonstration_expenditures',
        'preliminary_natural_gas_survey_and_investigation_charges_and_other_preliminary_survey_and_investigation_charges',
        'assets_and_other_debits',
        'unrecovered_purchased_gas_costs',
        'temporary_cash_investments',
        'other_materials_and_supplies',
        'clearing_accounts',
        'other_investments',
        'amortization_fund_federal',
        'accumulated_provision_for_depreciation_and_amortization_of_nonutility_property',
        'utility_plant',
        'sinking_funds',
        'gas_owed_to_system_gas',
        'nuclear_materials_held_for_sale',
        'notes_receivable',
        'system_balancing_gas',
        'gas_stored_in_reservoirs_and_pipelines_noncurrent',
        'other_gas_plant_adjustments',
        'customer_accounts_receivable',
        'derivative_instrument_assets',
        'interest_and_dividends_receivable',
        'accumulated_deferred_income_taxes',
        'deferred_losses_from_disposition_of_utility_plant',
        'investment_in_associated_companies',
        'liquefied_natural_gas_stored_and_held_for_processing',
        'utility_plant_net',
        'other_accounts_receivable',
        'nuclear_fuel',
        'investment_in_subsidiary_companies',
        'fuel_stock',
        'residuals_and_extracted_products',
        'accumulated_provision_for_depreciation_amortization_and_depletion_of_plant_utility',
        'merchandise',
        'utility_plant_and_nuclear_fuel_net',
        'miscellaneous_deferred_debits',
        'construction_work_in_progress',
        'gas_stored_base_gas',
        'derivative_instrument_assets_hedges_long_term',
        'depreciation_fund',
        'working_funds',
        'accrued_utility_revenues',
        'derivative_instrument_assets_hedges',
        'temporary_facilities',
        'special_deposits',
        'other_regulatory_assets',
        'stores_expense_undistributed',
        'fuel_stock_expenses_undistributed',
        'accumulated_provision_for_amortization_of_nuclear_fuel_assemblies',
        'current_and_accrued_assets',
        'nuclear_fuel_net',
        'deferred_debits',
        'allowance_inventory_and_withheld',
        'plant_materials_and_operating_supplies',
        'preliminary_survey_and_investigation_charges',
        'gas_stored_current',
        'rents_receivable',
        'unamortized_debt_expense',
        'noncurrent_portion_of_allowances',
        'miscellaneous_current_and_accrued_assets',
        'unrecovered_plant_and_regulatory_study_costs',
        'cash',
        'derivative_instrument_assets_long_term',
        'notes_receivable_from_associated_companies'
    )
    
    df = df.loc[:, balance_sheet_asset_items]
    
    return df


# Extract the Balance Sheet Liabilities and Equity
def get_ferc2_balance_sheet_liabilities_and_equity(db_file, subject_id):
    '''
    Extract Balance Sheet liabilities and equity out of a PUDL FERC Form 2 SQLite file.
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
    sql_table = 'comparative_balance_sheet_liabilities_and_other_credits_110_instant'
    sql_query = f'SELECT * FROM {sql_table}'
    df = pd.read_sql(sql_query + " WHERE entity_id = ?", conn, params=(subject_id,))
    
    # Close the database connection
    conn.close()

    # Rearrange columns to match Statement of Income order
    balance_sheet_liabilities_and_equity_items = (
        'entity_id',
        'filing_name',
        'publication_time',
        'date',
        'notes_payable_to_associated_companies',
        'advances_from_associated_companies',
        'accumulated_provision_for_pensions_and_benefits',
        'accumulated_provision_for_property_insurance',
        'other_deferred_credits',
        'accounts_payable',
        'unappropriated_undistributed_subsidiary_earnings',
        'discount_on_capital_stock',
        'premium_on_capital_stock',
        'obligations_under_capital_lease_noncurrent',
        'dividends_declared',
        'customer_advances_for_construction',
        'unamortized_premium_on_long_term_debt',
        'other_long_term_debt',
        'current_portion_of_long_term_debt',
        'other_paid_in_capital',
        'proprietary_capital',
        'obligations_under_capital_leases_current',
        'long_term_portion_of_derivative_instrument_liabilities_hedges',
        'reacquired_bonds',
        'accumulated_provision_for_rate_refunds',
        'miscellaneous_current_and_accrued_liabilities',
        'long_term_debt',
        'accounts_payable_to_associated_companies',
        'long_term_portion_of_derivative_instrument_liabilities',
        'accumulated_deferred_income_taxes_accelerated_amortization_property',
        'retained_earnings',
        'unamortized_gain_on_reacquired_debt',
        'accumulated_deferred_income_taxes_other_property',
        'matured_long_term_debt',
        'other_regulatory_liabilities',
        'preferred_stock_issued',
        'bonds',
        'common_stock_issued',
        'stock_liability_for_conversion',
        'accumulated_provision_for_injuries_and_damages',
        'customer_deposits',
        'accumulated_deferred_income_taxes_other',
        'deferred_gains_from_disposition_of_utility_plant',
        'deferred_credits',
        'current_and_accrued_liabilities',
        'accumulated_other_comprehensive_income',
        'taxes_accrued',
        'other_noncurrent_liabilities',
        'derivative_instrument_liabilities_hedges',
        'notes_payable',
        'reacquired_capital_stock',
        'matured_interest',
        'derivatives_instrument_liabilities',
        'capital_stock_expense',
        'capital_stock_subscribed',
        'liabilities_and_other_credits',
        'installments_received_on_capital_stock',
        'accumulated_miscellaneous_operating_provisions',
        'accumulated_deferred_investment_tax_credits',
        'interest_accrued',
        'asset_retirement_obligations',
        'unamortized_discount_on_long_term_debt_debit',
        'tax_collections_payable'
    )
    
    df = df.loc[:, balance_sheet_liabilities_and_equity_items]
    
    return df


# Extract the Statement of Cash Flows
def get_ferc2_statement_of_cash_flows(db_file, subject_id):
    '''
    Extract Statement of Cash FLows out of a PUDL FERC Form 2 SQLite file.
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
    sql_table = 'statement_of_cash_flows_120_duration'
    sql_query = f'SELECT * FROM {sql_table}'
    df = pd.read_sql(sql_query + " WHERE entity_id = ?", conn, params=(subject_id,))
    
    # Close the database connection
    conn.close()

    # Rearrange columns to match Statement of Income order
    cash_flow_items = (
        'entity_id',
        'filing_name',
        'publication_time',
        'start_date',
        'end_date',
        'gross_additions_to_nonutility_plant_investing_activities',
        'net_increase_decrease_in_inventory_investing_activities',
        'gross_additions_to_nuclear_fuel_investing_activities',
        'proceeds_from_sales_of_investment_securities',
        'other_adjustments_to_cash_flows_from_investment_activities',
        'net_increase_decrease_in_payables_and_accrued_expenses_operating_activities',
        'net_increase_decrease_in_other_regulatory_assets_operating_activities',
        'disposition_of_investments_in_and_advances_to_associated_and_subsidiary_companies',
        'net_increase_in_short_term_debt',
        'proceeds_from_issuance_of_preferred_stock_financing_activities',
        'gross_additions_to_utility_plant_less_nuclear_fuel_investing_activities',
        'net_decrease_in_short_term_debt',
        'other_adjustments_to_cash_flows_from_financing_activities',
        'investment_tax_credit_adjustments_net',
        'dividends_on_preferred_stock',
        'payments_for_retirement_of_long_term_debt_financing_activities',
        'net_increase_decrease_in_other_regulatory_liabilities_operating_activities',
        'deferred_income_taxes_net',
        'noncash_adjustments_to_cash_flows_from_operating_activities',
        'net_increase_decrease_in_allowances_inventory_operating_activities',
        'cash_outflows_for_plant',
        'payments_for_retirement_of_common_stock_financing_activities',
        'net_increase_decrease_in_allowances_held_for_speculation_investing_activities',
        'payments_for_retirement_of_preferred_stock_financing_activities',
        'undistributed_earnings_from_subsidiary_companies_operating_activities',
        'net_increase_decrease_in_receivables_investing_activities',
        'net_increase_decrease_in_inventory_operating_activities',
        'proceeds_from_disposal_of_noncurrent_assets',
        'net_increase_decrease_in_payables_and_accrued_expenses_investing_activities',
        'other_adjustments_to_cash_flows_from_operating_activities',
        'net_increase_decrease_in_receivables_operating_activities',
        'cash_flows_provided_from_used_in_investment_activities',
        'proceeds_from_issuance_of_common_stock_financing_activities',
        'cash_provided_by_outside_sources',
        'allowance_for_other_funds_used_during_construction_investing_activities',
        'allowance_for_other_funds_used_during_construction_operating_activities',
        'net_increase_decrease_in_cash_and_cash_equivalents',
        'proceeds_from_issuance_of_long_term_debt_financing_activities',
        'other_construction_and_acquisition_of_plant_investment_activities',
        'other_adjustments_by_outside_sources_to_cash_flows_from_financing_activities',
        'contributions_and_advances_from_associated_and_subsidiary_companies',
        'net_income_loss',
        'other_retirements_of_balances_impacting_cash_flows_from_financing_activities',
        'loans_made_or_purchased',
        'cash_flows_provided_from_used_in_financing_activities',
        'gross_additions_to_common_utility_plant_investing_activities',
        'net_cash_provided_by_used_in_operating_activities',
        'dividends_on_common_stock',
        'acquisition_of_other_noncurrent_assets',
        'collections_on_loans',
        'depreciation_and_depletion',
        'purchase_of_investment_securities',
        'investments_in_and_advances_to_associated_and_subsidiary_companies'
    )
    
    df = df.loc[:, cash_flow_items]
    
    return df