
# Extract the Income Statement
def get_income_statement(db_file, sql_query, subject_id):
    '''
    Extract the income stateemnt out of a PUDL FERC Form 6 SQL file. Available at:
    https://s3.us-west2.amazonaws.com/pudl.catalyst.coop/nightly/ferc2_xbrl.sqlite.zip

    Parameters:
        db_file (object): Path to PUDL FERC Form 6 sqlite database file
        sql_query (string): string of the sql_query
        subject_id (string): the entity_id to filter the database by

    Returns:
        DataFrame (object): A Pandas DataFrame of the filtered Income Statement
    '''
    # Connect to the sql database using sqlite3
    conn = sqlite3.connect(db_file)

    # Query the database file and assign to pandas dataframe
    df = pd.read_sql(sql_query + " WHERE entity_id = ?", conn, params=(subject_id,))
    
    # Close the database connection
    conn.close()

    # Rearange columns to match income statement format
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