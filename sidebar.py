from __future__ import annotations

import sys
from pathlib import Path
import threading
import asyncio
from collections import Counter
from datetime import timedelta
from decimal import Decimal
from itertools import combinations
from typing import Iterable, Literal
import pickle

# Import with error handling for potentially missing dependencies
try:
    import pandas as pd
    # Try to set copy_on_write, but don't fail if not supported
    try:
        pd.options.mode.copy_on_write = True
    except (AttributeError, TypeError):
        pass  # Older pandas versions don't have this option
except Exception:
    pd = None

try:
    import numpy as np
except Exception:
    np = None

try:
    from dotmap import DotMap
except Exception:
    DotMap = None

try:
    from matplotlib import pyplot as plt
except Exception:
    plt = None

try:
    from pydantic import BaseModel
except Exception:
    BaseModel = None

try:
    import tqdm.asyncio as _tqdm_asyncio
    from tqdm import tqdm
except Exception:
    _tqdm_asyncio = None
    tqdm = None

# OpenAI imports
try:
    from openai import AsyncOpenAI, OpenAI
    ACLIENT = AsyncOpenAI()
    CLIENT = OpenAI()
except Exception:
    ACLIENT = None
    CLIENT = None

from pyxll import xl_macro, create_ctp, xl_app
from PySide6 import QtWidgets, QtCore, QtGui

# ============================================================================
# CONFIGURATION
# ============================================================================

# Absolute paths - root directory where data dir resides
PROJECT_ROOT = Path(r"C:\Users\usa\Documents\steward-view-main")
INPUT_DATA_DIR = PROJECT_ROOT / 'data' / 'input'
OUTPUT_DATA_DIR = PROJECT_ROOT / 'data' / 'output'

# Chart configuration
CATEGORY_MIN = 500
VENDOR_MIN = 500

# Column name mappings
COLUMNS = {
    'Date': 'date',
    'Transaction Date': 'date',
    'Order Date': 'date',
    0: 'date',
    'Description': 'description',
    'Product Name': 'description',
    3: 'description',
    'Amount': 'amount',
    'Total Owed': 'amount',
    1: 'amount'
}

# Row exclusion indicators
ROW_EXCLUSION_INDICATORS = 'Mobile Payment|Beginning balance|EPAY ID'

# Vendor dictionary for known vendors
VENDOR_DICT = {
    'ALLY AUTO': {'vendor': 'Ally Auto', 'category': 'Automotive'},
    'CITY OF DALLAS UTILITIES': {'vendor': 'City of Dallas', 'category': 'Utilities'},
    'GOOGLE STORAGE': {'vendor': 'Google', 'category': 'Subscriptions'},
    'GREYSTAR': {'vendor': 'Greystar', 'category': 'Rent'},
    'NETFLIX': {'vendor': 'Netflix', 'category': 'Subscriptions'},
    'PLANET FITNESS': {'vendor': 'Planet Fitness', 'category': 'Gym & Fitness'},
    'PLNT FITNESS': {'vendor': 'Planet Fitness', 'category': 'Gym & Fitness'},
    'SPOTIFY': {'vendor': 'Spotify', 'category': 'Subscriptions'},
    'TXU ENERGY': {'vendor': 'TXU Energy', 'category': 'Utilities'},
    'WSJ DIGITAL': {'vendor': 'Wall Street Journal', 'category': 'Subscriptions'}
}

# Expense categories
ExpenseCategory = Literal[
    'Automotive',
    'Baby & Child',
    'Books',
    'Clothing & Accessories',
    'Electronics & Accessories',
    'General',
    'Groceries',
    'Gym & Fitness',
    'Health & Personal Care',
    'Home',
    'Office Supplies',
    'Other Food & Beverage',
    'Pet Supplies',
    'Sports & Recreation',
    'Other'
]

# OpenAI configuration
MODEL = 'gpt-5-mini'
SERVICE_TIER = 'priority'

PRODUCT_LABELING_INSTRUCTIONS = '''The user will provide an Amazon 
    product name. Return the most fitting category from the supplied
    JSON schema.'''

PAYMENT_LABELING_INSTRUCTIONS = '''The user will provide a transaction
    description from a bank statement. Return the common short brand 
    name of the counterparty and the most fitting category from the 
    supplied JSON schema.'''

CHAT_INSTRUCTIONS = '''Provide a direct, terse answer to the user's
    questions about the expense data in the supplied file using the 
    Python tool (a.k.a. the Code Interpreter tool) as necessary. Do not
    offer to share created files. Do not mention the supplied file or
    its structure (e.g. its columns); from the user's perspective, 
    these are implementation details.'''

# ============================================================================
# UTILITY CLASSES AND HELPERS
# ============================================================================

# Utility classes - only define if dependencies are available
if _tqdm_asyncio is not None:
    class ProgressBar:
        """A minimal ProgressBar wrapper for asynchronous operations."""
        @staticmethod
        async def gather(*args, **kwargs):
            return await _tqdm_asyncio.tqdm_asyncio.gather(*args, **kwargs)

        @staticmethod
        def as_completed(*args, **kwargs):
            return _tqdm_asyncio.tqdm_asyncio.as_completed(*args, **kwargs)
else:
    class ProgressBar:
        @staticmethod
        async def gather(*args, **kwargs):
            import asyncio
            return await asyncio.gather(*args, **kwargs)
        @staticmethod
        def as_completed(*args, **kwargs):
            import asyncio
            return asyncio.as_completed(*args, **kwargs)

if BaseModel is not None:
    class CategoryResponse(BaseModel):
        category: ExpenseCategory

    class DualResponse(BaseModel):
        vendor: str
        category: ExpenseCategory
else:
    # Fallback classes if pydantic is not available
    class CategoryResponse:
        def __init__(self, category):
            self.category = category
    
    class DualResponse:
        def __init__(self, vendor, category):
            self.vendor = vendor
            self.category = category


def convert_to_decimal(value):
    """Convert value to Decimal with 2 decimal places."""
    cent = Decimal('0.01')
    return Decimal(value).quantize(cent)


# ============================================================================
# DATA LOADING
# ============================================================================

def load_data():
    """
    Load Jack and Jill's checking, credit card, and Amazon data from Excel
    sheets in the active workbook and return a dictionary of DataFrames.
    
    Uses PyXLL's COM access to read data directly from Excel without requiring openpyxl.
    
    Expected sheet names in the active workbook:
    - 'amazon' (header row 0)
    - 'jack_checking' (no header)
    - 'jack_credit_card' (header row 0)
    - 'jill_checking' (header row 0)
    - 'jill_credit_card' (header row 4)
    """
    if pd is None:
        raise ImportError("pandas is required for load_data()")
    
    # Get the active workbook
    app = xl_app()
    wb = app.ActiveWorkbook
    if wb is None:
        raise RuntimeError("No active workbook found. Please open the Excel file first.")
    
    print("\n=== LOADING DATA FROM EXCEL ===")
    
    # Helper function to read Excel range into DataFrame using COM
    def read_sheet_to_dataframe(sheet_name, header_row=None):
        """Read an Excel sheet into a pandas DataFrame using PyXLL COM access."""
        try:
            ws = wb.Worksheets(sheet_name)
        except Exception:
            raise RuntimeError(f"Sheet '{sheet_name}' not found in the active workbook.")
        
        # Get the used range to find the extent of data
        used_range = ws.UsedRange
        if used_range is None:
            return pd.DataFrame()
        
        # Find the last row and column with data
        last_row = used_range.Row + used_range.Rows.Count - 1
        last_col = used_range.Column + used_range.Columns.Count - 1
        
        # Debug: print range being read
        print(f"    Reading {sheet_name}: Excel rows {used_range.Row} to {last_row}, columns {used_range.Column} to {last_col}")
        
        # Always read from row 1 to ensure we capture header rows correctly
        # This matches CSV behavior where we read from the beginning
        # Read from row 1, column 1 to last_row, last_col
        data_range = ws.Range(ws.Cells(1, 1), ws.Cells(last_row, last_col))
        values = data_range.Value
        
        # Convert COM return value to list of lists
        if values is None:
            return pd.DataFrame()
        
        # Normalize COM return value to list of lists
        # COM can return: scalar, 1D list (row/column), or 2D list
        if not isinstance(values, (list, tuple)):
            # Single cell - wrap in list
            data = [[values]]
        elif len(values) == 0:
            # Empty range
            return pd.DataFrame()
        else:
            # Check if first element is a list/tuple (indicating 2D structure)
            first_elem = values[0] if len(values) > 0 else None
            if isinstance(first_elem, (list, tuple)):
                # 2D array - convert tuples to lists if needed
                data = [list(row) if isinstance(row, tuple) else row for row in values]
            else:
                # Single row - wrap in list
                # Convert tuple to list if needed
                data = [list(values) if isinstance(values, tuple) else values]
        
        # Convert to DataFrame
        if header_row is None:
            # No header - use default column names
            df = pd.DataFrame(data)
        else:
            # Has header - use specified row as column names
            # header_row is 0-indexed, so row 0 = Excel row 1, row 4 = Excel row 5
            if len(data) <= header_row:
                raise RuntimeError(f"Sheet '{sheet_name}' has fewer rows than header row {header_row}.")
            # Convert header row to list if it's a tuple
            header = list(data[header_row]) if isinstance(data[header_row], tuple) else data[header_row]
            # Start data from row after header (header_row + 1)
            df = pd.DataFrame(data[header_row + 1:], columns=header)
        
        return df
    
    # Read data from Excel sheets (sheet names match CSV file names without .csv extension)
    # Map: sheet_name -> (data_key, header_row)
    sheet_configs = {
        'amazon': ('amzn', 0),
        'jack_checking': ('jack_ch', None),
        'jack_credit_card': ('jack_cc', 0),
        'jill_checking': ('jill_ch', 0),
        'jill_credit_card': ('jill_cc', 4)
    }
    
    data = {}
    for sheet_name, (data_key, header_row) in sheet_configs.items():
        try:
            df = read_sheet_to_dataframe(sheet_name, header_row)
            data[data_key] = df
            print(f"  Loaded {sheet_name} ({data_key}): {len(df)} rows, {len(df.columns)} columns")
        except Exception as e:
            raise RuntimeError(f"Error reading sheet '{sheet_name}': {str(e)}")
    
    print("=" * 50)
    return data


# ============================================================================
# DATA TAGGING
# ============================================================================

def tag(data):
    """
    Add 'account' and 'spender' columns with the appropriate values.
    """
    for account in data:
        data[account]['account'] = account
        data[account]['spender'] = 'Jack' if 'jack' in account else 'Jill'
    
    return data


# ============================================================================
# DATA CLEANING
# ============================================================================

def rename_columns(data):
    """Rename the necessary columns in each data table."""
    for account in data:
        data[account] = data[account].rename(columns=COLUMNS)
    return data


def filter_amazon(table):
    """
    Isolate Amazon products purchased exclusively with Jill's Visa 
    credit card ending in 1234.
    """
    filter = table['Payment Instrument Type'] == 'Visa - 1234'
    table = table[filter]
    return table


def filter_cc(table):
    """Remove unnecessary rows from both credit card accounts."""
    filter = ~table['description'].str.contains(ROW_EXCLUSION_INDICATORS)
    table = table[filter]
    return table


def filter_rows(data):
    """
    Remove unnecessary rows from both credit card accounts and isolate
    Amazon products purchased exclusively with Jill's Visa credit card
    ending in 1234.
    """
    for account, table in data.items():
        if account == 'amzn':
            data[account] = filter_amazon(table)
        elif 'cc' in account:
            data[account] = filter_cc(table)
    return data


def drop_columns(data):
    """
    Drop all columns except date, amount, description, account, and spender.
    """
    for account, table in data.items():
        data[account] = table[['date', 'amount', 'description', 'account', 'spender']]
    return data


def cast_date_dtype(data, account):
    """
    Cast the dtype of the 'amzn' date column to datetime and all other
    date columns to date.
    
    Handles timezone-aware datetimes from Excel COM by normalizing to UTC
    then removing timezone info to match CSV behavior.
    """
    if account == 'amzn':
        # For Amazon, convert to datetime (timezone-naive)
        # Handle timezone-aware datetimes from Excel COM
        data[account]['date'] = pd.to_datetime(data[account]['date'], utc=True).dt.tz_localize(None)
    else:
        # For other accounts, convert to date (timezone-naive)
        # Handle timezone-aware datetimes from Excel COM
        data[account]['date'] = pd.to_datetime(data[account]['date'], utc=True).dt.tz_localize(None).dt.date
    return data


def cast_amount_dtype(data, account):
    """Cast the amount column of data[account] to Decimal."""
    data[account]['amount'] = data[account]['amount'].apply(convert_to_decimal)
    return data


def cast_dtypes(data):
    """
    Cast the dtype of the 'amzn' date column to datetime, all other
    date columns to date, and all amount columns to Decimal.
    """
    for account in data:
        data = cast_date_dtype(data, account)
        data = cast_amount_dtype(data, account)
    return data


def clean(data):
    """
    Remove unnecessary rows and columns and set the names and dtypes
    of the remaining columns.
    """
    # Debug: print row counts before cleaning
    print("\n=== CLEANING DATA ===")
    for account in data:
        print(f"  {account}: {len(data[account])} rows before clean")
    
    data = rename_columns(data)
    data = filter_rows(data)
    
    # Debug: print row counts after filtering
    print("\nAfter filtering:")
    for account in data:
        print(f"  {account}: {len(data[account])} rows")
    
    data = drop_columns(data)
    data = cast_dtypes(data)
    
    # Debug: print row counts after cleaning
    print("\nAfter cleaning:")
    for account in data:
        print(f"  {account}: {len(data[account])} rows")
    print("=" * 50)
    
    return data


# ============================================================================
# DATA COMBINING - MATCHER CLASS
# ============================================================================

class Order:
    """
    Create a new Order instance for matching Amazon products to payments.
    """
    
    def __init__(self, matcher, order_id):
        self.date = order_id.date()
        self.pmts = DotMap({
            'candidates': self.identify_candidates(matcher),
            'matched': pd.Index([], dtype='int64')
        })
        self.prods = DotMap({
            'unmatched': Order.extract_products(matcher, order_id),
            'matched': pd.Index([], dtype='int64')
        })
        self.counter = Counter({
            'match_all_products': 0,
            'match_single_products': 0,
            'match_product_combos': 0
        })
    
    def match(self):
        """
        Identify the matching payments and products associated with a 
        single Amazon order.
        """
        # Step 1
        if len(self.pmts.candidates) == 0:
            return None
        else:
            self.match_all_products()
        
        # Step 2
        if len(self.prods.unmatched) < 2:
            return None
        else:
            self.match_single_products()
        
        # Step 3
        if len(self.prods.unmatched) < 4:
            return None
        else:
            self.match_product_combos()
    
    def match_all_products(self):
        prods_amt = self.prods.unmatched['amount'].sum()
        for pmt_idx, pmt_amt in self.pmts.candidates['amount'].items():
            if pmt_amt == prods_amt:
                self.record_match(
                    pd.Index([pmt_idx]),
                    self.prods.unmatched.index,
                    'match_all_products'
                )
                break
    
    def match_single_products(self):
        for pmt_idx, pmt_amt in self.pmts.candidates['amount'].items():
            for prod_idx, prod_amt in self.prods.unmatched['amount'].items():
                if prod_amt == pmt_amt:
                    self.record_match(
                        pd.Index([pmt_idx]),
                        pd.Index([prod_idx]),
                        'match_single_products'
                    )
                    if len(self.prods.unmatched) > 1:
                        self.match_all_products()
                    break
            if len(self.prods.unmatched) == 0:
                break
    
    def match_product_combos(self):
        initial_prod_count = len(self.prods.unmatched)
        for combo_length in self.generate_combo_lengths(initial_prod_count):
            self.match_combos_of_length(combo_length)
            if len(self.prods.unmatched) <= combo_length:
                break
    
    @staticmethod
    def generate_combo_lengths(initial_prod_count):
        return range(2, initial_prod_count // 2 + 1)
    
    def match_combos_of_length(self, combo_length):
        for pmt_idx, pmt_amt in self.pmts.candidates['amount'].items():
            for combo in self.generate_combinations(combo_length):
                combo_amt = self.calculate_combo_amount(combo)
                if combo_amt == pmt_amt:
                    self.record_match(
                        pd.Index([pmt_idx]),
                        pd.Index(combo),
                        'match_product_combos'
                    )
                    if len(self.prods.unmatched) >= combo_length:
                        self.match_all_products()
                    break
            if len(self.prods.unmatched) <= combo_length:
                break
    
    def generate_combinations(self, combo_length) -> Iterable[tuple[int]]:
        return combinations(self.prods.unmatched.index, combo_length)
    
    def calculate_combo_amount(self, combo):
        return self.prods.unmatched.loc[list(combo), 'amount'].sum()
    
    def record_match(self, payment_index: pd.Index, product_index: pd.Index, function):
        self.pmts.matched = self.pmts.matched.append(payment_index)
        self.pmts.candidates = self.pmts.candidates.drop(index=payment_index)
        self.prods.matched = self.prods.matched.append(product_index)
        self.prods.unmatched = self.prods.unmatched.drop(index=product_index)
        self.counter[function] += 1
    
    def identify_candidates(self, matcher, max_delay=3) -> pd.DataFrame:
        payments = matcher.pmts.filtered
        filter = payments['date'].between(self.date, self.date + timedelta(days=max_delay))
        return payments[filter]
    
    @staticmethod
    def extract_products(matcher, order_id) -> pd.DataFrame:
        products = matcher.prods.original
        products = products[products['date'] == order_id]
        return products


class Matcher:
    """
    Create a new Matcher instance for matching Amazon payments with products.
    """
    
    def __init__(self, payments, products, path):
        self.pmts = DotMap({
            'original': payments,
            'filtered': payments[payments['description'].str.contains('AMAZON')],
            'matched': pd.Index([], dtype='int64'),
            'unmatched': pd.Index([], dtype='int64')
        })
        
        self.prods = DotMap({
            'original': products,
            'order_ids': products['date'].unique(),
            'matched': pd.Index([], dtype='int64'),
            'unmatched': pd.Index([], dtype='int64')
        })
        
        self.counter = Counter({
            'match_all_products': 0,
            'match_single_products': 0,
            'match_product_combos': 0
        })
        
        self.integrated_data = pd.DataFrame({})
        self.path = path / 'matcher.pkl'
    
    def match(self):
        """
        Replace bank records of Amazon payments with the more detailed 
        product data to enable more meaningful expense classification.
        """
        self.process_orders()
        self.compile_results()
        self.save()
    
    def process_orders(self):
        if tqdm is None:
            # Fallback if tqdm is not available
            for id in self.prods.order_ids:
                order = Order(self, id)
                order.match()
                self.update(order)
        else:
            for id in tqdm(self.prods.order_ids, desc='Matching Amazon Orders', unit='order', bar_format='{l_bar}{bar} {n_fmt}/{total_fmt} [{elapsed}]'):
                order = Order(self, id)
                order.match()
                self.update(order)
    
    def update(self, order):
        self.pmts.matched = self.pmts.matched.append(order.pmts.matched)
        self.prods.matched = self.prods.matched.append(order.prods.matched)
        self.prods.unmatched = self.prods.unmatched.append(order.prods.unmatched.index)
        self.counter += order.counter
    
    def compile_results(self):
        self.integrated_data = self.integrate_data()
        self.pmts.unmatched = self.isolate_unmatched_payments()
    
    def integrate_data(self):
        pmts_minus_matches = self.pmts.original.drop(index=self.pmts.matched)
        matched_prods_to_add = self.prods.original.loc[self.prods.matched]
        integrated_data = pd.concat([pmts_minus_matches, matched_prods_to_add], ignore_index=True)
        # Convert all dates to date objects (timezone-naive)
        # Data should already be timezone-naive from cast_date_dtype(), but handle
        # any potential timezone-aware datetimes that might have slipped through
        # Use utc=True to normalize any timezone-aware datetimes, then remove timezone
        # This ensures all dates are unified to date objects, matching original behavior
        try:
            # Try with utc=True first (handles timezone-aware datetimes)
            integrated_data['date'] = pd.to_datetime(integrated_data['date'], utc=True).dt.tz_localize(None).dt.date
        except (TypeError, ValueError):
            # Fallback: if utc=True fails (e.g., with date objects), convert normally
            # This matches the original behavior exactly
            integrated_data['date'] = pd.to_datetime(integrated_data['date']).dt.date
        return integrated_data
    
    def isolate_unmatched_payments(self):
        return self.pmts.filtered.drop(index=self.pmts.matched).index
    
    def save(self):
        with open(self.path, 'wb') as f:
            pickle.dump(self, f)


def integrate_product_data(payments, products):
    """
    Replace matchable Amazon payments with corresponding products.
    Returns tuple: (integrated_data, unmatched_products)
    """
    matcher = Matcher(payments, products, OUTPUT_DATA_DIR)
    matcher.match()
    # Get unmatched products: all original products minus matched products
    # This is more reliable than using prods.unmatched which accumulates per order
    all_product_indices = matcher.prods.original.index
    matched_indices = matcher.prods.matched
    unmatched_indices = all_product_indices.difference(matched_indices)
    unmatched_products = matcher.prods.original.loc[unmatched_indices].copy()
    
    # Convert unmatched products dates to date objects to match integrated_data format
    # This ensures all dates are the same type (date objects) for consistent comparison
    if len(unmatched_products) > 0 and 'date' in unmatched_products.columns:
        try:
            # Handle timezone-aware datetimes if present
            unmatched_products['date'] = pd.to_datetime(unmatched_products['date'], utc=True).dt.tz_localize(None).dt.date
        except (TypeError, ValueError):
            # Fallback: if utc=True fails, convert normally
            unmatched_products['date'] = pd.to_datetime(unmatched_products['date']).dt.date
    
    return matcher.integrated_data, unmatched_products


def combine_ch_and_cc_data(data):
    """Combine all checking and credit card data, plus unmatched Amazon products."""
    data_to_combine = [
        data['jack_ch'],
        data['jack_cc'],
        data['jill_ch'],
        data['jill_cc']
    ]
    # Add unmatched Amazon products if they exist
    if 'amzn_unmatched' in data and len(data['amzn_unmatched']) > 0:
        data_to_combine.append(data['amzn_unmatched'])
    return pd.concat(data_to_combine, ignore_index=True)


def combine(data):
    """
    Swap Amazon payments in Jill's credit card data with more detailed
    product purchase data and then combine all checking and credit card
    data, plus unmatched Amazon products.
    """
    # Debug: print row counts before combining
    print("\n=== COMBINING DATA ===")
    print(f"  jill_cc before integration: {len(data['jill_cc'])} rows")
    print(f"  amzn products: {len(data['amzn'])} rows")
    
    # Integrate products - returns (integrated_data, unmatched_products)
    data['jill_cc'], data['amzn_unmatched'] = integrate_product_data(data['jill_cc'], data['amzn'])
    print(f"  jill_cc after integration: {len(data['jill_cc'])} rows")
    print(f"  amzn unmatched products: {len(data['amzn_unmatched'])} rows")
    
    data['combined'] = combine_ch_and_cc_data(data)
    print(f"  combined total: {len(data['combined'])} rows")
    print("=" * 50)
    
    return data


# ============================================================================
# DATA LABELING
# ============================================================================

def vendor_is_known(row, table):
    """
    Return `True` if the row description contains a familiar vendor
    code and `False` if not.
    """
    description = table.loc[row, 'description']
    vendor_codes = VENDOR_DICT.keys()
    for code in vendor_codes:
        if code in description:
            return True
    return False


def get_known_labels(row, table):
    """Get pre-defined labels for a payment to a familiar vendor."""
    description = table.loc[row, 'description']
    vendor_codes = VENDOR_DICT.keys()
    for code in vendor_codes:
        if code in description:
            labels = VENDOR_DICT[code]
            return labels


async def make_product_labels(row, table):
    """Label one row of Amazon product data using the OpenAI API."""
    if ACLIENT is None:
        raise ImportError("OpenAI client is required for make_product_labels()")
    
    product_name = table.loc[row, 'description']
    
    instructions = PRODUCT_LABELING_INSTRUCTIONS
    
    response = await ACLIENT.responses.parse(
        model=MODEL,
        input=product_name,
        instructions=instructions,
        text_format=CategoryResponse,
        service_tier=SERVICE_TIER
    )
    
    labels = {
        'vendor': 'Amazon',
        'category': response.output_parsed.category,
        'llm_category': 1
    }
    
    return labels


async def make_labels_with_OpenAI(row, table):
    """Label one row of payment data using the OpenAI API."""
    if ACLIENT is None:
        raise ImportError("OpenAI client is required for make_labels_with_OpenAI()")
    
    description = table.loc[row, 'description']
    
    instructions = PAYMENT_LABELING_INSTRUCTIONS
    
    response = await ACLIENT.responses.parse(
        model=MODEL,
        input=description,
        instructions=instructions,
        text_format=DualResponse,
        service_tier=SERVICE_TIER
    )
    
    labels = {
        'vendor': response.output_parsed.vendor,
        'category': response.output_parsed.category,
        'llm_vendor': 1,
        'llm_category': 1
    }
    
    return labels


async def make_payment_labels(row, table):
    """
    Label one row of payment data.
    
    This function returns pre-defined labels for familiar transactions
    and makes new labels for other transactions with the OpenAI API.
    """
    if vendor_is_known(row, table):
        labels = get_known_labels(row, table)
    else:
        labels = await make_labels_with_OpenAI(row, table)
    
    return labels


def make_row_instructions(table):
    """Make a list with one labeling coroutine for each row in table."""
    row_instructions = []
    
    for row in table.index:
        if table.loc[row, 'account'] == 'amzn':
            row_instructions.append(make_product_labels(row, table))
        else:
            row_instructions.append(make_payment_labels(row, table))
    
    return row_instructions


async def process_asynchronously(row_instructions):
    """Process all coroutines in row_instructions asynchronously."""
    labels = await ProgressBar.gather(
        *row_instructions,
        desc='Labeling Rows with OpenAI',
        unit='row',
        bar_format='{l_bar}{bar} {n_fmt}/{total_fmt} [{elapsed}]'
    )
    return labels


def make_table_instructions(row_instructions):
    """
    Convert row_instructions into a function that contains all 
    asynchronous labeling operations for the source table.
    """
    def table_instructions():
        labels = asyncio.run(process_asynchronously(row_instructions))
        return labels
    
    return table_instructions


def make_labeling_instructions(table):
    """
    Make a function that contains all asynchronous labeling operations
    for table.
    """
    row_instructions = make_row_instructions(table)
    table_instructions = make_table_instructions(row_instructions)
    return table_instructions


def make_labels(instructions):
    """
    Make labels from instructions with the OpenAI API.
    
    This function executes all asynchronous labeling operations in a 
    new thread to avoid conflict with Jupyter Notebook event loops.
    """
    from concurrent.futures import ThreadPoolExecutor as JobManager
    with JobManager() as m:
        job = m.submit(instructions)
    
    labels = job.result()
    return labels


def apply_labels(all_labels, table):
    """Apply labels to table."""
    for row, row_labels in zip(table.index, all_labels):
        for column in row_labels:
            table.loc[row, column] = row_labels[column]
    
    return table


def save(table):
    """
    Save table as labeled_data.csv in the output data folder defined in
    the config module.
    """
    table.to_csv(OUTPUT_DATA_DIR / 'labeled_data.csv', index=False)


def label(data):
    """
    Label each row of data['combined'] and bind the result to 
    data['labeled'].
    
    This function adds the following columns:
        vendor: The transaction counterparty
        category: The transaction category
        llm_vendor: A binary flag that indicates that an LLM generated
        the corresponding value in the vendor column
        llm_category: A binary flag that indicates that an LLM  
        generated the corresponding value in the category column
    """
    if ACLIENT is None:
        raise ImportError("OpenAI client is required for label() function")
    
    instructions = make_labeling_instructions(data['combined'])
    labels = make_labels(instructions)
    data['labeled'] = apply_labels(labels, data['combined'])
    save(data['labeled'])
    
    return data


# ============================================================================
# DATA ANALYSIS - STATISTICS
# ============================================================================

def show_time_period(table):
    """Display the dates of the earliest and latest transactions in table."""
    # Ensure all dates are the same type (date objects) for comparison
    # Convert any datetime objects to date objects to avoid comparison errors
    date_col = table['date'].copy()
    if date_col.dtype.name.startswith('datetime'):
        # Has datetime objects - convert to date
        date_col = pd.to_datetime(date_col).dt.date
    else:
        # Check if we have mixed types (datetime and date)
        # Convert all to date objects to ensure consistency
        try:
            date_col = pd.to_datetime(date_col).dt.date
        except (TypeError, ValueError):
            # Already date objects or can't convert - use as is
            pass
    
    start_date = str(date_col.min())
    end_date = str(date_col.max())
    print('')
    print(f'Time Period: {start_date} to {end_date}')


def show_total_spend(table):
    """Print the sum of all expenses in the given table."""
    total_spend = table['amount'].sum()
    print('')
    print(f'Total Spending: ${total_spend:,.2f}')


def format_values(account_totals):
    """Format the values of account_totals as money."""
    return account_totals.map(lambda x: f'${x:,.2f}')


def remove_unnecessary_data(account_totals):
    """
    Remove the Series name, index name, and dtype from the 
    account_totals display.
    """
    return account_totals.to_string(header=False)


def format_account_totals(account_totals):
    """Format values and remove unnecessary data."""
    account_totals = format_values(account_totals)
    account_totals = remove_unnecessary_data(account_totals)
    return account_totals


def show_spend_by_account(table):
    """Print the sum of expenses by account."""
    grouped_table = table.groupby('account')
    account_totals = grouped_table['amount'].sum()
    formatted_totals = format_account_totals(account_totals)
    print('')
    print('Spending by Account:')
    print(formatted_totals)
    print('')


def show_stats(table):
    """Calculate and display total spending and spending by account."""
    show_total_spend(table)
    show_spend_by_account(table)


def save_summary(table):
    """
    Save summary statistics as CSV files in the output directory.
    Creates summary CSV files with formatted text matching the console output.
    """
    OUTPUT_DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    # Get summary data
    start_date = str(table['date'].min())
    end_date = str(table['date'].max())
    total_spend = float(table['amount'].sum())
    grouped_table = table.groupby('account')
    account_totals = grouped_table['amount'].sum()
    
    # Create comprehensive summary with formatted text (matching console output)
    summary_lines = [
        f'Time Period: {start_date} to {end_date}',
        '',
        f'Total Spending: ${total_spend:,.2f}',
        '',
        'Spending by Account:',
    ]
    
    # Add account breakdown (formatted like console output)
    for account, amount in account_totals.items():
        formatted_amount = f'${float(amount):,.2f}'
        summary_lines.append(f'{account:12} {formatted_amount}')
    
    summary_lines.append('')
    
    # Save as CSV with single column containing the formatted text
    summary_df = pd.DataFrame({
        'Summary': summary_lines
    })
    summary_df.to_csv(OUTPUT_DATA_DIR / 'summary.csv', index=False)
    
    # Also create structured CSV file for data processing
    summary_stats = pd.DataFrame({
        'Metric': ['Start Date', 'End Date', 'Total Spending'],
        'Value': [start_date, end_date, f'${total_spend:,.2f}']
    })
    summary_stats.to_csv(OUTPUT_DATA_DIR / 'summary_stats.csv', index=False)
    
    print(f"\nSummary saved to:")
    print(f"  - {OUTPUT_DATA_DIR / 'summary.csv'} (formatted text)")
    print(f"  - {OUTPUT_DATA_DIR / 'summary_stats.csv'} (structured data)")


# ============================================================================
# DATA ANALYSIS - CHARTS
# ============================================================================

def cast_amount_to_float(table):
    """Cast the dtype of table['amount'] to float."""
    table['amount'] = table['amount'].astype(float)
    return table


def sum_amount_by_group(table, grouping):
    """
    Group the rows in table by grouping and then sum 'amount' column 
    values.
    """
    return table.groupby(grouping)['amount'].sum()


def sort(data):
    """
    Sort rows in descending order with 'Other', if present, in the last
    position.
    """
    data = data.sort_values(ascending=False)
    
    if 'Other' in data.index:
        new_index = [*data.index.drop('Other'), 'Other']
        data = data.reindex(new_index)
    
    return data


def prepare_data(table, grouping):
    """
    Set the 'amount' column to a compatible dtype, sum amount by group,
    and sort the result.
    """
    data = cast_amount_to_float(table)
    data = sum_amount_by_group(table, grouping)
    data = sort(data)
    return data


def make_canvas(grouping):
    """Return a new, titled `Axes` object for plotting."""
    if plt is None:
        raise ImportError("matplotlib is required for chart generation")
    
    canvas = plt.subplot(111)
    canvas.set_title(f'Spending by {grouping.title()}')
    return canvas


def make_autopct_function(grouped_df):
    """
    Create a label formatting function to pass to the Matplotlib pie chart 
    method.
    """
    def autopct(wedge_percentage):
        total_dollar_value = grouped_df.sum()
        wedge_dollar_value = (wedge_percentage / 100) * total_dollar_value
        return f'${wedge_dollar_value:,.2f}\n({int(round(wedge_percentage)):d}%)'
    return autopct


def prepare_pie_elements(data, canvas):
    """Prepare pie chart elements for display."""
    canvas.pie(
        data,
        labels=data.index,
        autopct=make_autopct_function(data),
        startangle=90,
        counterclock=False
    )
    canvas.axis('equal')


def draw_bars(data, canvas):
    """Plot bars of data values on canvas from top to bottom."""
    data.plot(kind='barh', ax=canvas)
    canvas.invert_yaxis()


def remove_x_axis_labels(canvas):
    """Remove labels and ticks from the x axis of canvas."""
    canvas.set_xlabel(None)
    canvas.set_xticks([])
    canvas.tick_params(axis='x', bottom=False, labelbottom=False)


def add_bar_labels(data, canvas):
    """Label the value of each bar on canvas."""
    bars = canvas.containers[0]
    canvas.bar_label(
        bars,
        labels=[f'${v:,.2f}' for v in data.values],
        label_type='edge',
        padding=4
    )
    canvas.margins(x=0.2)


def set_labels(data, canvas):
    """Remove x axis labels and ticks and label each bar on canvas."""
    remove_x_axis_labels(canvas)
    add_bar_labels(data, canvas)


def prepare_bar_elements(data, canvas):
    """Plot and label bars on canvas."""
    draw_bars(data, canvas)
    set_labels(data, canvas)


def display_chart(table, grouping, chart_type, suppress_show=False):
    """
    Display a bar or pie chart of table data grouped by grouping.
    Also saves the chart as a PNG file in the output directory.
    
    Args:
        suppress_show: If True, don't call plt.show() (for widget context)
    """
    if plt is None:
        raise ImportError("matplotlib is required for chart generation")
    
    prepared_data = prepare_data(table, grouping)
    canvas = make_canvas(grouping)
    
    if chart_type == 'bar':
        prepare_bar_elements(prepared_data, canvas)
    elif chart_type == 'pie':
        prepare_pie_elements(prepared_data, canvas)
    else:
        raise ValueError(f"chart_type must be 'bar' or 'pie'; received {chart_type}.")
    
    # Save the chart as an image file
    OUTPUT_DATA_DIR.mkdir(parents=True, exist_ok=True)
    filename = f'spending_by_{grouping}.png'
    filepath = OUTPUT_DATA_DIR / filename
    plt.savefig(filepath, dpi=300, bbox_inches='tight')
    
    # Only display the chart if not suppressed (for widget context)
    if not suppress_show:
        plt.show()
    else:
        plt.close()  # Close the figure to free memory


def merge_groups(table, grouping, min):
    """
    For all rows in any group with a total expense amount under min,
    change the value of the grouping column to "Other".
    """
    expense_totals = table.groupby(grouping)['amount'].sum()
    small_groups = expense_totals[expense_totals < min].index
    filter = table[grouping].isin(small_groups)
    table.loc[filter, grouping] = 'Other'
    return table


def show_spend_by_spender(table, suppress_show=False):
    """Display a pie chart of total expenses by spender."""
    display_chart(table, 'spender', 'pie', suppress_show=suppress_show)


def show_spend_by_category(table, suppress_show=False):
    """
    Display a pie chart of total expenses by category.
    
    Merge categories with fewer than min transactions.
    """
    table = merge_groups(table, 'category', CATEGORY_MIN)
    display_chart(table, 'category', 'pie', suppress_show=suppress_show)


def show_spend_by_vendor(table, suppress_show=False):
    """
    Display a bar chart of total expenses by vendor.
    
    Merge vendors with fewer than min transactions.
    """
    table = merge_groups(table, 'vendor', VENDOR_MIN)
    display_chart(table, 'vendor', 'bar', suppress_show=suppress_show)


def show_charts(table, suppress_show=False):
    """
    Display spending by spender and by category as pie charts and 
    spending by vendor as a bar chart.
    """
    show_spend_by_spender(table, suppress_show=suppress_show)
    show_spend_by_category(table, suppress_show=suppress_show)
    show_spend_by_vendor(table, suppress_show=suppress_show)


def analyze(data, suppress_show=False):
    """
    Display charts and statistics for the labeled expense data.
    """
    table = data['labeled']
    show_time_period(table)
    show_stats(table)
    save_summary(table)
    show_charts(table, suppress_show=suppress_show)


# ============================================================================
# MAIN PIPELINE FUNCTION
# ============================================================================

def run_pipeline(suppress_show=False):
    """
    Execute the complete data processing pipeline:
    1. Load data from Excel sheets in the active workbook
    2. Tag data with account and spender information
    3. Clean data (rename columns, filter rows, drop columns, cast dtypes)
    4. Combine data (integrate Amazon products, combine all accounts)
    5. Label data (add vendor and category using OpenAI API)
    6. Analyze data (show statistics and charts)
    
    Args:
        suppress_show: If True, suppress matplotlib plt.show() calls (for widget context)
    """
    data = load_data()
    data = tag(data)
    data = clean(data)
    data = combine(data)
    data = label(data)
    analyze(data, suppress_show=suppress_show)
    return data  # Return data so widget can store it

# ============================================================================
# PYXLL WIDGET CODE (MINIMAL FOR TESTING)
# ============================================================================

_ctp = None
_qt_app = None


# ---------------------------
# Monte Carlo Simulation
# ---------------------------
def run_monte_carlo_simulation(table, num_simulations=1000, forecast_months=12):
    """
    Run Monte Carlo simulation on labeled expense data to forecast future spending.
    
    Args:
        table: DataFrame with labeled expense data (must have 'amount' and 'date' columns)
        num_simulations: Number of Monte Carlo iterations (default: 1000)
        forecast_months: Number of months to forecast (default: 12)
    
    Returns:
        tuple: (results_df, summary_dict) where results_df contains monthly forecasts
               and summary_dict contains key statistics
    """
    if pd is None:
        raise ImportError("pandas is required for Monte Carlo simulation")
    if np is None:
        raise ImportError("numpy is required for Monte Carlo simulation")
    
    # Convert amounts to float for calculations
    amounts = table['amount'].astype(float)
    
    # Calculate historical statistics by grouping actual monthly spending
    dates = pd.to_datetime(table['date'])
    
    # Group by month and year to get actual monthly totals
    monthly_spending = amounts.groupby(dates.dt.to_period('M')).sum()
    
    if len(monthly_spending) < 2:
        print("Warning: Not enough historical monthly data. Using overall average.")
        # Fallback: use total divided by number of months
        num_months = max(1, (dates.max() - dates.min()).days / 30.44)
        mean_monthly = amounts.sum() / num_months
        std_monthly = amounts.std() * np.sqrt(30.44)  # Scale daily std to monthly
    else:
        mean_monthly = float(monthly_spending.mean())
        std_monthly = float(monthly_spending.std())
    
    # Get last date in data and calculate forecast start (next month)
    last_date = pd.to_datetime(dates.max())
    # Start forecasting from the first day of the next month
    if last_date.day == 1:
        forecast_start = last_date
    else:
        forecast_start = (last_date + pd.DateOffset(months=1)).replace(day=1)
    
    # Run Monte Carlo simulations
    print(f"\nRunning {num_simulations} Monte Carlo simulations...")
    print(f"Historical data: {len(monthly_spending)} months")
    print(f"Historical mean monthly spending: ${mean_monthly:,.2f}")
    print(f"Historical std dev monthly spending: ${std_monthly:,.2f}")
    print(f"Forecasting {forecast_months} months starting from {forecast_start.strftime('%Y-%m-%d')}")
    
    simulations = []
    for i in range(num_simulations):
        # Generate random monthly spending based on normal distribution
        monthly_totals = np.random.normal(mean_monthly, std_monthly, forecast_months)
        # Ensure no negative spending
        monthly_totals = np.maximum(monthly_totals, 0)
        simulations.append(monthly_totals)
    
    # Calculate statistics across all simulations
    simulations_array = np.array(simulations)
    
    # Create results DataFrame with clearer column names
    results = []
    for month_idx in range(forecast_months):
        # Calculate forecast date (end of each month)
        forecast_date = forecast_start + pd.DateOffset(months=month_idx)
        month_end = (forecast_date + pd.DateOffset(months=1) - pd.DateOffset(days=1))
        
        month_simulations = simulations_array[:, month_idx]
        
        results.append({
            'Forecast Month': month_end.strftime('%Y-%m'),
            'Expected (Mean)': float(np.mean(month_simulations)),
            'Median': float(np.median(month_simulations)),
            '5th %ile (Low)': float(np.percentile(month_simulations, 5)),
            '25th %ile': float(np.percentile(month_simulations, 25)),
            '75th %ile': float(np.percentile(month_simulations, 75)),
            '95th %ile (High)': float(np.percentile(month_simulations, 95)),
            'Minimum': float(np.min(month_simulations)),
            'Maximum': float(np.max(month_simulations)),
            'Std Deviation': float(np.std(month_simulations))
        })
    
    results_df = pd.DataFrame(results)
    
    # Calculate annual summary statistics
    total_mean = results_df['Expected (Mean)'].sum()
    total_median = results_df['Median'].sum()
    total_std = np.sqrt((results_df['Std Deviation'] ** 2).sum())
    
    # Calculate confidence intervals for annual total
    annual_simulations = simulations_array.sum(axis=1)
    annual_5th = float(np.percentile(annual_simulations, 5))
    annual_25th = float(np.percentile(annual_simulations, 25))
    annual_75th = float(np.percentile(annual_simulations, 75))
    annual_95th = float(np.percentile(annual_simulations, 95))
    annual_min = float(np.min(annual_simulations))
    annual_max = float(np.max(annual_simulations))
    
    summary_dict = {
        'forecast_start': forecast_start,
        'forecast_months': forecast_months,
        'num_simulations': num_simulations,
        'historical_mean_monthly': mean_monthly,
        'historical_std_monthly': std_monthly,
        'historical_months': len(monthly_spending),
        'annual_mean': total_mean,
        'annual_median': total_median,
        'annual_std': total_std,
        'annual_5th': annual_5th,
        'annual_25th': annual_25th,
        'annual_75th': annual_75th,
        'annual_95th': annual_95th,
        'annual_min': annual_min,
        'annual_max': annual_max
    }
    
    print(f"\nMonte Carlo Simulation Complete!")
    print(f"Projected annual spending:")
    print(f"  Expected (Mean): ${total_mean:,.2f}")
    print(f"  Median: ${total_median:,.2f}")
    print(f"  90% Confidence Interval: ${annual_5th:,.2f} - ${annual_95th:,.2f}")
    
    return results_df, summary_dict


def _write_monte_carlo_results_to_excel(results_df, summary_dict, sheet_name):
    """
    Write Monte Carlo simulation results to a new Excel sheet with summary section and charts.
    """
    try:
        app = xl_app()
        wb = app.ActiveWorkbook
        if wb is None:
            raise RuntimeError("No active workbook found.")
        
        # Disable alerts to suppress delete confirmation dialog
        original_display_alerts = app.DisplayAlerts
        app.DisplayAlerts = False
        
        try:
            # Delete sheet if it already exists
            try:
                existing_ws = wb.Worksheets(sheet_name)
                existing_ws.Delete()
            except Exception:
                pass
        finally:
            # Restore original alert setting
            app.DisplayAlerts = original_display_alerts
        
        # Create new sheet at the very end
        last_sheet = wb.Worksheets(wb.Worksheets.Count)
        new_ws = wb.Worksheets.Add(After=last_sheet)
        new_ws.Name = sheet_name
        
        current_row = 1
        
        # ===== TITLE SECTION =====
        # Create a prominent title at the top
        title_range = new_ws.Range(new_ws.Cells(current_row, 1), new_ws.Cells(current_row, 10))
        title_range.Merge()
        title_range.Value = "Monte Carlo Simulation - Spending Forecast"
        title_range.Font.Bold = True
        title_range.Font.Size = 18
        title_range.Font.Color = 0xFFFFFF  # White text
        title_range.Interior.Color = 0x00602000  # Dark blue background (#002060 in BGR format)
        title_range.HorizontalAlignment = -4108  # xlCenter
        title_range.VerticalAlignment = -4107  # xlCenter
        title_range.RowHeight = 35
        
        current_row += 2
        
        # Summary statistics
        summary_labels = [
            ("Simulation Parameters:", ""),
            (f"  Historical Data Period:", f"{summary_dict['historical_months']} months"),
            (f"  Historical Mean Monthly:", f"${summary_dict['historical_mean_monthly']:,.2f}"),
            (f"  Historical Std Dev Monthly:", f"${summary_dict['historical_std_monthly']:,.2f}"),
            (f"  Number of Simulations:", f"{summary_dict['num_simulations']:,}"),
            (f"  Forecast Period:", f"{summary_dict['forecast_months']} months"),
            ("", ""),
            ("Annual Forecast Summary:", ""),
            (f"  Expected Annual Spending (Mean):", f"${summary_dict['annual_mean']:,.2f}"),
            (f"  Median Annual Spending:", f"${summary_dict['annual_median']:,.2f}"),
            (f"  Standard Deviation:", f"${summary_dict['annual_std']:,.2f}"),
            ("", ""),
            ("Confidence Intervals (Annual):", ""),
            (f"  90% Range (5th - 95th percentile):", f"${summary_dict['annual_5th']:,.2f} - ${summary_dict['annual_95th']:,.2f}"),
            (f"  50% Range (25th - 75th percentile):", f"${summary_dict['annual_25th']:,.2f} - ${summary_dict['annual_75th']:,.2f}"),
            (f"  Minimum Possible:", f"${summary_dict['annual_min']:,.2f}"),
            (f"  Maximum Possible:", f"${summary_dict['annual_max']:,.2f}"),
        ]
        
        for label, value in summary_labels:
            if label:
                new_ws.Cells(current_row, 1).Value = label
                if label.endswith(":"):
                    new_ws.Cells(current_row, 1).Font.Bold = True
                if value:
                    new_ws.Cells(current_row, 2).Value = value
                    if "$" in value:
                        new_ws.Cells(current_row, 2).NumberFormat = "$#,##0.00"
            current_row += 1
        
        current_row += 2
        
        # ===== MONTHLY FORECAST TABLE =====
        table_start_row = current_row
        new_ws.Cells(current_row, 1).Value = "Monthly Forecast Details"
        new_ws.Cells(current_row, 1).Font.Bold = True
        new_ws.Cells(current_row, 1).Font.Size = 12
        current_row += 1
        
        # Write headers
        for col_idx, col_name in enumerate(results_df.columns, start=1):
            new_ws.Cells(current_row, col_idx).Value = col_name
            new_ws.Cells(current_row, col_idx).Font.Bold = True
            new_ws.Cells(current_row, col_idx).Interior.Color = 0xD9E1F2  # Light blue background
        
        current_row += 1
        
        # Write data rows
        for row_idx, (_, row) in enumerate(results_df.iterrows(), start=current_row):
            for col_idx, value in enumerate(row, start=1):
                if pd.isna(value):
                    new_ws.Cells(row_idx, col_idx).Value = None
                else:
                    if isinstance(value, (int, float)):
                        new_ws.Cells(row_idx, col_idx).Value = float(value)
                        # Format as currency for all numeric columns except "Forecast Month"
                        col_name = results_df.columns[col_idx - 1]
                        if col_name != 'Forecast Month':
                            new_ws.Cells(row_idx, col_idx).NumberFormat = "$#,##0.00"
                    else:
                        new_ws.Cells(row_idx, col_idx).Value = str(value)
        
        table_end_row = current_row + len(results_df) - 1
        table_end_col = len(results_df.columns)
        
        # Format table borders
        table_range = new_ws.Range(
            new_ws.Cells(table_start_row + 1, 1),
            new_ws.Cells(table_end_row, table_end_col)
        )
        table_range.Borders.LineStyle = 1  # xlContinuous
        
        # Auto-fit columns
        try:
            new_ws.Columns.AutoFit()
        except Exception:
            pass
        
        # ===== CREATE CHARTS =====
        # Place charts below the table, starting a few rows down in column A
        chart_start_row = table_end_row + 3
        
        # Find column indices for key metrics
        col_map = {col: idx + 1 for idx, col in enumerate(results_df.columns)}
        month_col = col_map.get('Forecast Month', 1)
        expected_col = col_map.get('Expected (Mean)', 2)
        p5_col = col_map.get('5th %ile (Low)', 4)
        p95_col = col_map.get('95th %ile (High)', 7)
        median_col = col_map.get('Median', 3)
        
        # Chart 1: Monthly Forecast with Confidence Intervals (Line Chart)
        try:
            # Get the exact position from column A
            anchor_cell = new_ws.Cells(chart_start_row, 1)  # Column A
            chart1_left = anchor_cell.Left
            chart1_top = anchor_cell.Top
            chart1_obj = new_ws.ChartObjects().Add(chart1_left, chart1_top, 700, 400)
            # Set placement to free floating so Excel doesn't auto-position it
            chart1_obj.Placement = 3  # xlFreeFloating
            chart1 = chart1_obj.Chart
            chart1.ChartType = 4  # xlLineMarkers
            
            # Set data source: Include Month (X-axis) and key metrics (Y-axis)
            # Range includes headers and data for: Month, Expected, 5th %ile, 95th %ile, Median
            data_range = new_ws.Range(
                new_ws.Cells(table_start_row, month_col),  # Start from header row
                new_ws.Cells(table_end_row, p95_col)  # Include all forecast columns
            )
            chart1.SetSourceData(data_range)
            
            chart1.HasTitle = True
            chart1.ChartTitle.Text = "Monthly Spending Forecast with Confidence Intervals"
            chart1.ChartTitle.Font.Size = 14
            chart1.ChartTitle.Font.Bold = True
            chart1.HasLegend = True
            chart1.Legend.Position = -4160  # xlLegendPositionBottom
            chart1.Legend.Font.Size = 10
            
            # Format Y-axis as currency
            chart1.Axes(2).TickLabels.NumberFormat = "$#,##0"
            chart1.Axes(2).TickLabels.Font.Size = 10
            chart1.Axes(1).TickLabels.Font.Size = 10
            
            # Set X-axis title
            chart1.Axes(1).HasTitle = True
            chart1.Axes(1).AxisTitle.Text = "Forecast Month"
            chart1.Axes(1).AxisTitle.Font.Size = 11
            chart1.Axes(2).HasTitle = True
            chart1.Axes(2).AxisTitle.Text = "Spending ($)"
            chart1.Axes(2).AxisTitle.Font.Size = 11
            
            # Style the chart area with colored background
            chart1.ChartArea.Format.Fill.ForeColor.RGB = 0xF2F2F2  # Light gray background
            chart1.ChartArea.Format.Line.Visible = True
            chart1.ChartArea.Format.Line.ForeColor.RGB = 0xD0D0D0  # Light gray border
            chart1.ChartArea.Format.Line.Weight = 1.5
            
            # Style the plot area with subtle background
            chart1.PlotArea.Format.Fill.ForeColor.RGB = 0xFFFFFF  # White background
            chart1.PlotArea.Format.Line.Visible = True
            chart1.PlotArea.Format.Line.ForeColor.RGB = 0xE0E0E0  # Light border
            
            # Disable all gridlines (both horizontal and vertical)
            chart1.Axes(1).HasMajorGridlines = False
            chart1.Axes(2).HasMajorGridlines = False
            
            # Color the data series for better visibility
            try:
                # Expected (Mean) - Dark blue
                chart1.SeriesCollection(1).Format.Line.ForeColor.RGB = 0x002060  # Dark blue
                chart1.SeriesCollection(1).Format.Line.Weight = 2.5
                chart1.SeriesCollection(1).MarkerStyle = 8  # Circle markers
                chart1.SeriesCollection(1).MarkerSize = 6
            except Exception:
                pass
            
            try:
                # Median - Teal
                if chart1.SeriesCollection.Count >= 2:
                    chart1.SeriesCollection(2).Format.Line.ForeColor.RGB = 0x008080  # Teal
                    chart1.SeriesCollection(2).Format.Line.Weight = 2
                    chart1.SeriesCollection(2).MarkerStyle = 8
                    chart1.SeriesCollection(2).MarkerSize = 5
            except Exception:
                pass
            
            try:
                # 5th Percentile - Orange
                for i in range(1, chart1.SeriesCollection.Count + 1):
                    if "5th" in str(chart1.SeriesCollection(i).Name) or "Low" in str(chart1.SeriesCollection(i).Name):
                        chart1.SeriesCollection(i).Format.Line.ForeColor.RGB = 0xFF6B35  # Orange
                        chart1.SeriesCollection(i).Format.Line.Weight = 2
                        chart1.SeriesCollection(i).Format.Line.DashStyle = 2  # Dashed line
            except Exception:
                pass
            
            try:
                # 95th Percentile - Green
                for i in range(1, chart1.SeriesCollection.Count + 1):
                    if "95th" in str(chart1.SeriesCollection(i).Name) or "High" in str(chart1.SeriesCollection(i).Name):
                        chart1.SeriesCollection(i).Format.Line.ForeColor.RGB = 0x2E7D32  # Green
                        chart1.SeriesCollection(i).Format.Line.Weight = 2
                        chart1.SeriesCollection(i).Format.Line.DashStyle = 2  # Dashed line
            except Exception:
                pass
            
            # Explicitly set position after configuration (in case Excel moved it)
            chart1_obj.Left = chart1_left
            chart1_obj.Top = chart1_top
            
        except Exception as e:
            print(f"Warning: Could not create forecast chart: {str(e)}")
            import traceback
            print(traceback.format_exc())
        
        # Chart 2: Annual Summary Comparison (Bar Chart)
        # Place Chart 2 below Chart 1 (Chart 1 is ~400px tall, so roughly 30 rows down for spacing)
        chart2_start_row = chart_start_row + 30
        try:
            # Get the exact position from column A
            anchor_cell2 = new_ws.Cells(chart2_start_row, 1)  # Column A
            chart2_left = anchor_cell2.Left
            chart2_top = anchor_cell2.Top
            chart2_obj = new_ws.ChartObjects().Add(chart2_left, chart2_top, 700, 400)
            # Set placement to free floating so Excel doesn't auto-position it
            chart2_obj.Placement = 3  # xlFreeFloating
            chart2 = chart2_obj.Chart
            chart2.ChartType = 57  # xlBarClustered (horizontal bars)
            
            # Create a summary table for the chart (hidden, just for data source)
            # Place it in a column far to the right so it doesn't interfere
            summary_table_row = chart2_start_row
            summary_table_col = 20  # Place it far to the right (hidden)
            
            new_ws.Cells(summary_table_row, summary_table_col).Value = "Metric"
            new_ws.Cells(summary_table_row, summary_table_col + 1).Value = "Amount"
            new_ws.Cells(summary_table_row, summary_table_col).Font.Bold = True
            new_ws.Cells(summary_table_row, summary_table_col + 1).Font.Bold = True
            summary_table_row += 1
            
            summary_data = [
                ("Expected (Mean)", summary_dict['annual_mean']),
                ("Median", summary_dict['annual_median']),
                ("5th Percentile", summary_dict['annual_5th']),
                ("25th Percentile", summary_dict['annual_25th']),
                ("75th Percentile", summary_dict['annual_75th']),
                ("95th Percentile", summary_dict['annual_95th']),
            ]
            
            for i, (label, value) in enumerate(summary_data, start=summary_table_row):
                new_ws.Cells(i, summary_table_col).Value = label
                new_ws.Cells(i, summary_table_col + 1).Value = value
                new_ws.Cells(i, summary_table_col + 1).NumberFormat = "$#,##0.00"
            
            summary_range = new_ws.Range(
                new_ws.Cells(summary_table_row - 1, summary_table_col),  # Include header
                new_ws.Cells(summary_table_row + len(summary_data) - 1, summary_table_col + 1)
            )
            chart2.SetSourceData(summary_range)
            
            chart2.HasTitle = True
            chart2.ChartTitle.Text = "Annual Spending Forecast - Key Statistics"
            chart2.ChartTitle.Font.Size = 14
            chart2.ChartTitle.Font.Bold = True
            chart2.HasLegend = False
            
            # Format Y-axis as currency
            chart2.Axes(2).TickLabels.NumberFormat = "$#,##0"
            chart2.Axes(2).TickLabels.Font.Size = 10
            chart2.Axes(1).TickLabels.Font.Size = 10
            
            # Set axis titles
            chart2.Axes(1).HasTitle = True
            chart2.Axes(1).AxisTitle.Text = "Metric"
            chart2.Axes(1).AxisTitle.Font.Size = 11
            chart2.Axes(2).HasTitle = True
            chart2.Axes(2).AxisTitle.Text = "Annual Spending ($)"
            chart2.Axes(2).AxisTitle.Font.Size = 11
            
            # Style the chart area with colored background
            chart2.ChartArea.Format.Fill.ForeColor.RGB = 0xF2F2F2  # Light gray background
            chart2.ChartArea.Format.Line.Visible = True
            chart2.ChartArea.Format.Line.ForeColor.RGB = 0xD0D0D0  # Light gray border
            chart2.ChartArea.Format.Line.Weight = 1.5
            
            # Style the plot area with subtle background
            chart2.PlotArea.Format.Fill.ForeColor.RGB = 0xFFFFFF  # White background
            chart2.PlotArea.Format.Line.Visible = True
            chart2.PlotArea.Format.Line.ForeColor.RGB = 0xE0E0E0  # Light border
            
            # Disable all gridlines (both horizontal and vertical)
            chart2.Axes(1).HasMajorGridlines = False
            chart2.Axes(2).HasMajorGridlines = False
            
            # Increase bar thickness by reducing gap width in 3 iterations
            try:
                import time
                # Start with default gap width and reduce it: 3%, 3%, 3%
                gap_width = 50  # Start at 50%
                gap_width = gap_width - 3  # First iteration: -3%
                chart2.ChartGroups(1).GapWidth = gap_width
                time.sleep(0.3)  # Wait 0.3 seconds
                
                gap_width = gap_width - 3  # Second iteration: -3%
                chart2.ChartGroups(1).GapWidth = gap_width
                time.sleep(0.3)  # Wait 0.3 seconds
                
                gap_width = gap_width - 3  # Third iteration: -3%
                chart2.ChartGroups(1).GapWidth = gap_width  # Final gap width: 41%
            except Exception:
                pass
            
            # Color the bars with a single color
            try:
                # #44546A in RGB: R=68 (0x44), G=84 (0x54), B=106 (0x6A)
                # Excel BGR format: 0xBBGGRR = 0x006A5444
                bar_color = 0x006A5444
                border_color = 0x00404040  # Dark gray border
                
                # SeriesCollection is a method, need to get the collection first
                series_collection = chart2.SeriesCollection()
                series_count = series_collection.Count
                
                # For bar charts, try setting color on the series first
                for i in range(1, series_count + 1):
                    series = series_collection(i)
                    try:
                        # Method 1: Set series fill color
                        series.Format.Fill.ForeColor.RGB = bar_color
                        series.Format.Fill.Transparency = 0.0
                        # Set border
                        series.Format.Line.Visible = True
                        series.Format.Line.ForeColor.RGB = border_color
                        series.Format.Line.Weight = 1.5
                    except Exception:
                        # Method 2: Try setting on individual points if series method fails
                        try:
                            points = series.Points()
                            for point_idx in range(1, points.Count + 1):
                                point = points(point_idx)
                                point.Format.Fill.ForeColor.RGB = bar_color
                                point.Format.Fill.Transparency = 0.0
                                point.Format.Line.Visible = True
                                point.Format.Line.ForeColor.RGB = border_color
                                point.Format.Line.Weight = 1.5
                        except Exception:
                            pass
            except Exception as e:
                print(f"Warning: Could not set bar colors: {str(e)}")
                import traceback
                print(traceback.format_exc())
            
            # Hide the summary table columns (make them very narrow)
            new_ws.Columns(summary_table_col).ColumnWidth = 0.1
            new_ws.Columns(summary_table_col + 1).ColumnWidth = 0.1
            
            # Explicitly set position after configuration (in case Excel moved it)
            chart2_obj.Left = chart2_left
            chart2_obj.Top = chart2_top
            
        except Exception as e:
            print(f"Warning: Could not create summary chart: {str(e)}")
            import traceback
            print(traceback.format_exc())
        
        print(f"✓ Monte Carlo results written to sheet '{sheet_name}' with charts")
        
    except Exception as e:
        print(f"Warning: Could not write to Excel sheet: {str(e)}")
        import traceback
        print(traceback.format_exc())


# ---------------------------
# Excel Import Helper
# ---------------------------
def _add_picture_to_sheet(ws, img_path: Path, anchor_cell: str, scale: float = 0.6):
    """
    Add a picture to an Excel worksheet at the specified cell.
    
    Args:
        ws: Excel worksheet object
        img_path: Path to the image file
        anchor_cell: Cell reference (e.g., "D1")
        scale: Scale factor for the image (default 0.6 = 60% of original size)
    """
    if not img_path.exists():
        ws.Range(anchor_cell).Value = f"Missing: {img_path.name}"
        ws.Range(anchor_cell).Font.Bold = True
        return None
    
    try:
        # Get the anchor cell
        cell = ws.Range(anchor_cell)
        
        # Add picture using COM
        shape = ws.Shapes.AddPicture(
            str(img_path),
            False,  # LinkToFile
            True,   # SaveWithDocument
            cell.Left,
            cell.Top,
            -1,  # Width (-1 = original)
            -1,  # Height (-1 = original)
        )
        
        # Scale the image
        shape.LockAspectRatio = True
        shape.ScaleWidth(scale, True)
        shape.ScaleHeight(scale, True)
        
        return shape
    except Exception as e:
        print(f"Error adding picture {img_path.name}: {e}")
        return None


def _import_csv_to_active_workbook(csv_path: Path, sheet_name: str = "labeled_data"):
    """
    Import a CSV file into the active Excel workbook as a new sheet at the very end.
    Reads CSV with pandas and writes directly to Excel using COM, preserving exact format.
    """
    if pd is None:
        print(f"Warning: pandas is required to import CSV")
        return
        
    try:
        app = xl_app()
        wb = app.ActiveWorkbook
        
        if not csv_path.exists():
            print(f"Warning: CSV file not found at {csv_path}")
            return
        
        # Read CSV with pandas (preserves exact data format)
        df = pd.read_csv(csv_path)
        
        # Disable alerts to suppress delete confirmation dialog
        original_display_alerts = app.DisplayAlerts
        app.DisplayAlerts = False
        
        try:
            # Delete sheet if it already exists (to avoid duplicates)
            try:
                existing_ws = wb.Worksheets(sheet_name)
                existing_ws.Delete()
            except Exception:
                pass
        finally:
            # Restore original alert setting
            app.DisplayAlerts = original_display_alerts
        
        # Create new sheet at the very end
        last_sheet = wb.Worksheets(wb.Worksheets.Count)
        new_ws = wb.Worksheets.Add(After=last_sheet)
        new_ws.Name = sheet_name
        
        # Write DataFrame to Excel sheet using COM
        # Write headers
        for col_idx, col_name in enumerate(df.columns, start=1):
            new_ws.Cells(1, col_idx).Value = col_name
        
        # Write data rows
        for row_idx, (_, row) in enumerate(df.iterrows(), start=2):
            for col_idx, value in enumerate(row, start=1):
                # Preserve None/NaN as empty, convert other values
                if pd.isna(value):
                    new_ws.Cells(row_idx, col_idx).Value = None
                else:
                    new_ws.Cells(row_idx, col_idx).Value = value
        
        # Auto-fit columns
        try:
            new_ws.Columns.AutoFit()
        except Exception:
            pass
        
        print(f"✓ Imported {csv_path.name} to sheet '{sheet_name}' at the end")
        
    except Exception as e:
        print(f"Warning: Could not import CSV to workbook: {str(e)}")
        import traceback
        print(traceback.format_exc())


# ---------------------------
# Qt bootstrap (DO NOT BREAK)
# ---------------------------
def _ensure_qt():
    global _qt_app
    app = QtWidgets.QApplication.instance()
    if app is None:
        app = QtWidgets.QApplication([])
    _qt_app = app
    return app


# ---------------------------
# Custom output stream for widget
# ---------------------------
class WidgetOutputStream(QtCore.QObject):
    """Custom output stream that emits signals for widget updates."""
    text_written = QtCore.Signal(str)
    
    def write(self, text):
        # Emit all text, including empty strings for progress bar updates
        self.text_written.emit(text)
    
    def flush(self):
        pass
    
    def isatty(self):
        # Return False to indicate this is not a terminal
        # This helps tqdm format output appropriately
        return False


# ---------------------------
# Widget
# ---------------------------
class PipelineWidget(QtWidgets.QWidget):
    def __init__(self):
        super().__init__()
        
        # Store pipeline data state
        self.pipeline_data = None
        
        # Set dark blue background for the widget
        self.setStyleSheet("""
            QWidget {
                background-color: #1e3a5f;
                color: white;
            }
        """)
        
        # Create output area with dark theme
        self.output = QtWidgets.QTextEdit()
        self.output.setReadOnly(True)
        self.output.setFont(QtGui.QFont("Consolas", 9))
        self.output.setStyleSheet("""
            QTextEdit {
                background-color: #0f1f2e;
                color: white;
                border: 1px solid #2a4a6f;
                border-radius: 4px;
            }
        """)
        
        # Create label with white text
        label = QtWidgets.QLabel("Workflow output:")
        label.setStyleSheet("""
            QLabel {
                color: white;
                font-weight: bold;
            }
        """)
        
        # Button style template
        button_style = """
            QPushButton {
                color: white;
                font-size: 11px;
                font-weight: bold;
                padding: 6px;
                border-radius: 4px;
                min-width: 120px;
            }
            QPushButton:hover {
                opacity: 0.9;
            }
            QPushButton:disabled {
                background-color: #3a4a5a;
                color: #888888;
            }
        """
        
        # Create buttons for each step
        self.load_process_btn = QtWidgets.QPushButton("1. Load & Process")
        self.load_process_btn.clicked.connect(self.run_load_and_process)
        self.load_process_btn.setStyleSheet(button_style + """
            QPushButton {
                background-color: #2a5a7a;
            }
        """)
        
        self.label_btn = QtWidgets.QPushButton("2. Label Data")
        self.label_btn.clicked.connect(self.run_labeling)
        self.label_btn.setStyleSheet(button_style + """
            QPushButton {
                background-color: #5a7a9a;
            }
        """)
        
        self.charts_btn = QtWidgets.QPushButton("3. Generate Charts")
        self.charts_btn.clicked.connect(self.run_charts)
        self.charts_btn.setStyleSheet(button_style + """
            QPushButton {
                background-color: #7a9aba;
            }
        """)
        
        # Create Monte Carlo button (green background)
        self.monte_carlo_btn = QtWidgets.QPushButton("Run Monte Carlo")
        self.monte_carlo_btn.clicked.connect(self.run_monte_carlo)
        self.monte_carlo_btn.setStyleSheet(button_style + """
            QPushButton {
                background-color: #4a9a4a;
            }
        """)
        
        # Create clear button
        self.clear_btn = QtWidgets.QPushButton("Clear Output")
        self.clear_btn.clicked.connect(self.clear_output)
        self.clear_btn.setStyleSheet(button_style + """
            QPushButton {
                background-color: #6a4a5a;
            }
        """)
        
        # Layout for step buttons
        step_buttons_layout = QtWidgets.QHBoxLayout()
        step_buttons_layout.addWidget(self.load_process_btn)
        step_buttons_layout.addWidget(self.label_btn)
        step_buttons_layout.addWidget(self.charts_btn)
        step_buttons_layout.addStretch()
        
        # Layout for action buttons
        action_buttons_layout = QtWidgets.QHBoxLayout()
        action_buttons_layout.addWidget(self.monte_carlo_btn)
        action_buttons_layout.addWidget(self.clear_btn)
        action_buttons_layout.addStretch()
        
        layout = QtWidgets.QVBoxLayout(self)
        layout.addWidget(label)
        layout.addWidget(self.output)
        layout.addLayout(step_buttons_layout)
        layout.addLayout(action_buttons_layout)
        
        # Custom output stream
        self.output_stream = WidgetOutputStream()
        self.output_stream.text_written.connect(self.log)
        
        # Store original stdout/stderr
        self.original_stdout = sys.stdout
        self.original_stderr = sys.stderr
    
    def log(self, text: str):
        """Append text to the output widget."""
        # Handle progress bar updates (lines ending with \r)
        if text.endswith('\r'):
            # Progress bar update - replace last line
            cursor = self.output.textCursor()
            cursor.movePosition(cursor.MoveOperation.End)
            # Move to start of current line
            cursor.movePosition(cursor.MoveOperation.StartOfLine)
            cursor.movePosition(cursor.MoveOperation.End, cursor.MoveMode.KeepAnchor)
            cursor.removeSelectedText()
            # Insert new progress text (remove \r)
            self.output.insertPlainText(text.rstrip('\r'))
        elif text.strip():  # Regular text (skip empty lines)
            self.output.append(text.rstrip())
        # Auto-scroll to bottom
        scrollbar = self.output.verticalScrollBar()
        scrollbar.setValue(scrollbar.maximum())
    
    def clear_output(self):
        """Clear the output widget."""
        self.output.clear()
    
    def _run_in_thread(self, func, success_msg, buttons_to_disable):
        """Helper method to run a function in a thread with output capture."""
        for btn in buttons_to_disable:
            btn.setEnabled(False)
        
        def run():
            try:
                # Redirect stdout/stderr to widget
                sys.stdout = self.output_stream
                sys.stderr = self.output_stream
                
                # Run the function
                func()
                
                self.output.append("")
                self.output.append("=" * 70)
                self.output.append(success_msg)
                self.output.append("=" * 70)
                self.output.append("")
                
            except Exception as e:
                self.output.append("")
                self.output.append("=" * 70)
                self.output.append(f"ERROR: {str(e)}")
                self.output.append("=" * 70)
                import traceback
                self.output.append(traceback.format_exc())
            finally:
                # Restore original stdout/stderr
                sys.stdout = self.original_stdout
                sys.stderr = self.original_stderr
                
                # Re-enable buttons on the main thread
                # Use QTimer to ensure we're on the main thread
                def re_enable_buttons():
                    for btn in buttons_to_disable:
                        btn.setEnabled(True)
                
                QtCore.QTimer.singleShot(0, re_enable_buttons)
        
        thread = threading.Thread(target=run, daemon=True)
        thread.start()
    
    def run_load_and_process(self):
        """Run steps 1-4: Load, tag, clean, and combine data."""
        self.output.append("=" * 70)
        self.output.append("STEP 1: LOADING AND PROCESSING DATA")
        self.output.append("=" * 70)
        self.output.append("")
        
        def execute():
            self.pipeline_data = load_data()
            self.pipeline_data = tag(self.pipeline_data)
            self.pipeline_data = clean(self.pipeline_data)
            self.pipeline_data = combine(self.pipeline_data)
        
        self._run_in_thread(
            execute,
            "LOAD & PROCESS COMPLETED SUCCESSFULLY",
            [self.load_process_btn]
        )
    
    def run_labeling(self):
        """Run step 5: Label data with OpenAI."""
        if self.pipeline_data is None or 'combined' not in self.pipeline_data:
            self.output.append("ERROR: Please run 'Load & Process' first!")
            return
        
        self.output.append("=" * 70)
        self.output.append("STEP 2: LABELING DATA WITH OPENAI")
        self.output.append("=" * 70)
        self.output.append("")
        
        def execute():
            self.pipeline_data = label(self.pipeline_data)
            # Import CSV to active workbook
            csv_path = OUTPUT_DATA_DIR / 'labeled_data.csv'
            _import_csv_to_active_workbook(csv_path, "labeled_data")
        
        self._run_in_thread(
            execute,
            "LABELING COMPLETED SUCCESSFULLY",
            [self.label_btn]
        )
    
    def run_charts(self):
        """Run step 6: Generate charts and statistics."""
        if self.pipeline_data is None or 'labeled' not in self.pipeline_data:
            self.output.append("ERROR: Please run 'Label Data' first!")
            return
        
        self.output.append("=" * 70)
        self.output.append("STEP 3: GENERATING CHARTS AND STATISTICS")
        self.output.append("=" * 70)
        self.output.append("")
        
        def execute():
            analyze(self.pipeline_data, suppress_show=True)
            self.output.append("")
            self.output.append(f"Output files saved to: {OUTPUT_DATA_DIR}")
            self.output.append("")
            self.output.append("Charts saved:")
            self.output.append(f"  - {OUTPUT_DATA_DIR / 'spending_by_spender.png'}")
            self.output.append(f"  - {OUTPUT_DATA_DIR / 'spending_by_category.png'}")
            self.output.append(f"  - {OUTPUT_DATA_DIR / 'spending_by_vendor.png'}")
            
            # Import summary.csv to active workbook
            summary_csv_path = OUTPUT_DATA_DIR / 'summary.csv'
            if summary_csv_path.exists():
                _import_csv_to_active_workbook(summary_csv_path, "Summary")
                
                # Insert charts into the Summary sheet
                try:
                    app = xl_app()
                    wb = app.ActiveWorkbook
                    if wb is not None:
                        # Get the Summary sheet
                        try:
                            summary_ws = wb.Worksheets("Summary")
                            
                            # Find the last used row to place charts below the data
                            used_range = summary_ws.UsedRange
                            if used_range is not None:
                                last_row = used_range.Row + used_range.Rows.Count
                                # Add some spacing
                                chart_start_row = last_row + 3
                            else:
                                chart_start_row = 1
                            
                            # Chart files to insert (starting from column A)
                            chart_files = [
                                ("spending_by_spender.png", "A"),
                                ("spending_by_category.png", "C"),
                                ("spending_by_vendor.png", "H"),
                            ]
                            
                            # Add chart titles and images
                            for i, (chart_file, col) in enumerate(chart_files):
                                chart_path = OUTPUT_DATA_DIR / chart_file
                                if chart_path.exists():
                                    # Add title
                                    title_cell = f"{col}{chart_start_row - 1}"
                                    summary_ws.Range(title_cell).Value = chart_file.replace(".png", "").replace("_", " ").title()
                                    summary_ws.Range(title_cell).Font.Bold = True
                                    
                                    # Add chart image
                                    anchor_cell = f"{col}{chart_start_row}"
                                    _add_picture_to_sheet(summary_ws, chart_path, anchor_cell, scale=0.5)
                            
                            self.output.append("")
                            self.output.append("Charts inserted into Summary sheet")
                        except Exception as e:
                            print(f"Error inserting charts: {e}")
                            self.output.append(f"Note: Could not insert charts into Summary sheet: {e}")
                except Exception as e:
                    print(f"Error accessing workbook for chart insertion: {e}")
        
        self._run_in_thread(
            execute,
            "CHARTS GENERATED SUCCESSFULLY",
            [self.charts_btn]
        )
    
    def run_monte_carlo(self):
        """Run Monte Carlo simulation on labeled data."""
        if self.pipeline_data is None or 'labeled' not in self.pipeline_data:
            self.output.append("ERROR: Please run 'Label Data' first!")
            return
        
        self.output.append("=" * 70)
        self.output.append("MONTE CARLO SIMULATION")
        self.output.append("=" * 70)
        self.output.append("")
        
        def execute():
            results_df, summary_dict = run_monte_carlo_simulation(self.pipeline_data['labeled'])
            _write_monte_carlo_results_to_excel(results_df, summary_dict, "Monte Carlo Results")
            self.output.append("")
            self.output.append("Monte Carlo simulation completed successfully!")
        
        self._run_in_thread(
            execute,
            "MONTE CARLO SIMULATION COMPLETED",
            [self.monte_carlo_btn]
        )



# ---------------------------
# Macros
# ---------------------------
@xl_macro
def show_sidebar_v2():
    """Show the pipeline runner widget in a Custom Task Pane."""
    global _ctp
    
    try:
        _ensure_qt()
        
        if _ctp is None:
            widget = PipelineWidget()
            _ctp = create_ctp(widget, width=600)
        
        return "Sidebar shown successfully!"
    except Exception as e:
        import traceback
        error_msg = f"Error showing pipeline runner: {str(e)}\n{traceback.format_exc()}"
        print(error_msg)
        return error_msg

