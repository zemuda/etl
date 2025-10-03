# silver_sales_transformer_enhanced.py
import duckdb
from datetime import datetime
from typing import Dict, List, Tuple, Optional
import logging
import re


class SilverSalesTransformerEnhanced:
    def __init__(self, db_path: str = ":memory:"):
        self.conn = duckdb.connect(db_path)
        self._init_silver_schema()
        self.whitespace_log = []

    def _init_silver_schema(self):
        """Initialize enhanced silver sales schema with data quality tracking"""
        init_scripts = [
            "CREATE SCHEMA IF NOT EXISTS silver_sales;",
            """
            CREATE TABLE IF NOT EXISTS silver_sales.process_log (
                process_id INTEGER PRIMARY KEY,
                source_table VARCHAR,
                target_table VARCHAR,
                records_processed INTEGER,
                records_valid INTEGER,
                records_invalid INTEGER,
                credit_notes_separated INTEGER,
                duplicates_removed INTEGER,
                whitespace_issues_found INTEGER,
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                status VARCHAR,
                error_message VARCHAR
            );
            """,
            "CREATE SEQUENCE IF NOT EXISTS silver_sales.process_id_seq;",
            """
            CREATE TABLE IF NOT EXISTS silver_sales.data_quality_log (
                log_id INTEGER PRIMARY KEY,
                process_id INTEGER,
                table_name VARCHAR,
                column_name VARCHAR,
                issue_type VARCHAR,
                issue_description VARCHAR,
                affected_rows INTEGER,
                sample_data VARCHAR,
                logged_at TIMESTAMP
            );
            """,
            "CREATE SEQUENCE IF NOT EXISTS silver_sales.data_quality_log_seq;",
        ]

        for script in init_scripts:
            try:
                self.conn.execute(script)
            except Exception as e:
                logging.warning(f"Could not initialize silver sales schema: {e}")

    def log_data_quality_issue(
        self,
        process_id: int,
        table_name: str,
        column_name: str,
        issue_type: str,
        description: str,
        affected_rows: int,
        sample_data: str = "",
    ):
        """Log data quality issues for monitoring"""
        try:
            log_id = self.conn.execute(
                "SELECT nextval('silver_sales.data_quality_log_seq')"
            ).fetchone()[0]

            self.conn.execute(
                """
                INSERT INTO silver_sales.data_quality_log 
                (log_id, process_id, table_name, column_name, issue_type, issue_description, affected_rows, sample_data, logged_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    log_id,
                    process_id,
                    table_name,
                    column_name,
                    issue_type,
                    description,
                    affected_rows,
                    sample_data,
                    datetime.now(),
                ),
            )
        except Exception as e:
            logging.error(f"Failed to log data quality issue: {e}")

    def detect_and_log_trailing_whitespace(
        self, table_name: str, process_id: int
    ) -> Dict[str, int]:
        """Detect and log columns with trailing whitespace"""
        whitespace_issues = {}

        # Get all string columns from the table
        columns_result = self.conn.execute(
            f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = '{table_name.replace("'", "''")}'
            AND data_type LIKE '%VARCHAR%'
        """
        ).fetchall()

        string_columns = [col[0] for col in columns_result]

        for column in string_columns:
            try:
                # Count rows with trailing whitespace
                result = self.conn.execute(
                    f"""
                    SELECT COUNT(*) 
                    FROM {table_name} 
                    WHERE {column} LIKE '% '
                    OR {column} LIKE ' %'
                    OR {column} LIKE '%\\t'
                    OR {column} LIKE '\\t%'
                """
                ).fetchone()

                if result[0] > 0:
                    whitespace_issues[column] = result[0]

                    # Get sample data with issues
                    sample = self.conn.execute(
                        f"""
                        SELECT {column} 
                        FROM {table_name} 
                        WHERE {column} LIKE '% '
                        OR {column} LIKE ' %'
                        OR {column} LIKE '%\\t'
                        OR {column} LIKE '\\t%'
                        LIMIT 5
                    """
                    ).fetchall()

                    sample_data = ", ".join([str(s[0]) for s in sample])

                    self.log_data_quality_issue(
                        process_id,
                        table_name,
                        column,
                        "TRAILING_WHITESPACE",
                        f"Found {result[0]} rows with trailing/leading whitespace",
                        result[0],
                        sample_data,
                    )

            except Exception as e:
                logging.warning(f"Could not check whitespace for column {column}: {e}")

        return whitespace_issues

    def remove_whitespace_from_table(self, table_name: str):
        """Remove trailing and leading whitespace from all string columns"""
        # Get all string columns
        columns_result = self.conn.execute(
            f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = '{table_name.replace("'", "''")}'
            AND data_type LIKE '%VARCHAR%'
        """
        ).fetchall()

        string_columns = [col[0] for col in columns_result]

        if not string_columns:
            return 0

        # Build UPDATE statement to trim all string columns
        set_clauses = []
        for column in string_columns:
            set_clauses.append(f"{column} = TRIM({column})")

        update_sql = f"""
            UPDATE {table_name} 
            SET {', '.join(set_clauses)}
            WHERE 1=1
        """

        try:
            self.conn.execute(update_sql)
            return len(string_columns)
        except Exception as e:
            logging.error(f"Failed to remove whitespace from {table_name}: {e}")
            return 0

    def detect_duplicates(self, table_name: str, key_columns: List[str]) -> int:
        """Detect and count duplicate rows based on key columns"""
        key_columns_str = ", ".join(key_columns)

        result = self.conn.execute(
            f"""
            SELECT COUNT(*) - COUNT(DISTINCT {key_columns_str}) as duplicate_count
            FROM {table_name}
        """
        ).fetchone()

        return result[0] if result else 0

    def remove_duplicates(self, table_name: str, key_columns: List[str]) -> int:
        """Remove duplicate rows keeping the first occurrence"""
        key_columns_str = ", ".join(key_columns)
        columns_result = self.conn.execute(
            f"PRAGMA table_info({table_name})"
        ).fetchall()
        all_columns = [col[1] for col in columns_result]
        all_columns_str = ", ".join(all_columns)

        # Create temporary table with distinct rows
        temp_table = f"temp_{table_name}_{datetime.now().strftime('%Y%m%d%H%M%S')}"

        try:
            # Create temporary table with distinct rows
            self.conn.execute(
                f"""
                CREATE TEMPORARY TABLE {temp_table} AS
                SELECT DISTINCT ON ({key_columns_str}) {all_columns_str}
                FROM {table_name}
                ORDER BY {key_columns_str}
            """
            )

            # Count duplicates removed
            original_count = self.conn.execute(
                f"SELECT COUNT(*) FROM {table_name}"
            ).fetchone()[0]
            new_count = self.conn.execute(
                f"SELECT COUNT(*) FROM {temp_table}"
            ).fetchone()[0]
            duplicates_removed = original_count - new_count

            # Replace original table
            self.conn.execute(f"DELETE FROM {table_name}")
            self.conn.execute(f"INSERT INTO {table_name} SELECT * FROM {temp_table}")
            self.conn.execute(f"DROP TABLE {temp_table}")

            return duplicates_removed

        except Exception as e:
            logging.error(f"Failed to remove duplicates from {table_name}: {e}")
            # Clean up temporary table if it exists
            try:
                self.conn.execute(f"DROP TABLE IF EXISTS {temp_table}")
            except:
                pass
            return 0

    def create_silver_summarized_invoices(self):
        """Create silver layer for summarized invoices with comprehensive data cleaning"""
        process_id = self.conn.execute(
            "SELECT nextval('silver_sales.process_id_seq')"
        ).fetchone()[0]
        started_at = datetime.now()

        try:
            self.conn.execute(
                """
                INSERT INTO silver_sales.process_log 
                (process_id, source_table, target_table, started_at, status)
                VALUES (?, 'bronze_sales_cash_invoices_summarized', 'silver_sales.summarized_invoices', ?, 'STARTED')
                """,
                (process_id, started_at),
            )

            # Data cleaning phase 1: Remove whitespace
            whitespace_columns_fixed = self.remove_whitespace_from_table(
                "bronze_sales_cash_invoices_summarized"
            )

            # Data cleaning phase 2: Detect and log whitespace issues
            whitespace_issues = self.detect_and_log_trailing_whitespace(
                "bronze_sales_cash_invoices_summarized", process_id
            )

            # Data cleaning phase 3: Remove duplicates
            duplicates_removed = self.remove_duplicates(
                "bronze_sales_cash_invoices_summarized",
                ["DOCUMENT_NUMBER", "CUS_CODE", "TRN_DATE", "AMOUNT"],
            )

            # Log data quality findings
            if whitespace_issues:
                logging.info(
                    f"Found whitespace issues in {len(whitespace_issues)} columns"
                )
            if duplicates_removed > 0:
                logging.info(f"Removed {duplicates_removed} duplicate rows")
                self.log_data_quality_issue(
                    process_id,
                    "bronze_sales_cash_invoices_summarized",
                    "ALL",
                    "DUPLICATES_REMOVED",
                    f"Removed {duplicates_removed} duplicate rows",
                    duplicates_removed,
                    "",
                )

            # Detect credit notes and negative amounts for separation
            total_records, credit_notes_count = self.detect_credit_notes(
                "bronze_sales_cash_invoices_summarized"
            )

            # Create main invoices table (positive amounts only)
            self.conn.execute(
                """
                CREATE OR REPLACE TABLE silver_sales.summarized_invoices AS
                WITH cleaned_data AS (
                    SELECT 
                        -- Invoice identification with primary key
                        TRY_CAST(DOCUMENT_NUMBER AS VARCHAR) AS invoice_number,
                        CASE 
                            WHEN DOCUMENT_NUMBER LIKE 'CN%' OR AMOUNT < 0 THEN 'CREDIT_NOTE'
                            ELSE 'INVOICE'
                        END AS document_type,
                        
                        -- Customer information
                        TRY_CAST(TRIM(CUS_CODE) AS VARCHAR) AS customer_code,
                        TRY_CAST(TRIM(CUSTOMER_NAME) AS VARCHAR) AS customer_name,
                        
                        -- Date handling
                        CASE 
                            WHEN TRY_CAST(TRN_DATE AS DATE) IS NOT NULL THEN TRY_CAST(TRN_DATE AS DATE)
                            WHEN TRY_CAST(SUBSTR(TRN_DATE, 1, 10) AS DATE) IS NOT NULL THEN TRY_CAST(SUBSTR(TRN_DATE, 1, 10) AS DATE)
                            ELSE DATE '1900-01-01'
                        END AS transaction_date,
                        
                        TRY_CAST(TRN_YEAR AS INTEGER) AS transaction_year,
                        TRY_CAST(TRN_MONTH AS INTEGER) AS transaction_month,
                        
                        -- Amount handling (only positive amounts for main table)
                        CASE 
                            WHEN DOCUMENT_NUMBER LIKE 'CN%' OR AMOUNT < 0 THEN ABS(TRY_CAST(AMOUNT AS DECIMAL(15,2)))
                            ELSE TRY_CAST(AMOUNT AS DECIMAL(15,2))
                        END AS amount,
                        
                        TRY_CAST(AMOUNT_INCLUSIVE AS DECIMAL(15,2)) AS amount_inclusive,
                        TRY_CAST(AMOUNT_EXCLUSIVE AS DECIMAL(15,2)) AS amount_exclusive,
                        TRY_CAST(VAT AS DECIMAL(15,2)) AS vat_amount,
                        TRY_CAST(DISC AS DECIMAL(15,2)) AS discount_amount,
                        
                        -- Branch information
                        TRY_CAST(BCODE AS INTEGER) AS branch_code,
                        TRY_CAST(TRIM(USERNAME) AS VARCHAR) AS cashier_username,
                        TRY_CAST(TRIM(BRANCH_NAME) AS VARCHAR) AS branch_name,
                        TRY_CAST(TRIM(MPESA_TILL) AS VARCHAR) AS mpesa_till_number,
                        
                        -- Contact information
                        REGEXP_REPLACE(TRIM(TELEPHONE), '[^0-9]', '') AS customer_phone,
                        LOWER(TRIM(EMAIL)) AS customer_email,
                        TRY_CAST(TRIM(COMPANY) AS VARCHAR) AS company_name,
                        TRY_CAST(TRIM(ADDRESS) AS VARCHAR) AS customer_address,
                        
                        -- Source system
                        TRY_CAST(TRIM(_source_sheet) AS VARCHAR) AS source_system,
                        TRY_CAST(_processed_at AS TIMESTAMP) AS source_processed_at,
                        
                        -- Data quality flags
                        CASE WHEN TRY_CAST(AMOUNT AS DECIMAL(15,2)) IS NULL THEN 1 ELSE 0 END AS amount_invalid_flag,
                        CASE WHEN TRY_CAST(TRN_DATE AS DATE) IS NULL THEN 1 ELSE 0 END AS date_invalid_flag,
                        CASE WHEN TRIM(CUS_CODE) = '' OR CUS_CODE IS NULL THEN 1 ELSE 0 END AS customer_missing_flag,
                        
                        -- Bronze layer metadata
                        source_file_path,
                        load_timestamp AS bronze_load_timestamp,
                        batch_id AS bronze_batch_id
                        
                    FROM bronze_sales_cash_invoices_summarized
                    WHERE DOCUMENT_NUMBER IS NOT NULL
                    AND TRY_CAST(AMOUNT AS DECIMAL(15,2)) > 0  -- Exclude negative amounts and zero
                )
                SELECT *,
                    -- Silver layer metadata
                    CURRENT_TIMESTAMP AS silver_load_timestamp,
                    {} AS silver_process_id,
                    
                    -- Business logic flags
                    CASE 
                        WHEN document_type = 'CREDIT_NOTE' THEN 1
                        ELSE 0
                    END AS is_credit_note,
                    
                    -- Calculated fields
                    amount_inclusive - amount_exclusive AS vat_calculated,
                    CASE 
                        WHEN amount_exclusive > 0 THEN (discount_amount / amount_exclusive) * 100 
                        ELSE 0 
                    END AS discount_percentage
                    
                FROM cleaned_data
                """.format(
                    process_id
                )
            )

            # Create separate credit notes table for negative amounts
            self.conn.execute(
                """
                CREATE OR REPLACE TABLE silver_sales.summarized_credit_notes AS
                WITH credit_data AS (
                    SELECT 
                        TRY_CAST(DOCUMENT_NUMBER AS VARCHAR) AS credit_note_number,
                        TRY_CAST(TRIM(CUS_CODE) AS VARCHAR) AS customer_code,
                        TRY_CAST(TRIM(CUSTOMER_NAME) AS VARCHAR) AS customer_name,
                        CASE 
                            WHEN TRY_CAST(TRN_DATE AS DATE) IS NOT NULL THEN TRY_CAST(TRN_DATE AS DATE)
                            WHEN TRY_CAST(SUBSTR(TRN_DATE, 1, 10) AS DATE) IS NOT NULL THEN TRY_CAST(SUBSTR(TRN_DATE, 1, 10) AS DATE)
                            ELSE DATE '1900-01-01'
                        END AS transaction_date,
                        TRY_CAST(TRN_YEAR AS INTEGER) AS transaction_year,
                        TRY_CAST(TRN_MONTH AS INTEGER) AS transaction_month,
                        ABS(TRY_CAST(AMOUNT AS DECIMAL(15,2))) AS credit_amount,
                        ABS(TRY_CAST(AMOUNT_INCLUSIVE AS DECIMAL(15,2))) AS credit_amount_inclusive,
                        ABS(TRY_CAST(AMOUNT_EXCLUSIVE AS DECIMAL(15,2))) AS credit_amount_exclusive,
                        ABS(TRY_CAST(VAT AS DECIMAL(15,2))) AS credit_vat_amount,
                        ABS(TRY_CAST(DISC AS DECIMAL(15,2))) AS credit_discount_amount,
                        TRY_CAST(BCODE AS INTEGER) AS branch_code,
                        TRY_CAST(TRIM(USERNAME) AS VARCHAR) AS cashier_username,
                        TRY_CAST(TRIM(BRANCH_NAME) AS VARCHAR) AS branch_name,
                        TRY_CAST(TRIM(MPESA_TILL) AS VARCHAR) AS mpesa_till_number,
                        REGEXP_REPLACE(TRIM(TELEPHONE), '[^0-9]', '') AS customer_phone,
                        LOWER(TRIM(EMAIL)) AS customer_email,
                        TRY_CAST(TRIM(COMPANY) AS VARCHAR) AS company_name,
                        TRY_CAST(TRIM(ADDRESS) AS VARCHAR) AS customer_address,
                        TRY_CAST(TRIM(_source_sheet) AS VARCHAR) AS source_system,
                        source_file_path,
                        load_timestamp AS bronze_load_timestamp,
                        batch_id AS bronze_batch_id
                    FROM bronze_sales_cash_invoices_summarized
                    WHERE DOCUMENT_NUMBER IS NOT NULL
                    AND TRY_CAST(AMOUNT AS DECIMAL(15,2)) <= 0  -- Only negative amounts and zero
                )
                SELECT *,
                    CURRENT_TIMESTAMP AS silver_load_timestamp,
                    {} AS silver_process_id,
                    CASE 
                        WHEN credit_amount = 0 THEN 'ZERO_AMOUNT'
                        ELSE 'CREDIT_NOTE'
                    END AS credit_note_type
                FROM credit_data
                """.format(
                    process_id
                )
            )

            # Get statistics
            invoice_stats = self.conn.execute(
                "SELECT COUNT(*) as invoice_count FROM silver_sales.summarized_invoices"
            ).fetchone()

            credit_stats = self.conn.execute(
                "SELECT COUNT(*) as credit_note_count FROM silver_sales.summarized_credit_notes"
            ).fetchone()

            # Update process log with comprehensive metrics
            self.conn.execute(
                """
                UPDATE silver_sales.process_log 
                SET completed_at = ?, 
                    records_processed = ?,
                    records_valid = ?,
                    credit_notes_separated = ?,
                    duplicates_removed = ?,
                    whitespace_issues_found = ?,
                    status = 'COMPLETED'
                WHERE process_id = ?
                """,
                (
                    datetime.now(),
                    total_records,
                    invoice_stats[0],
                    credit_stats[0],
                    duplicates_removed,
                    len(whitespace_issues),
                    process_id,
                ),
            )

            logging.info(
                f"✅ Summarized Invoices: {invoice_stats[0]:,} invoices, {credit_stats[0]:,} credit notes/zero amounts"
            )
            logging.info(
                f"   Removed {duplicates_removed} duplicates, found {len(whitespace_issues)} columns with whitespace issues"
            )
            return True

        except Exception as e:
            self.conn.execute(
                """
                UPDATE silver_sales.process_log 
                SET completed_at = ?, status = 'FAILED', error_message = ?
                WHERE process_id = ?
                """,
                (datetime.now(), str(e), process_id),
            )
            logging.error(f"❌ Failed to create summarized silver layer: {e}")
            return False

    def create_silver_detailed_invoices(self):
        """Create silver layer for detailed invoices with primary key and comprehensive data cleaning"""
        process_id = self.conn.execute(
            "SELECT nextval('silver_sales.process_id_seq')"
        ).fetchone()[0]
        started_at = datetime.now()

        try:
            # Check if detailed table exists
            table_exists = self.conn.execute(
                """
                SELECT 1 FROM information_schema.tables 
                WHERE table_name = 'bronze_sales_cash_invoices_detailed'
                """
            ).fetchone()

            if not table_exists:
                logging.info("ℹ️ Detailed invoices table not found, skipping...")
                return True

            self.conn.execute(
                """
                INSERT INTO silver_sales.process_log 
                (process_id, source_table, target_table, started_at, status)
                VALUES (?, 'bronze_sales_cash_invoices_detailed', 'silver_sales.detailed_invoices', ?, 'STARTED')
                """,
                (process_id, started_at),
            )

            # Data cleaning phase 1: Remove whitespace
            whitespace_columns_fixed = self.remove_whitespace_from_table(
                "bronze_sales_cash_invoices_detailed"
            )

            # Data cleaning phase 2: Detect and log whitespace issues
            whitespace_issues = self.detect_and_log_trailing_whitespace(
                "bronze_sales_cash_invoices_detailed", process_id
            )

            # Data cleaning phase 3: Remove duplicates using composite primary key
            duplicates_removed = self.remove_duplicates(
                "bronze_sales_cash_invoices_detailed",
                ["DOCUMENT_NUMBER", "ITEM_CODE", "TRN_DATE", "AMOUNT"],
            )

            # Detect credit notes
            total_records, credit_notes_count = self.detect_credit_notes(
                "bronze_sales_cash_invoices_detailed"
            )

            # Create detailed invoices table with primary key
            self.conn.execute(
                """
                CREATE OR REPLACE TABLE silver_sales.detailed_invoices AS
                WITH cleaned_data AS (
                    SELECT 
                        -- Primary key components
                        TRY_CAST(DOCUMENT_NUMBER AS VARCHAR) AS invoice_number,
                        TRY_CAST(TRIM(ITEM_CODE) AS VARCHAR) AS product_code,
                        
                        -- Composite primary key
                        invoice_number || '_' || product_code AS invoice_line_id,
                        
                        -- Document type
                        CASE 
                            WHEN DOCUMENT_NUMBER LIKE 'CN%' OR AMOUNT < 0 THEN 'CREDIT_NOTE'
                            ELSE 'INVOICE'
                        END AS document_type,
                        
                        -- Customer information
                        TRY_CAST(TRIM(CUS_CODE) AS VARCHAR) AS customer_code,
                        TRY_CAST(TRIM(CUSTOMER_NAME) AS VARCHAR) AS customer_name,
                        
                        -- Date handling
                        CASE 
                            WHEN TRY_CAST(TRN_DATE AS DATE) IS NOT NULL THEN TRY_CAST(TRN_DATE AS DATE)
                            WHEN TRY_CAST(SUBSTR(TRN_DATE, 1, 10) AS DATE) IS NOT NULL THEN TRY_CAST(SUBSTR(TRN_DATE, 1, 10) AS DATE)
                            ELSE DATE '1900-01-01'
                        END AS transaction_date,
                        
                        TRY_CAST(TRN_YEAR AS INTEGER) AS transaction_year,
                        TRY_CAST(TRN_MONTH AS INTEGER) AS transaction_month,
                        
                        -- Amount handling (positive amounts only for main table)
                        CASE 
                            WHEN DOCUMENT_NUMBER LIKE 'CN%' OR AMOUNT < 0 THEN ABS(TRY_CAST(AMOUNT AS DECIMAL(15,2)))
                            ELSE TRY_CAST(AMOUNT AS DECIMAL(15,2))
                        END AS amount,
                        
                        TRY_CAST(AMOUNT_INCLUSIVE AS DECIMAL(15,2)) AS amount_inclusive,
                        TRY_CAST(AMOUNT_EXCLUSIVE AS DECIMAL(15,2)) AS amount_exclusive,
                        TRY_CAST(VAT AS DECIMAL(15,2)) AS vat_amount,
                        TRY_CAST(DISC AS DECIMAL(15,2)) AS discount_amount,
                        TRY_CAST(TAX_AMOUNT AS DECIMAL(15,2)) AS tax_amount,
                        
                        -- Line item details
                        TRY_CAST(QUANTITY AS DECIMAL(10,2)) AS quantity,
                        TRY_CAST(DISCOUNT_PERCENT AS DECIMAL(5,2)) AS discount_percent,
                        TRY_CAST(DISCOUNT AS DECIMAL(15,2)) AS line_discount,
                        TRY_CAST(AMOUNT_VALUE AS DECIMAL(15,2)) AS amount_value,
                        TRY_CAST(AMT_EXCL AS DECIMAL(15,2)) AS amount_excl,
                        TRY_CAST(TRIM(DESCRIPTION) AS VARCHAR) AS product_description,
                        TRY_CAST(ITEM_PRICE AS DECIMAL(15,2)) AS unit_price,
                        TRY_CAST(TRIM(PW) AS VARCHAR) AS price_weight,
                        TRY_CAST(PWQTY AS DECIMAL(10,2)) AS price_weight_quantity,
                        TRY_CAST(TRIM(TRN_TYPE) AS VARCHAR) AS transaction_type,
                        
                        -- Branch information
                        TRY_CAST(BCODE AS INTEGER) AS branch_code,
                        TRY_CAST(TRIM(USERNAME) AS VARCHAR) AS cashier_username,
                        TRY_CAST(TRIM(BRANCH_NAME) AS VARCHAR) AS branch_name,
                        TRY_CAST(TRIM(MPESA_TILL) AS VARCHAR) AS mpesa_till_number,
                        
                        -- Contact information
                        REGEXP_REPLACE(TRIM(TELEPHONE), '[^0-9]', '') AS customer_phone,
                        LOWER(TRIM(EMAIL)) AS customer_email,
                        TRY_CAST(TRIM(COMPANY) AS VARCHAR) AS company_name,
                        TRY_CAST(TRIM(ADDRESS) AS VARCHAR) AS customer_address,
                        
                        -- Source system
                        TRY_CAST(TRIM(_source_sheet) AS VARCHAR) AS source_system,
                        TRY_CAST(_processed_at AS TIMESTAMP) AS source_processed_at,
                        
                        -- Data quality flags
                        CASE WHEN TRY_CAST(AMOUNT AS DECIMAL(15,2)) IS NULL THEN 1 ELSE 0 END AS amount_invalid_flag,
                        CASE WHEN TRY_CAST(TRN_DATE AS DATE) IS NULL THEN 1 ELSE 0 END AS date_invalid_flag,
                        CASE WHEN TRIM(CUS_CODE) = '' OR CUS_CODE IS NULL THEN 1 ELSE 0 END AS customer_missing_flag,
                        CASE WHEN TRY_CAST(QUANTITY AS DECIMAL(10,2)) IS NULL OR TRY_CAST(QUANTITY AS DECIMAL(10,2)) <= 0 THEN 1 ELSE 0 END AS quantity_invalid_flag,
                        CASE WHEN TRY_CAST(ITEM_PRICE AS DECIMAL(15,2)) IS NULL OR TRY_CAST(ITEM_PRICE AS DECIMAL(15,2)) <= 0 THEN 1 ELSE 0 END AS price_invalid_flag,
                        
                        -- Bronze layer metadata
                        source_file_path,
                        load_timestamp AS bronze_load_timestamp,
                        batch_id AS bronze_batch_id
                        
                    FROM bronze_sales_cash_invoices_detailed
                    WHERE DOCUMENT_NUMBER IS NOT NULL
                    AND TRIM(ITEM_CODE) IS NOT NULL
                    AND TRY_CAST(AMOUNT AS DECIMAL(15,2)) > 0  -- Exclude negative amounts and zero
                )
                SELECT *,
                    -- Silver layer metadata
                    CURRENT_TIMESTAMP AS silver_load_timestamp,
                    {} AS silver_process_id,
                    
                    -- Business logic flags
                    CASE 
                        WHEN document_type = 'CREDIT_NOTE' THEN 1
                        ELSE 0
                    END AS is_credit_note,
                    
                    -- Line item calculations
                    quantity * unit_price AS calculated_line_total,
                    CASE 
                        WHEN quantity > 0 AND unit_price > 0 THEN quantity * unit_price
                        ELSE amount
                    END AS effective_amount,
                    unit_price * (1 - COALESCE(discount_percent, 0) / 100) AS discounted_unit_price,
                    quantity * unit_price * (1 - COALESCE(discount_percent, 0) / 100) AS net_line_amount
                    
                FROM cleaned_data
                """.format(
                    process_id
                )
            )

            # Create separate detailed credit notes table
            self.conn.execute(
                """
                CREATE OR REPLACE TABLE silver_sales.detailed_credit_notes AS
                WITH credit_data AS (
                    SELECT 
                        -- Primary key components
                        TRY_CAST(DOCUMENT_NUMBER AS VARCHAR) AS credit_note_number,
                        TRY_CAST(TRIM(ITEM_CODE) AS VARCHAR) AS product_code,
                        credit_note_number || '_' || product_code AS credit_line_id,
                        
                        TRY_CAST(TRIM(CUS_CODE) AS VARCHAR) AS customer_code,
                        TRY_CAST(TRIM(CUSTOMER_NAME) AS VARCHAR) AS customer_name,
                        CASE 
                            WHEN TRY_CAST(TRN_DATE AS DATE) IS NOT NULL THEN TRY_CAST(TRN_DATE AS DATE)
                            WHEN TRY_CAST(SUBSTR(TRN_DATE, 1, 10) AS DATE) IS NOT NULL THEN TRY_CAST(SUBSTR(TRN_DATE, 1, 10) AS DATE)
                            ELSE DATE '1900-01-01'
                        END AS transaction_date,
                        TRY_CAST(TRN_YEAR AS INTEGER) AS transaction_year,
                        TRY_CAST(TRN_MONTH AS INTEGER) AS transaction_month,
                        ABS(TRY_CAST(AMOUNT AS DECIMAL(15,2))) AS credit_amount,
                        ABS(TRY_CAST(AMOUNT_INCLUSIVE AS DECIMAL(15,2))) AS credit_amount_inclusive,
                        ABS(TRY_CAST(AMOUNT_EXCLUSIVE AS DECIMAL(15,2))) AS credit_amount_exclusive,
                        ABS(TRY_CAST(VAT AS DECIMAL(15,2))) AS credit_vat_amount,
                        ABS(TRY_CAST(DISC AS DECIMAL(15,2))) AS credit_discount_amount,
                        ABS(TRY_CAST(TAX_AMOUNT AS DECIMAL(15,2))) AS credit_tax_amount,
                        TRY_CAST(QUANTITY AS DECIMAL(10,2)) AS credit_quantity,
                        TRY_CAST(DISCOUNT_PERCENT AS DECIMAL(5,2)) AS credit_discount_percent,
                        ABS(TRY_CAST(DISCOUNT AS DECIMAL(15,2))) AS credit_line_discount,
                        TRY_CAST(TRIM(DESCRIPTION) AS VARCHAR) AS credit_product_description,
                        TRY_CAST(ITEM_PRICE AS DECIMAL(15,2)) AS credit_unit_price,
                        TRY_CAST(TRIM(PW) AS VARCHAR) AS credit_price_weight,
                        TRY_CAST(PWQTY AS DECIMAL(10,2)) AS credit_price_weight_quantity,
                        TRY_CAST(TRIM(TRN_TYPE) AS VARCHAR) AS credit_transaction_type,
                        TRY_CAST(BCODE AS INTEGER) AS branch_code,
                        TRY_CAST(TRIM(USERNAME) AS VARCHAR) AS cashier_username,
                        TRY_CAST(TRIM(BRANCH_NAME) AS VARCHAR) AS branch_name,
                        TRY_CAST(TRIM(MPESA_TILL) AS VARCHAR) AS mpesa_till_number,
                        REGEXP_REPLACE(TRIM(TELEPHONE), '[^0-9]', '') AS customer_phone,
                        LOWER(TRIM(EMAIL)) AS customer_email,
                        TRY_CAST(TRIM(COMPANY) AS VARCHAR) AS company_name,
                        TRY_CAST(TRIM(ADDRESS) AS VARCHAR) AS customer_address,
                        TRY_CAST(TRIM(_source_sheet) AS VARCHAR) AS source_system,
                        source_file_path,
                        load_timestamp AS bronze_load_timestamp,
                        batch_id AS bronze_batch_id
                    FROM bronze_sales_cash_invoices_detailed
                    WHERE DOCUMENT_NUMBER IS NOT NULL
                    AND TRIM(ITEM_CODE) IS NOT NULL
                    AND TRY_CAST(AMOUNT AS DECIMAL(15,2)) <= 0  -- Only negative amounts and zero
                )
                SELECT *,
                    CURRENT_TIMESTAMP AS silver_load_timestamp,
                    {} AS silver_process_id,
                    CASE 
                        WHEN credit_amount = 0 THEN 'ZERO_AMOUNT'
                        ELSE 'CREDIT_NOTE'
                    END AS credit_note_type,
                    credit_quantity * credit_unit_price AS credit_calculated_total
                FROM credit_data
                """.format(
                    process_id
                )
            )

            # Get statistics
            invoice_stats = self.conn.execute(
                "SELECT COUNT(*) as invoice_count FROM silver_sales.detailed_invoices"
            ).fetchone()

            credit_stats = self.conn.execute(
                "SELECT COUNT(*) as credit_note_count FROM silver_sales.detailed_credit_notes"
            ).fetchone()

            self.conn.execute(
                """
                UPDATE silver_sales.process_log 
                SET completed_at = ?, 
                    records_processed = ?,
                    records_valid = ?,
                    credit_notes_separated = ?,
                    duplicates_removed = ?,
                    whitespace_issues_found = ?,
                    status = 'COMPLETED'
                WHERE process_id = ?
                """,
                (
                    datetime.now(),
                    total_records,
                    invoice_stats[0],
                    credit_stats[0],
                    duplicates_removed,
                    len(whitespace_issues),
                    process_id,
                ),
            )

            logging.info(
                f"✅ Detailed Invoices: {invoice_stats[0]:,} invoices, {credit_stats[0]:,} credit notes/zero amounts"
            )
            logging.info(
                f"   Removed {duplicates_removed} duplicates, found {len(whitespace_issues)} columns with whitespace issues"
            )
            return True

        except Exception as e:
            self.conn.execute(
                """
                UPDATE silver_sales.process_log 
                SET completed_at = ?, status = 'FAILED', error_message = ?
                WHERE process_id = ?
                """,
                (datetime.now(), str(e), process_id),
            )
            logging.error(f"❌ Failed to create detailed silver layer: {e}")
            return False

    def detect_credit_notes(self, table_name: str) -> Tuple[int, int]:
        """Detect credit notes and zero amounts in a bronze table"""
        try:
            result = self.conn.execute(
                f"""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(CASE WHEN DOCUMENT_NUMBER LIKE 'CN%' OR AMOUNT < 0 OR AMOUNT = 0 THEN 1 END) as credit_and_zero_notes
                FROM {table_name}
                """
            ).fetchone()
            return result
        except Exception as e:
            logging.error(f"Error detecting credit notes in {table_name}: {e}")
            return 0, 0

    def create_silver_daily_sales_summary(self):
        """Create daily sales summary combining both summarized and detailed data"""
        # Implementation similar to original but using new credit note tables
        process_id = self.conn.execute(
            "SELECT nextval('silver_sales.process_id_seq')"
        ).fetchone()[0]
        started_at = datetime.now()

        try:
            self.conn.execute(
                """
                INSERT INTO silver_sales.process_log 
                (process_id, source_table, target_table, started_at, status)
                VALUES (?, 'silver_sales tables', 'silver_sales.daily_sales_summary', ?, 'STARTED')
                """,
                (process_id, started_at),
            )

            self.conn.execute(
                """
                CREATE OR REPLACE TABLE silver_sales.daily_sales_summary AS
                WITH daily_invoices AS (
                    -- From summarized invoices
                    SELECT 
                        transaction_date,
                        transaction_year,
                        transaction_month,
                        branch_name,
                        branch_code,
                        cashier_username,
                        COUNT(*) AS invoice_count,
                        SUM(amount) AS total_sales,
                        SUM(amount_inclusive) AS total_sales_inclusive,
                        SUM(amount_exclusive) AS total_sales_exclusive,
                        SUM(vat_amount) AS total_vat,
                        SUM(discount_amount) AS total_discount,
                        COUNT(DISTINCT customer_code) AS unique_customers,
                        'SUMMARIZED' AS data_source
                    FROM silver_sales.summarized_invoices
                    GROUP BY transaction_date, transaction_year, transaction_month, branch_name, branch_code, cashier_username
                    
                    UNION ALL
                    
                    -- From detailed invoices
                    SELECT 
                        transaction_date,
                        transaction_year,
                        transaction_month,
                        branch_name,
                        branch_code,
                        cashier_username,
                        COUNT(DISTINCT invoice_number) AS invoice_count,
                        SUM(amount) AS total_sales,
                        SUM(amount_inclusive) AS total_sales_inclusive,
                        SUM(amount_exclusive) AS total_sales_exclusive,
                        SUM(vat_amount) AS total_vat,
                        SUM(discount_amount) AS total_discount,
                        COUNT(DISTINCT customer_code) AS unique_customers,
                        'DETAILED' AS data_source
                    FROM silver_sales.detailed_invoices
                    GROUP BY transaction_date, transaction_year, transaction_month, branch_name, branch_code, cashier_username
                ),
                daily_credits AS (
                    -- Credit notes from summarized
                    SELECT 
                        transaction_date,
                        branch_code,
                        COUNT(*) AS credit_note_count,
                        SUM(credit_amount) AS total_credits
                    FROM silver_sales.summarized_credit_notes
                    WHERE credit_note_type = 'CREDIT_NOTE'
                    GROUP BY transaction_date, branch_code
                    
                    UNION ALL
                    
                    -- Credit notes from detailed
                    SELECT 
                        transaction_date,
                        branch_code,
                        COUNT(*) AS credit_note_count,
                        SUM(credit_amount) AS total_credits
                    FROM silver_sales.detailed_credit_notes
                    WHERE credit_note_type = 'CREDIT_NOTE'
                    GROUP BY transaction_date, branch_code
                )
                SELECT 
                    di.transaction_date,
                    di.transaction_year,
                    di.transaction_month,
                    di.branch_name,
                    di.branch_code,
                    di.cashier_username,
                    di.data_source,
                    di.invoice_count,
                    di.total_sales,
                    di.total_sales_inclusive,
                    di.total_sales_exclusive,
                    di.total_vat,
                    di.total_discount,
                    di.unique_customers,
                    COALESCE(dc.credit_note_count, 0) AS credit_note_count,
                    COALESCE(dc.total_credits, 0) AS total_credits,
                    di.total_sales - COALESCE(dc.total_credits, 0) AS net_sales,
                    CURRENT_TIMESTAMP AS summary_timestamp
                FROM daily_invoices di
                LEFT JOIN daily_credits dc ON di.transaction_date = dc.transaction_date AND di.branch_code = dc.branch_code
                """
            )

            summary_stats = self.conn.execute(
                "SELECT COUNT(*) FROM silver_sales.daily_sales_summary"
            ).fetchone()

            self.conn.execute(
                """
                UPDATE silver_sales.process_log 
                SET completed_at = ?, 
                    records_processed = ?,
                    records_valid = ?,
                    status = 'COMPLETED'
                WHERE process_id = ?
                """,
                (datetime.now(), summary_stats[0], summary_stats[0], process_id),
            )

            logging.info(
                f"✅ Daily Sales Summary: {summary_stats[0]:,} records created"
            )
            return True

        except Exception as e:
            self.conn.execute(
                """
                UPDATE silver_sales.process_log 
                SET completed_at = ?, status = 'FAILED', error_message = ?
                WHERE process_id = ?
                """,
                (datetime.now(), str(e), process_id),
            )
            logging.error(f"❌ Failed to create daily sales summary: {e}")
            return False

    def run_comprehensive_cleaning(self):
        """Run comprehensive data cleaning pipeline"""
        logging.info("🔄 Starting comprehensive data cleaning pipeline...")

        try:
            # Process summarized invoices
            logging.info("📊 Processing summarized invoices...")
            self.create_silver_summarized_invoices()

            # Process detailed invoices
            logging.info("📋 Processing detailed invoices...")
            self.create_silver_detailed_invoices()

            # Create daily summary
            logging.info("📈 Creating daily sales summary...")
            self.create_silver_daily_sales_summary()

            # Log overall data quality metrics
            self.log_overall_data_quality()

            logging.info("✅ Comprehensive data cleaning completed successfully!")
            return True

        except Exception as e:
            logging.error(f"❌ Comprehensive data cleaning failed: {e}")
            return False

    def log_overall_data_quality(self):
        """Log overall data quality metrics"""
        try:
            # Get process statistics
            stats = self.conn.execute(
                """
                SELECT 
                    COUNT(*) as total_processes,
                    SUM(records_processed) as total_records_processed,
                    SUM(records_valid) as total_valid_records,
                    SUM(credit_notes_separated) as total_credit_notes,
                    SUM(duplicates_removed) as total_duplicates_removed,
                    SUM(whitespace_issues_found) as total_whitespace_issues
                FROM silver_sales.process_log 
                WHERE status = 'COMPLETED'
            """
            ).fetchone()

            logging.info("📊 OVERALL DATA QUALITY METRICS:")
            logging.info(f"   Total Records Processed: {stats[1]:,}")
            logging.info(f"   Valid Records: {stats[2]:,}")
            logging.info(f"   Credit Notes Separated: {stats[3]:,}")
            logging.info(f"   Duplicates Removed: {stats[4]:,}")
            logging.info(f"   Columns with Whitespace Issues: {stats[5]:,}")

            # Data quality issues summary
            issues = self.conn.execute(
                """
                SELECT issue_type, COUNT(*) as issue_count, SUM(affected_rows) as total_rows_affected
                FROM silver_sales.data_quality_log
                GROUP BY issue_type
                ORDER BY total_rows_affected DESC
            """
            ).fetchall()

            if issues:
                logging.info("🔍 DATA QUALITY ISSUES FOUND:")
                for issue_type, count, rows in issues:
                    logging.info(
                        f"   {issue_type}: {count} issues affecting {rows:,} rows"
                    )

        except Exception as e:
            logging.error(f"Failed to log overall data quality: {e}")

    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()


# Usage example
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    # Initialize transformer
    transformer = SilverSalesTransformerEnhanced("sales_data.db")

    # Run comprehensive cleaning pipeline
    success = transformer.run_comprehensive_cleaning()

    # Close connection
    transformer.close()

    if success:
        logging.info("🎉 Data cleaning pipeline completed successfully!")
    else:
        logging.error("💥 Data cleaning pipeline encountered errors!")
