import duckdb
from datetime import datetime
from typing import Dict, List, Tuple
import logging


class SilverSalesTransformer:
    def __init__(self, db_path: str = ":memory:"):
        self.conn = duckdb.connect(db_path)
        self._init_silver_schema()

    def _init_silver_schema(self):
        """Initialize silver sales schema"""
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
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                status VARCHAR,
                error_message VARCHAR
            );
            """,
            "CREATE SEQUENCE IF NOT EXISTS silver_sales.process_id_seq;",
        ]

        for script in init_scripts:
            try:
                self.conn.execute(script)
            except Exception as e:
                print(f"Warning: Could not initialize silver sales schema: {e}")

    def detect_credit_notes(self, table_name: str) -> Tuple[int, int]:
        """Detect credit notes in a bronze table"""
        try:
            result = self.conn.execute(
                f"""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(CASE WHEN DOCUMENT_NUMBER LIKE 'CN%' OR AMOUNT < 0 THEN 1 END) as credit_notes
                FROM {table_name}
            """
            ).fetchone()
            return result
        except Exception as e:
            print(f"Error detecting credit notes in {table_name}: {e}")
            return 0, 0

    def create_silver_summarized_invoices(self):
        """Create silver layer for summarized invoices with credit note separation"""
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

            # Detect credit notes
            total_records, credit_notes_count = self.detect_credit_notes(
                "bronze_sales_cash_invoices_summarized"
            )
            print(
                f"📊 Summarized Invoices: {total_records:,} total, {credit_notes_count:,} credit notes"
            )

            # Create regular invoices table (excluding credit notes)
            self.conn.execute(
                """
                CREATE OR REPLACE TABLE silver_sales.summarized_invoices AS
                WITH cleaned_data AS (
                    SELECT 
                        -- Invoice identification
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
                        
                        -- Amount handling (ensure positive values for invoices)
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

            # Create separate credit notes table
            self.conn.execute(
                """
                CREATE OR REPLACE TABLE silver_sales.detected_summarized_credit_notes AS
                SELECT 
                    invoice_number,
                    customer_code,
                    customer_name,
                    transaction_date,
                    transaction_year,
                    transaction_month,
                    amount AS credit_amount,
                    amount_inclusive AS credit_amount_inclusive,
                    amount_exclusive AS credit_amount_exclusive,
                    vat_amount AS credit_vat_amount,
                    discount_amount AS credit_discount_amount,
                    branch_code,
                    cashier_username,
                    branch_name,
                    mpesa_till_number,
                    customer_phone,
                    customer_email,
                    company_name,
                    customer_address,
                    source_system,
                    source_processed_at,
                    bronze_load_timestamp,
                    bronze_batch_id,
                    silver_load_timestamp,
                    silver_process_id,
                    vat_calculated AS credit_vat_calculated,
                    discount_percentage AS credit_discount_percentage
                FROM silver_sales.summarized_invoices
                WHERE is_credit_note = 1
            """
            )

            # Remove credit notes from main invoices table
            self.conn.execute(
                """
                DELETE FROM silver_sales.summarized_invoices 
                WHERE is_credit_note = 1
            """
            )

            # Get statistics
            invoice_stats = self.conn.execute(
                """
                SELECT COUNT(*) as invoice_count FROM silver_sales.summarized_invoices
            """
            ).fetchone()

            credit_stats = self.conn.execute(
                """
                SELECT COUNT(*) as credit_note_count FROM silver_sales.detected_summarized_credit_notes
            """
            ).fetchone()

            # Update process log
            self.conn.execute(
                """
                UPDATE silver_sales.process_log 
                SET completed_at = ?, 
                    records_processed = ?,
                    records_valid = ?,
                    credit_notes_separated = ?,
                    status = 'COMPLETED'
                WHERE process_id = ?
            """,
                (
                    datetime.now(),
                    total_records,
                    invoice_stats[0],
                    credit_stats[0],
                    process_id,
                ),
            )

            print(
                f"✅ Summarized Invoices: {invoice_stats[0]:,} invoices, {credit_stats[0]:,} credit notes"
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
            print(f"❌ Failed to create summarized silver layer: {e}")
            return False

    def create_silver_detailed_invoices(self):
        """Create silver layer for detailed invoices with credit note separation"""
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
                print("ℹ️ Detailed invoices table not found, skipping...")
                return True

            self.conn.execute(
                """
                INSERT INTO silver_sales.process_log 
                (process_id, source_table, target_table, started_at, status)
                VALUES (?, 'bronze_sales_cash_invoices_detailed', 'silver_sales.detailed_invoices', ?, 'STARTED')
            """,
                (process_id, started_at),
            )

            # Detect credit notes
            total_records, credit_notes_count = self.detect_credit_notes(
                "bronze_sales_cash_invoices_detailed"
            )
            print(
                f"📊 Detailed Invoices: {total_records:,} total, {credit_notes_count:,} credit notes"
            )

            # Create detailed invoices table
            self.conn.execute(
                """
                CREATE OR REPLACE TABLE silver_sales.detailed_invoices AS
                WITH cleaned_data AS (
                    SELECT 
                        -- Invoice identification
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
                        
                        -- Amount handling
                        CASE 
                            WHEN DOCUMENT_NUMBER LIKE 'CN%' OR AMOUNT < 0 THEN ABS(TRY_CAST(AMOUNT AS DECIMAL(15,2)))
                            ELSE TRY_CAST(AMOUNT AS DECIMAL(15,2))
                        END AS amount,
                        
                        TRY_CAST(AMOUNT_INCLUSIVE AS DECIMAL(15,2)) AS amount_inclusive,
                        TRY_CAST(AMOUNT_EXCLUSIVE AS DECIMAL(15,2)) AS amount_exclusive,
                        TRY_CAST(VAT AS DECIMAL(15,2)) AS vat_amount,
                        TRY_CAST(DISC AS DECIMAL(15,2)) AS discount_amount,
                        TRY_CAST(TAX_AMOUNT AS DECIMAL(15,2)) AS tax_amount,
                        
                        -- Line item details (specific to detailed table)
                        TRY_CAST(QUANTITY AS DECIMAL(10,2)) AS quantity,
                        TRY_CAST(DISCOUNT_PERCENT AS DECIMAL(5,2)) AS discount_percent,
                        TRY_CAST(DISCOUNT AS DECIMAL(15,2)) AS line_discount,
                        TRY_CAST(AMOUNT_VALUE AS DECIMAL(15,2)) AS amount_value,
                        TRY_CAST(AMT_EXCL AS DECIMAL(15,2)) AS amount_excl,
                        TRY_CAST(TRIM(DESCRIPTION) AS VARCHAR) AS product_description,
                        TRY_CAST(TRIM(ITEM_CODE) AS VARCHAR) AS product_code,
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
                CREATE OR REPLACE TABLE silver_sales.detected_detailed_credit_notes AS
                SELECT 
                    invoice_number,
                    customer_code,
                    customer_name,
                    transaction_date,
                    transaction_year,
                    transaction_month,
                    amount AS credit_amount,
                    amount_inclusive AS credit_amount_inclusive,
                    amount_exclusive AS credit_amount_exclusive,
                    vat_amount AS credit_vat_amount,
                    discount_amount AS credit_discount_amount,
                    tax_amount AS credit_tax_amount,
                    quantity AS credit_quantity,
                    discount_percent AS credit_discount_percent,
                    line_discount AS credit_line_discount,
                    product_description AS credit_product_description,
                    product_code AS credit_product_code,
                    unit_price AS credit_unit_price,
                    price_weight AS credit_price_weight,
                    price_weight_quantity AS credit_price_weight_quantity,
                    transaction_type AS credit_transaction_type,
                    branch_code,
                    cashier_username,
                    branch_name,
                    mpesa_till_number,
                    customer_phone,
                    customer_email,
                    company_name,
                    customer_address,
                    source_system,
                    source_processed_at,
                    bronze_load_timestamp,
                    bronze_batch_id,
                    silver_load_timestamp,
                    silver_process_id,
                    calculated_line_total AS credit_calculated_total,
                    effective_amount AS credit_effective_amount,
                    discounted_unit_price AS credit_discounted_unit_price,
                    net_line_amount AS credit_net_line_amount
                FROM silver_sales.detailed_invoices
                WHERE is_credit_note = 1
            """
            )

            # Remove credit notes from main detailed invoices table
            self.conn.execute(
                """
                DELETE FROM silver_sales.detailed_invoices 
                WHERE is_credit_note = 1
            """
            )

            # Get statistics
            invoice_stats = self.conn.execute(
                """
                SELECT COUNT(*) as invoice_count FROM silver_sales.detailed_invoices
            """
            ).fetchone()

            credit_stats = self.conn.execute(
                """
                SELECT COUNT(*) as credit_note_count FROM silver_sales.detected_detailed_credit_notes
            """
            ).fetchone()

            self.conn.execute(
                """
                UPDATE silver_sales.process_log 
                SET completed_at = ?, 
                    records_processed = ?,
                    records_valid = ?,
                    credit_notes_separated = ?,
                    status = 'COMPLETED'
                WHERE process_id = ?
            """,
                (
                    datetime.now(),
                    total_records,
                    invoice_stats[0],
                    credit_stats[0],
                    process_id,
                ),
            )

            print(
                f"✅ Detailed Invoices: {invoice_stats[0]:,} invoices, {credit_stats[0]:,} credit notes"
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
            print(f"❌ Failed to create detailed silver layer: {e}")
            return False

    def create_silver_daily_sales_summary(self):
        """Create daily sales summary combining both summarized and detailed data"""
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
                    
                    -- From detailed invoices (if available)
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
                        'DETAILED' AS data_source
                    FROM silver_sales.detailed_invoices
                    GROUP BY transaction_date, transaction_year, transaction_month, branch_name, branch_code, cashier_username
                ),
                daily_credits AS (
                    -- Credit notes from summarized
                    SELECT 
                        transaction_date,
                        branch_name,
                        branch_code,
                        COUNT(*) AS credit_note_count,
                        SUM(credit_amount) AS total_credits
                    FROM silver_sales.detected_summarized_credit_notes
                    GROUP BY transaction_date, branch_name, branch_code
                    
                    UNION ALL
                    
                    -- Credit notes from detailed (if available)
                    SELECT 
                        transaction_date,
                        branch_name,
                        branch_code,
                        COUNT(*) AS credit_note_count,
                        SUM(credit_amount) AS total_credits
                    FROM silver_sales.detected_detailed_credit_notes
                    GROUP BY transaction_date, branch_name, branch_code
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
                    CASE 
                        WHEN di.total_sales > 0 THEN (COALESCE(dc.total_credits, 0) / di.total_sales) * 100 
                        ELSE 0 
                    END AS credit_note_percentage,
                    CURRENT_TIMESTAMP AS processed_timestamp
                FROM daily_invoices di
                LEFT JOIN daily_credits dc ON di.transaction_date = dc.transaction_date 
                    AND di.branch_name = dc.branch_name 
                    AND di.branch_code = dc.branch_code
            """
            )

            record_count = self.conn.execute(
                "SELECT COUNT(*) FROM silver_sales.daily_sales_summary"
            ).fetchone()[0]

            self.conn.execute(
                """
                UPDATE silver_sales.process_log 
                SET completed_at = ?, records_processed = ?, status = 'COMPLETED'
                WHERE process_id = ?
            """,
                (datetime.now(), record_count, process_id),
            )

            print(f"✅ Daily Sales Summary: {record_count:,} records")
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
            print(f"❌ Failed to create daily sales summary: {e}")
            return False

    def transform_all(self):
        """Execute all silver layer transformations"""
        print("🚀 Starting Silver Sales Layer Transformations...")
        print("=" * 60)

        results = []

        # Execute transformations in order
        transformations = [
            ("Summarized Invoices", self.create_silver_summarized_invoices),
            ("Detailed Invoices", self.create_silver_detailed_invoices),
            ("Daily Sales Summary", self.create_silver_daily_sales_summary),
        ]

        for name, transform_func in transformations:
            print(f"\n📊 Processing: {name}...")
            success = transform_func()
            results.append((name, success))

        # Print summary
        print("\n" + "=" * 60)
        print("SILVER SALES LAYER TRANSFORMATION SUMMARY")
        print("=" * 60)

        for name, success in results:
            status = "✅ SUCCESS" if success else "❌ FAILED"
            print(f"{name:<25} {status}")

        return all(success for _, success in results)

    def get_silver_tables_info(self):
        """Get information about silver layer tables"""
        tables = [
            "summarized_invoices",
            "detected_summarized_credit_notes",
            "detailed_invoices",
            "detected_detailed_credit_notes",
            "daily_sales_summary",
        ]

        print("\n📈 SILVER SALES LAYER TABLES OVERVIEW")
        print("=" * 60)

        for table in tables:
            try:
                count = self.conn.execute(
                    f"SELECT COUNT(*) FROM silver_sales.{table}"
                ).fetchone()[0]
                columns = self.conn.execute(
                    f"PRAGMA table_info(silver_sales.{table})"
                ).fetchall()
                print(
                    f"{table:<25} | Records: {count:>8,} | Columns: {len(columns):>2}"
                )

            except Exception as e:
                print(f"{table:<25} | Error: {e}")
