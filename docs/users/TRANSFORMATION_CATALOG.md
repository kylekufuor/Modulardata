# Transformation Catalog

> **Total Transformations:** 53
> **Last Updated:** 2026-01-15

This document lists all available transformations in ModularData. You can request these using natural language - the AI will understand your intent.

---

## Quick Reference

| Category | Count | Transformations |
|----------|-------|-----------------|
| [Row Operations](#row-operations) | 4 | drop_rows, filter_rows, deduplicate, sort_rows |
| [Column Operations](#column-operations) | 4 | drop_columns, rename_column, reorder_columns, add_column |
| [Value Transformations](#value-transformations) | 6 | fill_nulls, replace_values, standardize, trim_whitespace, change_case, sanitize_headers |
| [Type Operations](#type-operations) | 3 | convert_type, parse_date, format_date |
| [Date Operations](#date-operations) | 4 | date_diff, date_add, extract_date_part, date_to_epoch |
| [Numeric Operations](#numeric-operations) | 8 | round_numbers, handle_outliers, normalize, abs_value, percent_of_total, bin_numeric, floor_ceiling |
| [Text Operations](#text-operations) | 7 | extract_pattern, split_column, merge_columns, substring, pad_string, clean_text, remove_html |
| [Data Restructuring](#data-restructuring) | 4 | pivot, melt, transpose, select_columns |
| [Filtering & Selection](#filtering--selection) | 2 | slice_rows, sample_rows |
| [Aggregation](#aggregation) | 4 | group_by, cumulative, join, rank |
| [Data Quality](#data-quality) | 3 | validate_format, mask_data, flag_duplicates |
| [Advanced Operations](#advanced-operations) | 4 | conditional_replace, coalesce, explode, lag_lead |

---

## Row Operations

### drop_rows
Remove rows matching a condition.

**Example phrases:**
- "Remove rows where email is blank"
- "Delete rows where age is less than 18"
- "Remove all rows with null values in the status column"

---

### filter_rows
Keep only rows matching a condition (inverse of drop_rows).

**Example phrases:**
- "Keep only rows where status is active"
- "Filter to rows where amount is greater than 100"

---

### deduplicate
Remove duplicate rows.

**Example phrases:**
- "Remove duplicate rows"
- "Deduplicate based on email column"
- "Remove duplicates keeping the last occurrence"

---

### sort_rows
Sort data by column(s).

**Example phrases:**
- "Sort by date descending"
- "Sort by name alphabetically"
- "Sort by category then by amount"

---

## Column Operations

### drop_columns
Remove columns.

**Example phrases:**
- "Remove the internal_id column"
- "Drop the created_at and updated_at columns"

---

### rename_column
Rename a column.

**Example phrases:**
- "Rename 'fname' to 'first_name'"
- "Change column name from 'amt' to 'amount'"

---

### reorder_columns
Change column order.

**Example phrases:**
- "Move email column to the front"
- "Reorder columns: name, email, phone, address"

---

### add_column
Add a calculated column.

**Example phrases:**
- "Add a column for total = price * quantity"
- "Create a full_name column by combining first and last name"

---

## Value Transformations

### fill_nulls
Fill missing values.

**Example phrases:**
- "Fill missing ages with 0"
- "Fill null prices with the average"
- "Fill blanks with 'Unknown'"

**Methods:** value, mean, median, mode, forward, backward, interpolate

---

### replace_values
Find and replace.

**Example phrases:**
- "Replace 'N/A' with blank"
- "Change 'yes' to 'true' and 'no' to 'false'"

---

### standardize
Trim whitespace and convert to lowercase.

**Example phrases:**
- "Standardize the email column"
- "Clean up the category column"

---

### trim_whitespace
Remove leading/trailing spaces.

**Example phrases:**
- "Trim whitespace from name column"
- "Remove extra spaces from all text columns"

---

### change_case
Convert text case.

**Example phrases:**
- "Convert status to lowercase"
- "Make all names title case"
- "Convert codes to uppercase"

**Options:** lower, upper, title, sentence

---

### sanitize_headers
Clean column names.

**Example phrases:**
- "Convert column names to snake_case"
- "Clean up the header names"

---

## Type Operations

### convert_type
Change data type.

**Example phrases:**
- "Convert age to integer"
- "Change price to decimal"
- "Convert is_active to boolean"

**Types:** int, float, str, bool, datetime, category

---

### parse_date
Parse strings as dates.

**Example phrases:**
- "Parse signup_date as a date"
- "Convert the date column from MM/DD/YYYY format"

---

### format_date
Format dates as strings.

**Example phrases:**
- "Format dates as YYYY-MM-DD"
- "Change date format to MM/DD/YYYY"

---

## Date Operations

### date_diff
Calculate days between dates.

**Example phrases:**
- "Calculate days between start_date and end_date"
- "Add a column for age in years from birth_date"

---

### date_add
Add/subtract time from dates.

**Example phrases:**
- "Add 30 days to the due_date"
- "Subtract 1 year from all dates"

---

### extract_date_part
Extract year, month, day, etc.

**Example phrases:**
- "Extract the year from signup_date"
- "Get the month and day of week from order_date"

**Parts:** year, month, day, hour, minute, weekday, week, quarter

---

### date_to_epoch
Convert to Unix timestamp.

**Example phrases:**
- "Convert dates to Unix timestamp"
- "Change datetime to epoch seconds"

---

## Numeric Operations

### round_numbers
Round numeric values.

**Example phrases:**
- "Round prices to 2 decimal places"
- "Round age to whole numbers"

---

### handle_outliers
Deal with outlier values.

**Example phrases:**
- "Cap outliers in the salary column"
- "Remove outlier rows based on price"
- "Flag outliers in the amount column"

**Methods:** cap, remove, flag, replace_null

---

### normalize
Scale numeric data.

**Example phrases:**
- "Normalize the scores to 0-1 range"
- "Apply z-score normalization to amounts"

**Methods:** minmax (0-1), zscore (mean=0, std=1)

---

### abs_value
Convert to absolute values.

**Example phrases:**
- "Make all amounts positive"
- "Convert to absolute values"

---

### percent_of_total
Calculate percentages.

**Example phrases:**
- "Calculate each sale as percent of total"
- "Add percentage of category total"

---

### bin_numeric
Create bins/buckets.

**Example phrases:**
- "Create age groups: 0-18, 18-35, 35-65, 65+"
- "Bin amounts into quartiles"

---

### floor_ceiling
Apply floor or ceiling.

**Example phrases:**
- "Round down all prices"
- "Round up quantities to whole numbers"

---

## Text Operations

### extract_pattern
Extract text matching a pattern.

**Example phrases:**
- "Extract the domain from email addresses"
- "Pull out the area code from phone numbers"

---

### split_column
Split into multiple columns.

**Example phrases:**
- "Split full_name into first and last name"
- "Separate address by comma"

---

### merge_columns
Combine columns.

**Example phrases:**
- "Combine first_name and last_name with a space"
- "Merge city, state, zip into full_address"

---

### substring
Extract part of string.

**Example phrases:**
- "Get the first 3 characters of the code"
- "Extract text before the @ in email"

---

### pad_string
Pad to fixed length.

**Example phrases:**
- "Pad employee_id to 6 digits with leading zeros"
- "Right-pad codes to 10 characters"

---

### clean_text
Remove special characters.

**Example phrases:**
- "Remove special characters from names"
- "Keep only letters and numbers in codes"

---

### remove_html
Strip HTML tags.

**Example phrases:**
- "Remove HTML tags from description"
- "Strip HTML from the content column"

---

## Data Restructuring

### pivot
Long to wide format.

**Example phrases:**
- "Pivot the data with dates as rows and categories as columns"
- "Create a pivot table of sales by region"

---

### melt
Wide to long format.

**Example phrases:**
- "Unpivot the monthly columns into rows"
- "Melt the year columns into a single column"

---

### transpose
Flip rows and columns.

**Example phrases:**
- "Transpose the table"
- "Flip rows and columns"

---

### select_columns
Keep only specific columns.

**Example phrases:**
- "Keep only name, email, and phone columns"
- "Select just the id and status columns"

---

## Filtering & Selection

### slice_rows
Get first/last N rows.

**Example phrases:**
- "Keep only the first 100 rows"
- "Get the last 50 rows"

---

### sample_rows
Random sample.

**Example phrases:**
- "Take a random sample of 1000 rows"
- "Sample 10% of the data"

---

## Aggregation

### group_by
Aggregate by groups.

**Example phrases:**
- "Sum sales by category"
- "Count orders by customer"
- "Calculate average price by region"

**Functions:** sum, mean, count, min, max, first, last, std

---

### cumulative
Running totals.

**Example phrases:**
- "Add a running total of sales"
- "Calculate cumulative count by category"

---

### join
Merge with another dataset.

**Example phrases:**
- "Join with the customer lookup table on customer_id"

---

### rank
Assign ranks.

**Example phrases:**
- "Rank employees by sales"
- "Add a rank column based on score"

---

## Data Quality

### validate_format
Check patterns.

**Example phrases:**
- "Flag invalid email addresses"
- "Check which phone numbers are invalid"

**Supported formats:** email, phone_us, phone_intl, url, zipcode_us, ssn, credit_card, ip_address, date_iso, uuid

---

### mask_data
Mask sensitive data.

**Example phrases:**
- "Mask the SSN column showing only last 4"
- "Hide credit card numbers"
- "Mask emails keeping the domain"

---

### flag_duplicates
Mark duplicate rows.

**Example phrases:**
- "Add a column indicating duplicate rows"
- "Flag duplicates based on email"

---

## Advanced Operations

### conditional_replace
If-then-else logic.

**Example phrases:**
- "If amount < 0 set status to 'refund', else 'sale'"
- "Categorize ages: under 18 is 'minor', 18-65 is 'adult', else 'senior'"

---

### coalesce
First non-null value.

**Example phrases:**
- "Use primary_email, if null use secondary_email"
- "Coalesce phone columns into one"

---

### explode
Expand lists to rows.

**Example phrases:**
- "Expand the tags array into separate rows"
- "Explode the categories list"

---

### lag_lead
Previous/next row values.

**Example phrases:**
- "Add previous day's value as a column"
- "Calculate change from previous row"

---

## Tips for Best Results

1. **Be specific** - "Remove rows where email is blank" works better than "clean up emails"

2. **Reference columns by name** - Use the exact column name when possible

3. **Batch related changes** - Add multiple transformations before applying

4. **Review the plan** - Check the explanation before applying

5. **Use version control** - Rollback if something goes wrong

---

*Can't find what you need? Use natural language to describe your transformation - the AI will do its best to understand.*
