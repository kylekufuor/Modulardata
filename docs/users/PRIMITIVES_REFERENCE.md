# ModularData Primitives Reference

## Overview

Primitives are the fundamental building blocks of the ModularData transformation system. Each primitive represents a single, deterministic data operation that:

- **Takes a DataFrame as input** and produces a transformed DataFrame as output
- **Has well-defined parameters** with types, defaults, and validation
- **Executes deterministically** - the same inputs always produce the same outputs
- **Reports its effects** - including row counts, column counts, and operation metadata

Primitives are designed to be composed into transformation chains that convert natural language requests into reproducible data transformations.

---

## Quick Reference

### Primitives by Category

| Category | Primitives | Description |
|----------|-----------|-------------|
| **rows** | `sort_rows`, `filter_rows`, `limit_rows`, `remove_duplicates`, `merge_duplicates`, `fill_blanks`, `add_rows`, `sample_rows`, `offset_rows`, `head_rows`, `tail_rows`, `shuffle_rows`, `row_number`, `explode_column` | Operations that modify rows: sorting, filtering, deduplication, sampling |
| **columns** | `select_columns`, `remove_columns`, `rename_columns`, `reorder_columns`, `add_column`, `split_column`, `merge_columns`, `copy_column`, `change_column_type`, `coalesce`, `replace_null`, `concat_columns`, `generate_uuid`, `fill_forward`, `fill_backward` | Operations that modify column structure and content |
| **format** | `format_date`, `format_phone`, `change_text_casing`, `trim_whitespace`, `standardize_values` | Value formatting and standardization |
| **text** | `find_replace`, `extract_text`, `text_length`, `pad_text`, `remove_characters`, `string_contains`, `substring`, `regex_replace`, `regex_extract` | String manipulation and text processing |
| **calculate** | `math_operation`, `round_numbers`, `percentage`, `running_total`, `rank`, `conditional_value`, `floor_ceil`, `bin_values`, `absolute_value`, `is_between`, `case_when`, `dense_rank`, `ntile` | Mathematical operations and calculations |
| **tables** | `join_tables`, `union_tables`, `lookup` | Multi-table operations: joins, unions, lookups |
| **groups** | `aggregate`, `pivot`, `unpivot` | Aggregation and reshaping operations |
| **dates** | `extract_date_part`, `date_diff`, `date_add` | Date and time operations |
| **quality** | `detect_nulls`, `profile_column`, `validate_pattern`, `is_duplicate`, `detect_header`, `validate_schema`, `infer_types`, `compare_schemas`, `detect_renamed_columns` | Data quality and validation |

---

## Row Operations

### sort_rows

**Description:** Sort rows by one or more columns in ascending or descending order

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `columns` | `list[str]` | Yes | - | Column(s) to sort by |
| `ascending` | `bool \| list[bool]` | No | `True` | Sort ascending (True) or descending (False). Can be a list for multiple columns. |
| `na_position` | `str` | No | `"last"` | Where to put NaN values: 'first' or 'last' |

**Example Prompts:**
- "Sort the data by lead_score from highest to lowest"
- "Arrange rows alphabetically by last name"
- "Order by created_date newest first, then by name A-Z"

---

### filter_rows

**Description:** Keep or remove rows based on filter conditions

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `conditions` | `list[Condition]` | Yes | - | List of filter conditions to apply |
| `logic` | `str` | No | `"and"` | How to combine conditions: 'and' or 'or' |
| `keep` | `bool` | No | `True` | If True, keep matching rows. If False, remove them. |

**Condition Structure:**
```json
{
  "column": "column_name",
  "operator": "gt|lt|eq|ne|gte|lte|isnull|notnull|contains|startswith|endswith|regex|in|not_in",
  "value": <compare_value>,
  "case_sensitive": true|false
}
```

**Example Prompts:**
- "Show only rows where lead_score is greater than 80"
- "Remove all rows where email is empty or null"
- "Keep rows where status is either 'active' or 'pending'"

---

### limit_rows

**Description:** Keep only the first or last N rows

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `count` | `int` | Yes | - | Number of rows to keep |
| `from_end` | `bool` | No | `False` | If True, take from end instead of beginning |

**Example Prompts:**
- "Keep only the first 100 rows"
- "Get the last 50 records"
- "Show me the top 10 entries"

---

### remove_duplicates

**Description:** Remove duplicate rows, keeping the first or last occurrence

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `subset` | `list[str] \| None` | No | `None` | Columns to check for duplicates. None means all columns. |
| `keep` | `str` | No | `"first"` | Which duplicate to keep: 'first', 'last', or 'none' (remove all) |

**Example Prompts:**
- "Remove duplicate rows based on email address"
- "Delete all duplicate entries, keeping the most recent one"
- "Remove rows where lead_id and email are duplicated"

---

### merge_duplicates

**Description:** Merge duplicate rows by combining values (sum, concat, max, etc.)

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `group_by` | `list[str]` | Yes | - | Columns that define duplicates (group by these) |
| `aggregations` | `dict[str, str]` | No | `None` | How to aggregate each column: {col: 'sum'\|'mean'\|'max'\|'min'\|'first'\|'last'\|'concat'} |
| `default_agg` | `str` | No | `"first"` | Default aggregation for columns not specified |

**Example Prompts:**
- "Combine duplicate orders by customer_id, summing the amounts"
- "Merge duplicate contacts by email, keeping the most recent activity_date"
- "Consolidate rows with same product_id, concatenating all notes"

---

### fill_blanks

**Description:** Fill null or blank values using various methods

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `column` | `str` | Yes | - | Column to fill blanks in |
| `method` | `str` | Yes | - | Fill method: 'value', 'mean', 'median', 'mode', 'forward', 'backward' |
| `value` | `Any` | No | `None` | Value to fill with (only used when method='value') |

**Example Prompts:**
- "Fill empty lead_score values with 0"
- "Replace missing prices with the average price"
- "Fill blank status fields with 'Unknown'"

---

### add_rows

**Description:** Add new rows to the top or bottom of the data

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `rows` | `list[dict]` | Yes | - | List of row dictionaries to add |
| `position` | `str` | No | `"bottom"` | Where to add rows: 'top' or 'bottom' |

**Example Prompts:**
- "Add a new row with name='Test User' and email='test@example.com'"
- "Insert a header row at the top with column descriptions"
- "Append three new product entries to the list"

---

### sample_rows

**Description:** Randomly sample a subset of rows for testing or preview

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `n` | `int` | No | `None` | Number of rows to sample (use n OR fraction, not both) |
| `fraction` | `float` | No | `None` | Fraction of rows to sample (0.0 to 1.0) |
| `seed` | `int` | No | `None` | Random seed for reproducibility |
| `replace` | `bool` | No | `False` | Sample with replacement (allows duplicates) |

**Example Prompts:**
- "Get a random sample of 100 rows"
- "Sample 10% of the data randomly"
- "Get 50 random rows with seed 42 for reproducibility"

---

### offset_rows

**Description:** Skip the first N rows, useful for removing headers or pagination

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `offset` | `int` | Yes | - | Number of rows to skip from the beginning |

**Example Prompts:**
- "Skip the first 5 rows"
- "Remove the header row (first row)"
- "Start from row 100 (skip first 99 rows)"

---

### head_rows

**Description:** Get the first N rows from the dataset

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `n` | `int` | No | `10` | Number of rows to return (default: 10) |

**Example Prompts:**
- "Show the first 20 rows"
- "Preview the top 5 records"
- "Get the head of the data"

---

### tail_rows

**Description:** Get the last N rows from the dataset

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `n` | `int` | No | `10` | Number of rows to return (default: 10) |

**Example Prompts:**
- "Show the last 20 rows"
- "Get the bottom 5 records"
- "Show the tail of the data"

---

### shuffle_rows

**Description:** Randomly shuffle (reorder) all rows in the dataset

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `seed` | `int` | No | `None` | Random seed for reproducibility |

**Example Prompts:**
- "Randomize the order of rows"
- "Shuffle the data with seed 42"
- "Mix up the row order randomly"

---

### row_number

**Description:** Add a column with sequential row numbers

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `new_column` | `str` | No | `"row_num"` | Name for the row number column |
| `start` | `int` | No | `1` | Starting number (default: 1) |
| `partition_by` | `list[str]` | No | `None` | Reset numbering for each group (like SQL PARTITION BY) |

**Example Prompts:**
- "Add row numbers to the data"
- "Number the rows starting from 0 in a column called idx"
- "Add row numbers that reset for each department"

---

### explode_column

**Description:** Split a column with delimited values into multiple rows

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `column` | `str` | Yes | - | Column containing delimited values to explode |
| `delimiter` | `str` | No | `","` | Delimiter to split on (default: comma) |
| `trim` | `bool` | No | `True` | Trim whitespace from resulting values |

**Example Prompts:**
- "Split the tags column into separate rows"
- "Explode the categories column using semicolon as separator"
- "Split the items column by pipe character"

---

## Column Operations

### select_columns

**Description:** Keep only the specified columns, removing all others

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `columns` | `list[str]` | Yes | - | List of column names to keep |

**Example Prompts:**
- "Keep only the name, email, and phone columns"
- "I only need the customer_id and order_total fields"
- "Show me just the first_name and last_name columns"

---

### remove_columns

**Description:** Remove the specified columns from the data

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `columns` | `list[str]` | Yes | - | List of column names to remove |

**Example Prompts:**
- "Delete the internal_id and created_by columns"
- "Remove the password field from the data"
- "Drop the temporary columns temp1 and temp2"

---

### rename_columns

**Description:** Rename one or more columns

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `mapping` | `dict[str, str]` | Yes | - | Mapping of old names to new names: {old_name: new_name} |

**Example Prompts:**
- "Rename 'cust_id' to 'customer_id'"
- "Change column names: fname to first_name and lname to last_name"
- "Rename the 'amt' column to 'amount'"

---

### reorder_columns

**Description:** Change the order of columns

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `order` | `list[str]` | Yes | - | New column order. Columns not listed will be appended at the end. |
| `strict` | `bool` | No | `False` | If True, only include columns in the order list |

**Example Prompts:**
- "Move email to the first column"
- "Arrange columns as: id, name, email, phone, status"
- "Put the date column at the beginning"

---

### add_column

**Description:** Add a new column with a constant value or derived from existing columns

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `name` | `str` | Yes | - | Name of the new column |
| `value` | `Any` | No | `None` | Constant value for all rows |
| `from_column` | `str` | No | `None` | Copy values from this column |
| `position` | `int \| str` | No | `"end"` | Position for new column: index number or 'start'/'end' |

**Example Prompts:**
- "Add a new column called 'source' with value 'import'"
- "Create a 'processed' column set to False"
- "Add an empty 'notes' column"

---

### split_column

**Description:** Split a column into multiple columns based on a delimiter

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `column` | `str` | Yes | - | Column to split |
| `delimiter` | `str` | Yes | - | Character or string to split on |
| `new_columns` | `list[str]` | Yes | - | Names for the new columns |
| `keep_original` | `bool` | No | `False` | Whether to keep the original column |

**Example Prompts:**
- "Split the 'full_name' column into 'first_name' and 'last_name' by space"
- "Separate the address column by comma into street, city, state"
- "Break apart the date column using '/' into month, day, year"

---

### merge_columns

**Description:** Combine multiple columns into a single column

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `columns` | `list[str]` | Yes | - | Columns to merge |
| `new_column` | `str` | Yes | - | Name of the merged column |
| `separator` | `str` | No | `" "` | String to put between merged values |
| `keep_original` | `bool` | No | `False` | Whether to keep the original columns |

**Example Prompts:**
- "Combine first_name and last_name into full_name with a space"
- "Join city, state, and zip into 'location' separated by ', '"
- "Concatenate area_code and phone_number into phone with a dash"

---

### copy_column

**Description:** Create a copy of an existing column with a new name

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `source` | `str` | Yes | - | Column to copy from |
| `destination` | `str` | Yes | - | Name for the new column |

**Example Prompts:**
- "Make a copy of the email column called email_backup"
- "Duplicate the price column as original_price"
- "Create a copy of status called status_original"

---

### change_column_type

**Description:** Convert a column to a different data type

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `column` | `str` | Yes | - | Column to convert |
| `to_type` | `str` | Yes | - | Target type: 'string', 'integer', 'float', 'boolean', 'datetime' |
| `date_format` | `str` | No | `None` | Date format for datetime conversion (e.g., '%Y-%m-%d') |
| `errors` | `str` | No | `"coerce"` | How to handle errors: 'raise', 'coerce' (set to NaN), 'ignore' |

**Example Prompts:**
- "Convert the lead_score column to integers"
- "Change the created_date column to datetime format"
- "Make the price column a float"

---

### coalesce

**Description:** Get the first non-null value from a list of columns for each row

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `columns` | `list[str]` | Yes | - | Columns to check in order (first non-null wins) |
| `new_column` | `str` | Yes | - | Name for the result column |
| `default` | `Any` | No | `None` | Default value if all columns are null |

**Example Prompts:**
- "Get the first non-empty value from phone, mobile, or work_phone as contact_number"
- "Combine email and backup_email into primary_email, using 'none@example.com' as default"
- "Get the first available address from shipping_address, billing_address, home_address"

---

### replace_null

**Description:** Replace null/missing values in one or more columns with a specified value

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `columns` | `list[str]` | No | `None` | Columns to replace nulls in (default: all columns) |
| `value` | `Any` | Yes | - | Value to replace nulls with |

**Example Prompts:**
- "Replace null values in the status column with 'Unknown'"
- "Replace all null values with 0"
- "Fill missing email and phone with 'N/A'"

---

### concat_columns

**Description:** Combine multiple columns into a single column with optional separator

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `columns` | `list[str]` | Yes | - | Columns to concatenate |
| `new_column` | `str` | Yes | - | Name for the result column |
| `separator` | `str` | No | `""` | Separator between values (default: no separator) |
| `skip_null` | `bool` | No | `True` | Skip null values when concatenating (default: True) |

**Example Prompts:**
- "Combine first_name and last_name into full_name with a space"
- "Create an address column from street, city, state, zip separated by commas"
- "Combine code1 and code2 into combined_code with hyphen"

---

### generate_uuid

**Description:** Add a column with unique identifiers (UUID) for each row

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `new_column` | `str` | No | `"uuid"` | Name for the UUID column |
| `format` | `str` | No | `"uuid4"` | UUID format: 'uuid4' (random), 'uuid1' (time-based), 'short' (8-char hex) |
| `prefix` | `str` | No | `""` | Optional prefix to add before each UUID |

**Example Prompts:**
- "Add a unique ID column to the data"
- "Generate a short 8-character ID for each row in column 'row_id'"
- "Create UUIDs with prefix 'TXN-' for transaction_id column"

---

### fill_forward

**Description:** Replace null values with the most recent non-null value (forward fill)

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `columns` | `list[str]` | No | `None` | Columns to fill (default: all columns) |
| `limit` | `int` | No | `None` | Maximum number of consecutive nulls to fill |
| `group_by` | `str \| list[str]` | No | `None` | Fill within groups (doesn't carry over between groups) |

**Example Prompts:**
- "Fill missing values in the price column with the previous price"
- "Forward fill all null values in the dataset"
- "Fill missing stock prices forward but within each ticker symbol"

---

### fill_backward

**Description:** Replace null values with the next non-null value (backward fill)

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `columns` | `list[str]` | No | `None` | Columns to fill (default: all columns) |
| `limit` | `int` | No | `None` | Maximum number of consecutive nulls to fill |
| `group_by` | `str \| list[str]` | No | `None` | Fill within groups (doesn't carry over between groups) |

**Example Prompts:**
- "Fill missing values in the target column with the next available value"
- "Backward fill all null values"
- "Fill missing budget values backward within each department"

---

## Format Operations

### format_date

**Description:** Standardize dates to a consistent format

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `column` | `str` | Yes | - | Column containing dates to format |
| `output_format` | `str` | No | `"%Y-%m-%d"` | Output date format (strftime format) |
| `input_format` | `str` | No | `None` | Input date format if known (for faster parsing) |

**Example Prompts:**
- "Standardize all dates in created_date to YYYY-MM-DD format"
- "Format the date column as MM/DD/YYYY"
- "Convert order_date to display as 'January 15, 2024'"

---

### format_phone

**Description:** Standardize phone numbers to a consistent format

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `column` | `str` | Yes | - | Column containing phone numbers |
| `format` | `str` | No | `"(XXX) XXX-XXXX"` | Output format: '(XXX) XXX-XXXX', 'XXX-XXX-XXXX', 'XXXXXXXXXX', '+1 XXX-XXX-XXXX' |
| `country_code` | `str` | No | `"1"` | Default country code for numbers without one |

**Example Prompts:**
- "Format phone numbers as (555) 123-4567"
- "Standardize the phone_number column to XXX-XXX-XXXX format"
- "Clean up phone numbers to just digits"

---

### change_text_casing

**Description:** Change text to uppercase, lowercase, title case, or sentence case

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `column` | `str` | Yes | - | Column to change casing |
| `case` | `str` | Yes | - | Target case: 'lower', 'upper', 'title', 'sentence' |

**Example Prompts:**
- "Convert all names to title case"
- "Make the email column all lowercase"
- "Change status values to uppercase"

---

### trim_whitespace

**Description:** Remove leading, trailing, and extra whitespace from text

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `columns` | `list[str] \| None` | No | `None` | Columns to trim. None means all string columns. |
| `trim_type` | `str` | No | `"all"` | What to trim: 'leading', 'trailing', 'both', 'all' (includes internal) |

**Example Prompts:**
- "Remove extra spaces from all text columns"
- "Trim whitespace from the name and email columns"
- "Clean up leading and trailing spaces in the description"

---

### standardize_values

**Description:** Replace multiple variant values with a single standard value

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `column` | `str` | Yes | - | Column to standardize |
| `mapping` | `dict[str, list[str]]` | Yes | - | Mapping of standard value to list of variants: {'Active': ['active', 'ACTIVE', 'A']} |
| `case_sensitive` | `bool` | No | `False` | Whether matching should be case-sensitive |

**Example Prompts:**
- "Standardize status values: replace 'Qualifed', 'qualfied', 'QUALIFIED' with 'Qualified'"
- "Normalize campaign_source: 'google ads' and 'Google Ads' should be 'Google Ads'"
- "Replace 'Y', 'yes', 'YES' with 'Yes' and 'N', 'no', 'NO' with 'No' in the confirmed column"

---

## Text Operations

### find_replace

**Description:** Find and replace text values using literal strings or regex patterns

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `column` | `str` | Yes | - | Column to perform find/replace on |
| `find` | `str` | Yes | - | Text or pattern to find |
| `replace` | `str` | Yes | - | Replacement text |
| `use_regex` | `bool` | No | `False` | Treat 'find' as a regex pattern |
| `case_sensitive` | `bool` | No | `True` | Whether matching is case-sensitive |

**Example Prompts:**
- "Replace all occurrences of 'N/A' with 'Unknown' in the status column"
- "Remove all dollar signs from the price column"
- "Replace any sequence of digits in notes with [REDACTED]"

---

### extract_text

**Description:** Extract a portion of text using position, pattern, or delimiter

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `column` | `str` | Yes | - | Column to extract from |
| `new_column` | `str` | Yes | - | Name for the extracted text column |
| `method` | `str` | Yes | - | Extraction method: 'position', 'regex', 'before', 'after', 'between' |
| `start` | `int` | No | `0` | Start position (for 'position' method) |
| `end` | `int` | No | `None` | End position (for 'position' method) |
| `pattern` | `str` | No | `None` | Regex pattern with capture group (for 'regex' method) |
| `delimiter` | `str` | No | `None` | Delimiter for 'before', 'after', 'between' methods |
| `delimiter2` | `str` | No | `None` | Second delimiter for 'between' method |

**Example Prompts:**
- "Extract the first 3 characters from product_code into a new column called category_code"
- "Extract the domain from email addresses into a column called email_domain"
- "Extract the number from strings like 'Order #12345' into order_number"

---

### text_length

**Description:** Get the character count of text values in a column

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `column` | `str` | Yes | - | Column to measure |
| `new_column` | `str` | Yes | - | Name for the length column |
| `count_spaces` | `bool` | No | `True` | Whether to include spaces in the count |

**Example Prompts:**
- "Add a column showing the length of each description"
- "Create a character count column for the name field called name_chars"
- "Calculate the length of comments without counting spaces"

---

### pad_text

**Description:** Add leading or trailing characters to reach a fixed length

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `column` | `str` | Yes | - | Column to pad |
| `length` | `int` | Yes | - | Target length after padding |
| `pad_char` | `str` | No | `"0"` | Character to use for padding |
| `side` | `str` | No | `"left"` | Where to add padding: 'left' or 'right' |

**Example Prompts:**
- "Zero-pad the employee_id column to 6 digits"
- "Pad product codes with leading zeros to 10 characters"
- "Add trailing spaces to name column to make all values 20 characters"

---

### remove_characters

**Description:** Strip specific characters or character types from text

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `column` | `str` | Yes | - | Column to clean |
| `characters` | `str` | No | `None` | Specific characters to remove (e.g., '$,.') |
| `remove_type` | `str` | No | `None` | Type to remove: 'digits', 'letters', 'punctuation', 'whitespace' |

**Example Prompts:**
- "Remove all dollar signs and commas from the amount column"
- "Strip all digits from the reference column"
- "Remove punctuation from the comments field"

---

### string_contains

**Description:** Create a boolean column indicating if text contains a substring

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `column` | `str` | Yes | - | Column to search in |
| `substring` | `str` | Yes | - | Substring to search for |
| `new_column` | `str` | No | `None` | Name for result column (default: column_contains) |
| `case_sensitive` | `bool` | No | `False` | Case-sensitive search (default: False) |
| `regex` | `bool` | No | `False` | Treat substring as regex pattern |

**Example Prompts:**
- "Check if the email column contains 'gmail'"
- "Flag names that contain 'Jr' or 'Sr' as has_suffix"
- "Check if description contains 'ERROR' (case-sensitive)"

---

### substring

**Description:** Extract characters from a string by start position and length

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `column` | `str` | Yes | - | Column to extract from |
| `start` | `int` | No | `0` | Starting position (0-based, default: 0) |
| `length` | `int` | No | `None` | Number of characters to extract (default: rest of string) |
| `new_column` | `str` | No | `None` | Name for result column (default: overwrites original) |

**Example Prompts:**
- "Get the first 3 characters from the code column"
- "Extract characters 5-10 from the id column into short_id"
- "Get the last 4 characters of the phone column (skip first 6)"

---

### regex_replace

**Description:** Transform text using regex patterns with capture group backreferences

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `column` | `str` | Yes | - | Column to transform |
| `pattern` | `str` | Yes | - | Regex pattern with capture groups, e.g., '^(.{3})(.{4})(.*)$' |
| `replacement` | `str` | Yes | - | Replacement string with backreferences, e.g., '\\1-\\2-XX' or '$1-$2-XX' |
| `new_column` | `str` | No | `None` | Name for result column (default: overwrites original) |
| `case_sensitive` | `bool` | No | `True` | Case-sensitive matching |

**Example Prompts:**
- "Transform ABCDHED to ABC-DHED-XX format (insert dashes after 3rd and 7th chars, add XX)"
- "Convert phone numbers from 1234567890 to (123) 456-7890 format"
- "Swap first and last name in 'LastName, FirstName' format to 'FirstName LastName'"

---

### regex_extract

**Description:** Extract text matching a regex pattern (with optional capture group) into a new column

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `column` | `str` | Yes | - | Column to extract from |
| `pattern` | `str` | Yes | - | Regex pattern (use capture group to extract specific part) |
| `new_column` | `str` | Yes | - | Name for the extracted value column |
| `group` | `int` | No | `0` | Capture group to extract (0 = entire match, 1+ = specific group) |
| `case_sensitive` | `bool` | No | `True` | Case-sensitive matching |

**Example Prompts:**
- "Extract all numbers from the text column into a numbers column"
- "Extract email addresses from the notes column"
- "Extract the domain from URLs in the link column"

---

## Calculate Operations

### math_operation

**Description:** Add, subtract, multiply, or divide columns or apply constants

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `new_column` | `str` | Yes | - | Name for the result column |
| `operation` | `str` | Yes | - | Operation: 'add', 'subtract', 'multiply', 'divide' |
| `column1` | `str` | Yes | - | First column (or left operand) |
| `column2` | `str` | No | `None` | Second column (if operating between columns) |
| `value` | `float` | No | `None` | Constant value (if applying to single column) |

**Example Prompts:**
- "Calculate total by multiplying quantity and price into a new column called total"
- "Add a 10% markup to the cost column and store in final_price"
- "Calculate profit by subtracting cost from revenue"

---

### round_numbers

**Description:** Round numeric values to a specified number of decimal places

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `column` | `str` | Yes | - | Column to round |
| `decimals` | `int` | No | `0` | Number of decimal places (0 for whole numbers) |
| `method` | `str` | No | `"round"` | Rounding method: 'round', 'floor', 'ceil' |

**Example Prompts:**
- "Round the price column to 2 decimal places"
- "Round down all values in quantity to whole numbers"
- "Round up the shipping_cost to the nearest dollar"

---

### percentage

**Description:** Calculate percentage of column total or ratio between columns

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `column` | `str` | Yes | - | Column to calculate percentage for |
| `new_column` | `str` | Yes | - | Name for the percentage column |
| `mode` | `str` | No | `"of_total"` | Mode: 'of_total' (% of column sum) or 'ratio' (column / denominator) |
| `denominator_column` | `str` | No | `None` | Denominator column for 'ratio' mode |
| `multiply_by_100` | `bool` | No | `True` | Whether to multiply by 100 (True: 50%, False: 0.5) |

**Example Prompts:**
- "Calculate what percentage each sale is of total sales"
- "Calculate completion rate as completed divided by total"
- "Show each region's contribution as a percentage of total revenue"

---

### running_total

**Description:** Calculate a running/cumulative total for a numeric column

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `column` | `str` | Yes | - | Column to calculate running total for |
| `new_column` | `str` | Yes | - | Name for the running total column |
| `group_by` | `str` | No | `None` | Optional column to reset running total for each group |

**Example Prompts:**
- "Add a running total column for the amount field"
- "Calculate cumulative sales for each month"
- "Create a running balance grouped by account_id"

---

### rank

**Description:** Assign rank to rows based on column values

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `column` | `str` | Yes | - | Column to rank by |
| `new_column` | `str` | Yes | - | Name for the rank column |
| `ascending` | `bool` | No | `True` | If True, lowest value = rank 1; if False, highest = rank 1 |
| `method` | `str` | No | `"dense"` | Ranking method: 'dense', 'min', 'max', 'first', 'average' |
| `group_by` | `str` | No | `None` | Optional column to rank within groups |

**Example Prompts:**
- "Rank employees by salary from highest to lowest"
- "Add a rank column for scores, lowest first"
- "Rank products by sales within each category"

---

### conditional_value

**Description:** Set column values based on if/then conditions

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `new_column` | `str` | Yes | - | Name for the result column |
| `conditions` | `list[dict]` | Yes | - | List of {condition, value} pairs. Each condition has column, operator, compare_value. |
| `default` | `Any` | No | `None` | Default value if no conditions match |

**Example Prompts:**
- "Create a grade column: A if score >= 90, B if >= 80, C if >= 70, else F"
- "Add a status column: 'High' if amount > 1000, otherwise 'Low'"
- "Create a tier column based on customer spend"

---

### floor_ceil

**Description:** Round numbers down (floor) or up (ceiling)

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `column` | `str` | Yes | - | Column to apply floor/ceil to |
| `method` | `str` | Yes | - | Method: 'floor' (round down) or 'ceil' (round up) |
| `precision` | `int` | No | `0` | Decimal places (0 = integer, 1 = one decimal, etc.) |

**Example Prompts:**
- "Round down all prices to the nearest integer"
- "Round up the amount column to the nearest whole number"
- "Floor values to one decimal place"

---

### bin_values

**Description:** Group numeric values into bins/buckets (like age ranges, price tiers)

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `column` | `str` | Yes | - | Numeric column to bin |
| `new_column` | `str` | No | `None` | Name for the bin column (default: column_bin) |
| `bins` | `int \| list[float]` | Yes | - | Number of equal-width bins OR list of bin edges [0, 10, 20, 50, 100] |
| `labels` | `list[str]` | No | `None` | Labels for each bin (must be len(bins)-1 if bins is a list) |
| `include_lowest` | `bool` | No | `True` | Include the lowest edge in the first bin |

**Example Prompts:**
- "Create 5 equal age groups from the age column"
- "Bin ages into groups: 0-18, 18-35, 35-55, 55+"
- "Create price tiers: Low (0-50), Medium (50-100), High (100+)"

---

### absolute_value

**Description:** Convert negative values to positive (absolute value)

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `column` | `str` | Yes | - | Column to apply absolute value to |
| `new_column` | `str` | No | `None` | Name for result column (default: overwrites original) |

**Example Prompts:**
- "Get the absolute value of the balance column"
- "Convert all negative amounts to positive, store in abs_amount"
- "Remove negative signs from the difference column"

---

### is_between

**Description:** Create a boolean column indicating if values are within a range

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `column` | `str` | Yes | - | Column to check |
| `min_value` | `float` | Yes | - | Minimum value of range (inclusive by default) |
| `max_value` | `float` | Yes | - | Maximum value of range (inclusive by default) |
| `new_column` | `str` | No | `None` | Name for result column (default: column_in_range) |
| `inclusive` | `str` | No | `"both"` | Include boundaries: 'both', 'neither', 'left', 'right' |

**Example Prompts:**
- "Check if age is between 18 and 65"
- "Flag prices between 10 and 100 as in_budget"
- "Check if score is strictly between 0 and 100 (not including endpoints)"

---

### case_when

**Description:** Assign values based on multiple conditions (like SQL CASE WHEN)

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `cases` | `list[dict]` | Yes | - | List of {condition: {column, operator, value}, result: value} pairs |
| `new_column` | `str` | Yes | - | Name for the result column |
| `default` | `Any` | No | `None` | Default value if no conditions match |

**Example Prompts:**
- "Create a tier column: 'Gold' if amount > 1000, 'Silver' if amount > 500, else 'Bronze'"
- "Set status_label to 'Active' if status='active', 'Pending' if status='pending', else 'Other'"
- "Create age_group: 'Minor' if age < 18, 'Adult' if age < 65, 'Senior' otherwise"

---

### dense_rank

**Description:** Assign dense rank (1,2,3 with no gaps for ties) like SQL DENSE_RANK()

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `column` | `str` | Yes | - | Column to rank by |
| `new_column` | `str` | No | `None` | Name for the rank column (default: column_dense_rank) |
| `ascending` | `bool` | No | `True` | If True, lowest value = rank 1; if False, highest = rank 1 |
| `partition_by` | `str \| list[str]` | No | `None` | Column(s) to partition/group by (like SQL PARTITION BY) |

**Example Prompts:**
- "Assign dense rank to employees by salary (highest first)"
- "Dense rank products by sales within each category"
- "Add a dense rank column for scores from lowest to highest"

---

### ntile

**Description:** Divide rows into N approximately equal buckets (like SQL NTILE)

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `n` | `int` | Yes | - | Number of buckets to divide into |
| `order_by` | `str` | Yes | - | Column to order by before dividing into buckets |
| `new_column` | `str` | No | `None` | Name for the bucket column (default: ntile_N) |
| `ascending` | `bool` | No | `True` | Order direction (True = lowest values in bucket 1) |
| `partition_by` | `str \| list[str]` | No | `None` | Column(s) to partition by before bucketing |

**Example Prompts:**
- "Divide customers into 4 quartiles by purchase amount"
- "Create 10 decile buckets for scores"
- "Split employees into 3 groups by salary within each department"

---

## Table Operations

### join_tables

**Description:** Join two tables using inner, left, right, or outer join

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `right_table` | `pd.DataFrame` | Yes | - | The table to join with |
| `left_on` | `str \| list[str]` | Yes | - | Column(s) from the left table to join on |
| `right_on` | `str \| list[str]` | Yes | - | Column(s) from the right table to join on |
| `how` | `str` | No | `"left"` | Join type: 'inner', 'left', 'right', 'outer' |
| `suffixes` | `tuple[str, str]` | No | `("_left", "_right")` | Suffixes for overlapping column names |

**Example Prompts:**
- "Join orders with customers on customer_id"
- "Match products with inventory using product_code, keeping only matches"
- "Combine employees and departments on dept_id, include all from both"

---

### union_tables

**Description:** Stack multiple tables vertically, combining all rows

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `other_tables` | `list[pd.DataFrame]` | Yes | - | Tables to append to the main table |
| `ignore_index` | `bool` | No | `True` | Reset index after union |
| `match_columns` | `bool` | No | `True` | If True, only include columns present in all tables |

**Example Prompts:**
- "Combine January, February, and March sales data into one table"
- "Append new customer records to the existing customer list"
- "Union all regional reports into a single dataset"

---

### lookup

**Description:** Look up values from another table (like Excel VLOOKUP)

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `lookup_table` | `pd.DataFrame` | Yes | - | Table to look up values from |
| `lookup_column` | `str` | Yes | - | Column in main table to match on |
| `lookup_key` | `str` | Yes | - | Column in lookup table to match against |
| `return_columns` | `list[str]` | Yes | - | Columns from lookup table to bring back |
| `default_value` | `Any` | No | `None` | Value to use when no match is found |

**Example Prompts:**
- "Look up customer name from customer table using customer_id"
- "Get product price and category from product catalog by product_code"
- "Find employee department using employee_id, use 'Unknown' if not found"

---

## Group Operations

### aggregate

**Description:** Group rows by columns and calculate aggregations (sum, avg, count, etc.)

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `group_by` | `list[str]` | Yes | - | Columns to group by |
| `aggregations` | `dict[str, str \| list[str]]` | Yes | - | Aggregations to perform: {column: 'sum'\|'mean'\|'count'\|'min'\|'max'\|'first'\|'last'} |
| `reset_index` | `bool` | No | `True` | Whether to reset index after grouping |

**Example Prompts:**
- "Calculate total sales by region"
- "Get average price and count of products by category"
- "Find min and max order amount by customer"

---

### pivot

**Description:** Create a pivot table with rows, columns, and values

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `index` | `str \| list[str]` | Yes | - | Column(s) to use as row labels |
| `columns` | `str` | Yes | - | Column to use for new column headers |
| `values` | `str \| list[str]` | Yes | - | Column(s) to aggregate |
| `aggfunc` | `str` | No | `"sum"` | Aggregation function: 'sum', 'mean', 'count', 'min', 'max' |
| `fill_value` | `Any` | No | `0` | Value to use for missing combinations |

**Example Prompts:**
- "Pivot sales data with products as rows and months as columns"
- "Create a pivot showing average score by student and subject"
- "Pivot order count by region and status"

---

### unpivot

**Description:** Transform wide format to long format (melt columns into rows)

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `id_columns` | `list[str]` | Yes | - | Columns to keep as identifiers (not unpivoted) |
| `value_columns` | `list[str]` | No | `None` | Columns to unpivot. If None, all non-ID columns. |
| `var_name` | `str` | No | `"variable"` | Name for the new variable column |
| `value_name` | `str` | No | `"value"` | Name for the new value column |

**Example Prompts:**
- "Unpivot monthly columns (Jan, Feb, Mar) into rows with product_id as identifier"
- "Melt the score columns into rows, keeping student_id and name"
- "Transform wide year columns (2020, 2021, 2022) to long format"

---

## Date Operations

### extract_date_part

**Description:** Extract year, month, day, week, quarter, or other parts from a date column

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `column` | `str` | Yes | - | Column containing dates |
| `part` | `str` | Yes | - | Part to extract: year, month, day, week, quarter, dayofweek, dayofyear, hour, minute, second, weekday_name, month_name |
| `new_column` | `str` | No | `None` | Name for the new column (default: column_part) |

**Example Prompts:**
- "Extract the year from the created_date column"
- "Get the month from order_date as a new column called order_month"
- "Extract the day of week from transaction_date"

---

### date_diff

**Description:** Calculate the difference between two dates in days, months, or years

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `start_column` | `str` | Yes | - | Column containing the start date |
| `end_column` | `str` | Yes | - | Column containing the end date |
| `unit` | `str` | No | `"days"` | Unit for difference: days, weeks, months, years, hours, minutes, seconds |
| `new_column` | `str` | No | `None` | Name for the new column (default: date_diff_{unit}) |
| `absolute` | `bool` | No | `False` | Return absolute value (always positive) |

**Example Prompts:**
- "Calculate days between start_date and end_date"
- "Find the number of months between order_date and ship_date"
- "Calculate age in years from birth_date to today"

---

### date_add

**Description:** Add days, weeks, months, or years to a date column

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `column` | `str` | Yes | - | Column containing the date |
| `amount` | `int` | Yes | - | Amount to add (negative to subtract) |
| `unit` | `str` | No | `"days"` | Unit: 'days', 'weeks', 'months', 'years', 'hours', 'minutes' |
| `new_column` | `str` | No | `None` | Name for result column (default: overwrites original) |

**Example Prompts:**
- "Add 30 days to the due_date column"
- "Subtract 1 year from the birth_date to get previous_year"
- "Add 2 weeks to the start_date"

---

## Quality Operations

### detect_nulls

**Description:** Detect and analyze null/missing values across columns

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `columns` | `list[str]` | No | `None` | Columns to check (default: all columns) |
| `add_null_flag` | `bool` | No | `False` | Add a boolean column flagging rows with any nulls |
| `add_null_count` | `bool` | No | `False` | Add a column counting nulls per row |
| `threshold` | `float` | No | `None` | Flag columns with null rate above this threshold (0.0 to 1.0) |

**Example Prompts:**
- "Check for null values in the email and phone columns"
- "Find all rows that have any missing values"
- "Identify columns with more than 10% missing values"

---

### profile_column

**Description:** Generate detailed statistics for a column (min, max, mean, nulls, unique, etc.)

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `column` | `str` | Yes | - | Column to profile |
| `include_percentiles` | `bool` | No | `True` | Include percentile values (25th, 50th, 75th) |
| `include_top_values` | `bool` | No | `True` | Include top N most frequent values |
| `top_n` | `int` | No | `5` | Number of top values to include |

**Example Prompts:**
- "Get statistics for the amount column"
- "Profile the status column with top 10 values"
- "Analyze the email column without percentiles"

---

### validate_pattern

**Description:** Check if values match a regex pattern (email, phone, custom, etc.)

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `column` | `str` | Yes | - | Column to validate |
| `pattern` | `str` | No | `None` | Regex pattern to match (use this OR pattern_type) |
| `pattern_type` | `str` | No | `None` | Predefined pattern: 'email', 'phone', 'url', 'date', 'zipcode', 'ssn' |
| `add_valid_flag` | `bool` | No | `True` | Add a boolean column flagging valid values |
| `new_column` | `str` | No | `None` | Name for the validation flag column (default: column_valid) |

**Example Prompts:**
- "Validate email addresses in the email column"
- "Check if phone numbers match the format (XXX) XXX-XXXX"
- "Validate that the url column contains valid URLs"

---

### is_duplicate

**Description:** Add a boolean column indicating if a row is a duplicate

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `subset` | `list[str]` | No | `None` | Columns to check for duplicates (default: all columns) |
| `keep` | `str` | No | `"first"` | Which occurrence to mark as NOT duplicate: 'first', 'last', or 'none' (mark all as duplicates) |
| `new_column` | `str` | No | `"_is_duplicate"` | Name for the duplicate flag column |

**Example Prompts:**
- "Flag duplicate rows based on email"
- "Mark all duplicate entries (don't keep any as original)"
- "Identify duplicate orders by customer_id and product_id"

---

### detect_header

**Description:** Find the header row in data that has junk/metadata rows before it

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `max_rows_to_check` | `int` | No | `20` | Maximum rows to analyze for header detection |
| `expected_columns` | `list[str]` | No | `None` | Expected column names to help identify header row |
| `min_string_columns` | `int` | No | `2` | Minimum number of string-like values to consider as header |
| `apply_header` | `bool` | No | `True` | If True, set the detected row as header and remove junk rows |

**Example Prompts:**
- "Find the header row in this messy data"
- "Detect header row, expecting columns: name, email, phone"
- "Find where the actual data headers start"

---

### validate_schema

**Description:** Check if columns match expected names and types

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `expected_columns` | `list[str]` | No | `None` | List of expected column names |
| `expected_types` | `dict[str, str]` | No | `None` | Expected types per column: {col: 'string'\|'number'\|'date'\|'boolean'} |
| `allow_extra_columns` | `bool` | No | `True` | Allow columns not in expected list |
| `case_sensitive` | `bool` | No | `False` | Case-sensitive column name matching |

**Example Prompts:**
- "Validate that columns include name, email, and phone"
- "Check schema: amount should be number, status should be string"
- "Ensure data has exactly these columns: id, name, value"

---

### infer_types

**Description:** Analyze and optionally convert columns to their detected types

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `columns` | `list[str]` | No | `None` | Columns to analyze (default: all) |
| `apply_conversion` | `bool` | No | `False` | If True, convert columns to detected types |
| `sample_size` | `int` | No | `1000` | Number of rows to sample for type detection |

**Example Prompts:**
- "Detect the data types of all columns"
- "Auto-detect and convert column types"
- "Analyze types for the amount and date columns"

---

### compare_schemas

**Description:** Compare expected schema against observed data and produce a detailed diff

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `expected_columns` | `list[str]` | Yes | - | List of expected column names |
| `expected_types` | `dict[str, str]` | No | `None` | Expected types: {col: 'string'\|'integer'\|'float'\|'datetime'\|'boolean'} |
| `expected_nullable` | `dict[str, bool]` | No | `None` | Expected nullability: {col: True/False} |
| `case_sensitive` | `bool` | No | `False` | Case-sensitive column name comparison |

**Example Prompts:**
- "Compare data against expected schema with columns: id, name, email, amount"
- "Check if schema matches: id (integer), name (string), amount (float)"
- "Compare schema and check that email cannot be null"

---

### detect_renamed_columns

**Description:** Suggest potential column renames by comparing expected vs observed using fuzzy matching

**Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `expected_columns` | `list[str]` | Yes | - | List of expected column names |
| `similarity_threshold` | `float` | No | `0.6` | Minimum similarity score (0-1) to suggest a rename |
| `check_type_compatibility` | `bool` | No | `True` | Also check if data types are compatible |

**Example Prompts:**
- "Find renamed columns, expected: customer_name, phone_number, email_address"
- "Suggest column renames with 70% similarity threshold"
- "Find possible renamed columns ignoring type compatibility"

---

## Deterministic Execution Model

All primitives in ModularData follow a deterministic execution model:

1. **Input Validation**: Each primitive validates its parameters before execution
2. **Immutable Operations**: The original DataFrame is never modified; a copy is always returned
3. **Reproducible Results**: Given the same input DataFrame and parameters, the output is always identical
4. **Structured Results**: Every execution returns a `PrimitiveResult` containing:
   - `success`: Boolean indicating operation success
   - `df`: The transformed DataFrame (if successful)
   - `error`: Error message (if failed)
   - `rows_before` / `rows_after`: Row count changes
   - `cols_before` / `cols_after`: Column count changes
   - `warnings`: Any non-fatal issues encountered
   - `metadata`: Operation-specific statistics

This deterministic nature ensures that transformation chains can be reliably reproduced, debugged, and optimized.

---

## Version Information

- **Document Version**: 1.0
- **Last Updated**: January 2026
- **Primitive Count**: 60+ primitives across 9 categories
