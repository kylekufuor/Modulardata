# transforms_v2 Test Prompts

This file contains all test prompts used to train the Strategist.
Each primitive needs 3+ natural language prompts that users might say.

**To add prompts**: Edit the primitives test_prompts list in the corresponding file.

**Total: 96 prompts across 32 primitives**

---

## ROWS

### add_rows
_Add new rows to the top or bottom of the data_

1. **"Add a new row with name='Test User' and email='test@example.com'"**
   - Params: {'rows': [{'name': 'Test User', 'email': 'test@example.com'}], 'position': 'bottom'}

2. **"Insert a header row at the top with column descriptions"**
   - Params: {'rows': [{'description': 'header row data'}], 'position': 'top'}

3. **"Append three new product entries to the list"**
   - Params: {'rows': [{'product': 'entry1'}, {'product': 'entry2'}, {'product': 'entry3'}], 'position': 'bottom'}

---

### fill_blanks
_Fill null or blank values using various methods_

1. **"Fill empty lead_score values with 0"**
   - Params: {'column': 'lead_score', 'method': 'value', 'value': 0}

2. **"Replace missing prices with the average price"**
   - Params: {'column': 'price', 'method': 'mean'}

3. **"Fill blank status fields with 'Unknown'"**
   - Params: {'column': 'status', 'method': 'value', 'value': 'Unknown'}

---

### filter_rows
_Keep or remove rows based on filter conditions_

1. **"Show only rows where lead_score is greater than 80"**
   - Params: {'conditions': [{'column': 'lead_score', 'operator': 'gt', 'value': 80}], 'keep': True}

2. **"Remove all rows where email is empty or null"**
   - Params: {'conditions': [{'column': 'email', 'operator': 'isnull'}], 'keep': False}

3. **"Keep rows where status is either 'active' or 'pending'"**
   - Params: {'conditions': [{'column': 'status', 'operator': 'in', 'value': ['active', 'pending']}], 'keep': True}

---

### limit_rows
_Keep only the first or last N rows_

1. **"Keep only the first 100 rows"**
   - Params: {'count': 100, 'from_end': False}

2. **"Get the last 50 records"**
   - Params: {'count': 50, 'from_end': True}

3. **"Show me the top 10 entries"**
   - Params: {'count': 10, 'from_end': False}

---

### merge_duplicates
_Merge duplicate rows by combining values (sum, concat, max, etc.)_

1. **"Combine duplicate orders by customer_id, summing the amounts"**
   - Params: {'group_by': ['customer_id'], 'aggregations': {'amount': 'sum'}}

2. **"Merge duplicate contacts by email, keeping the most recent activity_date"**
   - Params: {'group_by': ['email'], 'aggregations': {'activity_date': 'max'}}

3. **"Consolidate rows with same product_id, concatenating all notes"**
   - Params: {'group_by': ['product_id'], 'aggregations': {'notes': 'concat'}}

---

### remove_duplicates
_Remove duplicate rows, keeping the first or last occurrence_

1. **"Remove duplicate rows based on email address"**
   - Params: {'subset': ['email'], 'keep': 'first'}

2. **"Delete all duplicate entries, keeping the most recent one"**
   - Params: {'subset': None, 'keep': 'last'}

3. **"Remove rows where lead_id and email are duplicated"**
   - Params: {'subset': ['lead_id', 'email'], 'keep': 'first'}

---

### sort_rows
_Sort rows by one or more columns in ascending or descending order_

1. **"Sort the data by lead_score from highest to lowest"**
   - Params: {'columns': ['lead_score'], 'ascending': False}

2. **"Arrange rows alphabetically by last name"**
   - Params: {'columns': ['last_name'], 'ascending': True}

3. **"Order by created_date newest first, then by name A-Z"**
   - Params: {'columns': ['created_date', 'name'], 'ascending': [False, True]}

---

## COLUMNS

### add_column
_Add a new column with a constant value or derived from existing columns_

1. **"Add a new column called 'source' with value 'import'"**
   - Params: {'name': 'source', 'value': 'import'}

2. **"Create a 'processed' column set to False"**
   - Params: {'name': 'processed', 'value': False}

3. **"Add an empty 'notes' column"**
   - Params: {'name': 'notes', 'value': ''}

---

### change_column_type
_Convert a column to a different data type_

1. **"Convert the lead_score column to integers"**
   - Params: {'column': 'lead_score', 'to_type': 'integer'}

2. **"Change the created_date column to datetime format"**
   - Params: {'column': 'created_date', 'to_type': 'datetime'}

3. **"Make the price column a float"**
   - Params: {'column': 'price', 'to_type': 'float'}

---

### copy_column
_Create a copy of an existing column with a new name_

1. **"Make a copy of the email column called email_backup"**
   - Params: {'source': 'email', 'destination': 'email_backup'}

2. **"Duplicate the price column as original_price"**
   - Params: {'source': 'price', 'destination': 'original_price'}

3. **"Create a copy of status called status_original"**
   - Params: {'source': 'status', 'destination': 'status_original'}

---

### merge_columns
_Combine multiple columns into a single column_

1. **"Combine first_name and last_name into full_name with a space"**
   - Params: {'columns': ['first_name', 'last_name'], 'new_column': 'full_name', 'separator': ' '}

2. **"Join city, state, and zip into 'location' separated by ', '"**
   - Params: {'columns': ['city', 'state', 'zip'], 'new_column': 'location', 'separator': ', '}

3. **"Concatenate area_code and phone_number into phone with a dash"**
   - Params: {'columns': ['area_code', 'phone_number'], 'new_column': 'phone', 'separator': '-'}

---

### remove_columns
_Remove the specified columns from the data_

1. **"Delete the internal_id and created_by columns"**
   - Params: {'columns': ['internal_id', 'created_by']}

2. **"Remove the password field from the data"**
   - Params: {'columns': ['password']}

3. **"Drop the temporary columns temp1 and temp2"**
   - Params: {'columns': ['temp1', 'temp2']}

---

### rename_columns
_Rename one or more columns_

1. **"Rename 'cust_id' to 'customer_id'"**
   - Params: {'mapping': {'cust_id': 'customer_id'}}

2. **"Change column names: fname to first_name and lname to last_name"**
   - Params: {'mapping': {'fname': 'first_name', 'lname': 'last_name'}}

3. **"Rename the 'amt' column to 'amount'"**
   - Params: {'mapping': {'amt': 'amount'}}

---

### reorder_columns
_Change the order of columns_

1. **"Move email to the first column"**
   - Params: {'order': ['email']}

2. **"Arrange columns as: id, name, email, phone, status"**
   - Params: {'order': ['id', 'name', 'email', 'phone', 'status']}

3. **"Put the date column at the beginning"**
   - Params: {'order': ['date']}

---

### select_columns
_Keep only the specified columns, removing all others_

1. **"Keep only the name, email, and phone columns"**
   - Params: {'columns': ['name', 'email', 'phone']}

2. **"I only need the customer_id and order_total fields"**
   - Params: {'columns': ['customer_id', 'order_total']}

3. **"Show me just the first_name and last_name columns"**
   - Params: {'columns': ['first_name', 'last_name']}

---

### split_column
_Split a column into multiple columns based on a delimiter_

1. **"Split the 'full_name' column into 'first_name' and 'last_name' by space"**
   - Params: {'column': 'full_name', 'delimiter': ' ', 'new_columns': ['first_name', 'last_name']}

2. **"Separate the address column by comma into street, city, state"**
   - Params: {'column': 'address', 'delimiter': ',', 'new_columns': ['street', 'city', 'state']}

3. **"Break apart the date column using '/' into month, day, year"**
   - Params: {'column': 'date', 'delimiter': '/', 'new_columns': ['month', 'day', 'year']}

---

## FORMAT

### change_text_casing
_Change text to uppercase, lowercase, title case, or sentence case_

1. **"Convert all names to title case"**
   - Params: {'column': 'name', 'case': 'title'}

2. **"Make the email column all lowercase"**
   - Params: {'column': 'email', 'case': 'lower'}

3. **"Change status values to uppercase"**
   - Params: {'column': 'status', 'case': 'upper'}

---

### format_date
_Standardize dates to a consistent format_

1. **"Standardize all dates in created_date to YYYY-MM-DD format"**
   - Params: {'column': 'created_date', 'output_format': '%Y-%m-%d'}

2. **"Format the date column as MM/DD/YYYY"**
   - Params: {'column': 'date', 'output_format': '%m/%d/%Y'}

3. **"Convert order_date to display as 'January 15, 2024'"**
   - Params: {'column': 'order_date', 'output_format': '%B %d, %Y'}

---

### format_phone
_Standardize phone numbers to a consistent format_

1. **"Format phone numbers as (555) 123-4567"**
   - Params: {'column': 'phone', 'format': '(XXX) XXX-XXXX'}

2. **"Standardize the phone_number column to XXX-XXX-XXXX format"**
   - Params: {'column': 'phone_number', 'format': 'XXX-XXX-XXXX'}

3. **"Clean up phone numbers to just digits"**
   - Params: {'column': 'phone', 'format': 'XXXXXXXXXX'}

---

### standardize_values
_Replace multiple variant values with a single standard value_

1. **"Standardize status values: replace 'Qualifed', 'qualfied', 'QUALIFIED' with 'Qualified'"**
   - Params: {'column': 'status', 'mapping': {'Qualified': ['Qualifed', 'qualfied', 'QUALIFIED']}}

2. **"Normalize campaign_source: 'google ads' and 'Google Ads' should be 'Google Ads'"**
   - Params: {'column': 'campaign_source', 'mapping': {'Google Ads': ['google ads', 'Google ads', 'GOOGLE ADS']}}

3. **"Replace 'Y', 'yes', 'YES' with 'Yes' and 'N', 'no', 'NO' with 'No' in the confirmed column"**
   - Params: {'column': 'confirmed', 'mapping': {'Yes': ['Y', 'yes', 'YES'], 'No': ['N', 'no', 'NO']}}

---

### trim_whitespace
_Remove leading, trailing, and extra whitespace from text_

1. **"Remove extra spaces from all text columns"**
   - Params: {'columns': None, 'trim_type': 'all'}

2. **"Trim whitespace from the name and email columns"**
   - Params: {'columns': ['name', 'email'], 'trim_type': 'both'}

3. **"Clean up leading and trailing spaces in the description"**
   - Params: {'columns': ['description'], 'trim_type': 'both'}

---

## TEXT

### extract_text
_Extract a portion of text using position, pattern, or delimiter_

1. **"Extract the first 3 characters from product_code into a new column called category_code"**
   - Params: {'column': 'product_code', 'new_column': 'category_code', 'method': 'position', 'start': 0, 'end': 3}

2. **"Extract the domain from email addresses into a column called email_domain"**
   - Params: {'column': 'email', 'new_column': 'email_domain', 'method': 'after', 'delimiter': '@'}

3. **"Extract the number from strings like 'Order #12345' into order_number"**
   - Params: {'column': 'order_text', 'new_column': 'order_number', 'method': 'regex', 'pattern': '#(\\d+)'}

---

### find_replace
_Find and replace text values using literal strings or regex patterns_

1. **"Replace all occurrences of 'N/A' with 'Unknown' in the status column"**
   - Params: {'column': 'status', 'find': 'N/A', 'replace': 'Unknown'}

2. **"Remove all dollar signs from the price column"**
   - Params: {'column': 'price', 'find': '$', 'replace': ''}

3. **"Replace any sequence of digits in notes with [REDACTED]"**
   - Params: {'column': 'notes', 'find': '\\d+', 'replace': '[REDACTED]', 'use_regex': True}

---

### pad_text
_Add leading or trailing characters to reach a fixed length_

1. **"Zero-pad the employee_id column to 6 digits"**
   - Params: {'column': 'employee_id', 'length': 6, 'pad_char': '0', 'side': 'left'}

2. **"Pad product codes with leading zeros to 10 characters"**
   - Params: {'column': 'product_code', 'length': 10, 'pad_char': '0', 'side': 'left'}

3. **"Add trailing spaces to name column to make all values 20 characters"**
   - Params: {'column': 'name', 'length': 20, 'pad_char': ' ', 'side': 'right'}

---

### remove_characters
_Strip specific characters or character types from text_

1. **"Remove all dollar signs and commas from the amount column"**
   - Params: {'column': 'amount', 'characters': '$,'}

2. **"Strip all digits from the reference column"**
   - Params: {'column': 'reference', 'remove_type': 'digits'}

3. **"Remove punctuation from the comments field"**
   - Params: {'column': 'comments', 'remove_type': 'punctuation'}

---

### text_length
_Get the character count of text values in a column_

1. **"Add a column showing the length of each description"**
   - Params: {'column': 'description', 'new_column': 'description_length'}

2. **"Create a character count column for the name field called name_chars"**
   - Params: {'column': 'name', 'new_column': 'name_chars'}

3. **"Calculate the length of comments without counting spaces"**
   - Params: {'column': 'comments', 'new_column': 'comments_length', 'count_spaces': False}

---

## CALCULATE

### conditional_value
_Set column values based on if/then conditions_

1. **"Create a grade column: A if score >= 90, B if >= 80, C if >= 70, else F"**
   - Params: {'new_column': 'grade', 'conditions': [{'column': 'score', 'operator': 'gte', 'compare_value': 90, 'value': 'A'}, {'column': 'score', 'operator': 'gte', 'compare_value': 80, 'value': 'B'}, {'column': 'score', 'operator': 'gte', 'compare_value': 70, 'value': 'C'}], 'default': 'F'}

2. **"Add a status column: 'High' if amount > 1000, otherwise 'Low'"**
   - Params: {'new_column': 'status', 'conditions': [{'column': 'amount', 'operator': 'gt', 'compare_value': 1000, 'value': 'High'}], 'default': 'Low'}

3. **"Create a tier column based on customer spend"**
   - Params: {'new_column': 'tier', 'conditions': [{'column': 'total_spend', 'operator': 'gte', 'compare_value': 10000, 'value': 'Platinum'}, {'column': 'total_spend', 'operator': 'gte', 'compare_value': 5000, 'value': 'Gold'}, {'column': 'total_spend', 'operator': 'gte', 'compare_value': 1000, 'value': 'Silver'}], 'default': 'Bronze'}

---

### math_operation
_Add, subtract, multiply, or divide columns or apply constants_

1. **"Calculate total by multiplying quantity and price into a new column called total"**
   - Params: {'new_column': 'total', 'operation': 'multiply', 'column1': 'quantity', 'column2': 'price'}

2. **"Add a 10% markup to the cost column and store in final_price"**
   - Params: {'new_column': 'final_price', 'operation': 'multiply', 'column1': 'cost', 'value': 1.1}

3. **"Calculate profit by subtracting cost from revenue"**
   - Params: {'new_column': 'profit', 'operation': 'subtract', 'column1': 'revenue', 'column2': 'cost'}

---

### percentage
_Calculate percentage of column total or ratio between columns_

1. **"Calculate what percentage each sale is of total sales"**
   - Params: {'column': 'sale_amount', 'new_column': 'percent_of_total', 'mode': 'of_total'}

2. **"Calculate completion rate as completed divided by total"**
   - Params: {'column': 'completed', 'new_column': 'completion_rate', 'mode': 'ratio', 'denominator_column': 'total'}

3. **"Show each region's contribution as a percentage of total revenue"**
   - Params: {'column': 'revenue', 'new_column': 'revenue_pct', 'mode': 'of_total'}

---

### rank
_Assign rank to rows based on column values_

1. **"Rank employees by salary from highest to lowest"**
   - Params: {'column': 'salary', 'new_column': 'salary_rank', 'ascending': False}

2. **"Add a rank column for scores, lowest first"**
   - Params: {'column': 'score', 'new_column': 'rank', 'ascending': True}

3. **"Rank products by sales within each category"**
   - Params: {'column': 'sales', 'new_column': 'category_rank', 'ascending': False, 'group_by': 'category'}

---

### round_numbers
_Round numeric values to a specified number of decimal places_

1. **"Round the price column to 2 decimal places"**
   - Params: {'column': 'price', 'decimals': 2}

2. **"Round down all values in quantity to whole numbers"**
   - Params: {'column': 'quantity', 'decimals': 0, 'method': 'floor'}

3. **"Round up the shipping_cost to the nearest dollar"**
   - Params: {'column': 'shipping_cost', 'decimals': 0, 'method': 'ceil'}

---

### running_total
_Calculate a running/cumulative total for a numeric column_

1. **"Add a running total column for the amount field"**
   - Params: {'column': 'amount', 'new_column': 'running_total'}

2. **"Calculate cumulative sales for each month"**
   - Params: {'column': 'sales', 'new_column': 'cumulative_sales'}

3. **"Create a running balance grouped by account_id"**
   - Params: {'column': 'transaction_amount', 'new_column': 'running_balance', 'group_by': 'account_id'}

---
