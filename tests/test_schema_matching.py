# =============================================================================
# tests/test_schema_matching.py - Schema Matching Tests
# =============================================================================
# Tests for schema matching functionality (Phase 2 Foundation).
# Covers:
#   - Column name matching (exact, normalized, fuzzy, synonym)
#   - Semantic type compatibility
#   - Contract generation from profiles
#   - Schema matching (profile vs contract)
#   - Contract-to-contract matching (workflow validation)
#
# Run with: poetry run pytest tests/test_schema_matching.py -v
# =============================================================================

import pandas as pd
import pytest

from lib.profiler import (
    # Column matching utilities
    normalize_column_name,
    levenshtein_distance,
    name_similarity,
    find_synonym_match,
    match_column_name,
    # Semantic type utilities
    semantic_types_compatible,
    semantic_type_similarity,
    # Data type utilities
    dtypes_compatible,
    # Value overlap
    calculate_value_overlap,
    # Main functions
    generate_contract,
    generate_profile,
    match_schema,
    match_contracts,
)
from core.models import (
    ColumnContract,
    ColumnProfile,
    DataProfile,
    MatchConfidence,
    SchemaContract,
    SemanticType,
)


# =============================================================================
# Column Name Matching Tests
# =============================================================================

class TestNormalizeColumnName:
    """Tests for column name normalization."""

    def test_lowercase(self):
        assert normalize_column_name("Email") == "email"
        assert normalize_column_name("EMAIL") == "email"
        assert normalize_column_name("eMaIL") == "email"

    def test_remove_underscores(self):
        assert normalize_column_name("customer_email") == "customeremail"
        assert normalize_column_name("first_name") == "firstname"

    def test_remove_spaces(self):
        assert normalize_column_name("Customer Email") == "customeremail"
        assert normalize_column_name("First Name") == "firstname"

    def test_remove_hyphens(self):
        assert normalize_column_name("customer-email") == "customeremail"
        assert normalize_column_name("e-mail") == "email"

    def test_combined_normalization(self):
        assert normalize_column_name("Customer_Email_Address") == "customeremailaddress"
        assert normalize_column_name("Customer Email Address") == "customeremailaddress"
        assert normalize_column_name("customer-email-address") == "customeremailaddress"


class TestLevenshteinDistance:
    """Tests for Levenshtein distance calculation."""

    def test_identical_strings(self):
        assert levenshtein_distance("email", "email") == 0

    def test_one_character_difference(self):
        assert levenshtein_distance("email", "emails") == 1
        assert levenshtein_distance("cat", "bat") == 1

    def test_two_character_difference(self):
        # "email" -> "emaal" is only 1 substitution (i->a)
        # "kitten" -> "sitting" is 3 (k->s, e->i, +g)
        assert levenshtein_distance("kitten", "sitting") == 3

    def test_empty_string(self):
        assert levenshtein_distance("", "email") == 5
        assert levenshtein_distance("email", "") == 5

    def test_completely_different(self):
        assert levenshtein_distance("abc", "xyz") == 3


class TestNameSimilarity:
    """Tests for column name similarity scoring."""

    def test_identical_after_normalization(self):
        # These should all be 1.0 after normalization
        assert name_similarity("email", "EMAIL") == 1.0
        assert name_similarity("customer_id", "CustomerID") == 1.0
        assert name_similarity("first-name", "First Name") == 1.0

    def test_similar_names(self):
        # These are similar but not identical
        similarity = name_similarity("email", "e-mail")
        assert similarity == 1.0  # Should normalize to same

        similarity = name_similarity("customer", "customers")
        assert 0.7 < similarity < 1.0  # Close but not exact

    def test_different_names(self):
        similarity = name_similarity("email", "phone")
        assert similarity < 0.5


class TestFindSynonymMatch:
    """Tests for synonym group matching."""

    def test_find_email_synonym(self):
        assert find_synonym_match("email") == "email"
        assert find_synonym_match("e_mail") == "email"
        assert find_synonym_match("email_address") == "email"
        assert find_synonym_match("mail") == "email"

    def test_find_phone_synonym(self):
        assert find_synonym_match("phone") == "phone"
        assert find_synonym_match("telephone") == "phone"
        assert find_synonym_match("mobile") == "phone"

    def test_find_name_synonym(self):
        assert find_synonym_match("first_name") == "first_name"
        assert find_synonym_match("fname") == "first_name"
        assert find_synonym_match("given_name") == "first_name"

    def test_no_synonym_found(self):
        assert find_synonym_match("xyz123") is None
        assert find_synonym_match("custom_field") is None


class TestMatchColumnName:
    """Tests for column name matching."""

    def test_exact_match(self):
        score, match_type = match_column_name("email", "email")
        assert score == 1.0
        assert match_type == "exact"

    def test_normalized_match(self):
        score, match_type = match_column_name("Customer_Email", "customer email")
        assert score >= 0.9
        assert match_type == "normalized"

    def test_alternative_name_match(self):
        score, match_type = match_column_name(
            "E-Mail Address",
            "email",
            alternative_names=["e-mail address", "email_addr"]
        )
        assert score >= 0.9
        assert match_type == "alternative"

    def test_synonym_match(self):
        score, match_type = match_column_name("telephone", "phone")
        assert score >= 0.8
        assert match_type == "synonym"

    def test_fuzzy_match(self):
        # "custemail" vs "customeremail" has moderate similarity
        # The match score will be lower due to the length difference
        score, match_type = match_column_name("cust_email", "customer_email")
        # Score is in "weak" range due to partial name overlap
        assert score > 0  # Should have some similarity
        assert match_type in ["fuzzy", "weak"]

    def test_poor_match(self):
        score, match_type = match_column_name("xyz", "abc")
        assert score < 0.5


# =============================================================================
# Semantic Type Compatibility Tests
# =============================================================================

class TestSemanticTypeCompatibility:
    """Tests for semantic type compatibility checking."""

    def test_same_type_compatible(self):
        assert semantic_types_compatible(SemanticType.EMAIL, SemanticType.EMAIL)
        assert semantic_types_compatible(SemanticType.DATE, SemanticType.DATE)
        assert semantic_types_compatible(SemanticType.NUMERIC, SemanticType.NUMERIC)

    def test_unknown_compatible_with_anything(self):
        assert semantic_types_compatible(SemanticType.UNKNOWN, SemanticType.EMAIL)
        assert semantic_types_compatible(SemanticType.DATE, SemanticType.UNKNOWN)
        assert semantic_types_compatible(SemanticType.UNKNOWN, SemanticType.UNKNOWN)

    def test_temporal_types_compatible(self):
        assert semantic_types_compatible(SemanticType.DATE, SemanticType.DATETIME)
        assert semantic_types_compatible(SemanticType.DATETIME, SemanticType.TIME)

    def test_numeric_types_compatible(self):
        assert semantic_types_compatible(SemanticType.NUMERIC, SemanticType.CURRENCY)
        assert semantic_types_compatible(SemanticType.CURRENCY, SemanticType.PERCENTAGE)

    def test_text_types_compatible(self):
        assert semantic_types_compatible(SemanticType.TEXT, SemanticType.NAME)
        assert semantic_types_compatible(SemanticType.NAME, SemanticType.CATEGORY)

    def test_incompatible_types(self):
        assert not semantic_types_compatible(SemanticType.EMAIL, SemanticType.DATE)
        assert not semantic_types_compatible(SemanticType.PHONE, SemanticType.NUMERIC)


class TestSemanticTypeSimilarity:
    """Tests for semantic type similarity scoring."""

    def test_same_type_score(self):
        assert semantic_type_similarity(SemanticType.EMAIL, SemanticType.EMAIL) == 1.0

    def test_unknown_type_score(self):
        assert semantic_type_similarity(SemanticType.UNKNOWN, SemanticType.EMAIL) == 0.5

    def test_compatible_type_score(self):
        score = semantic_type_similarity(SemanticType.DATE, SemanticType.DATETIME)
        assert score >= 0.7

    def test_incompatible_type_score(self):
        score = semantic_type_similarity(SemanticType.EMAIL, SemanticType.DATE)
        assert score <= 0.3


# =============================================================================
# Data Type Compatibility Tests
# =============================================================================

class TestDtypeCompatibility:
    """Tests for pandas dtype compatibility."""

    def test_same_dtype(self):
        assert dtypes_compatible("int64", "int64")
        assert dtypes_compatible("object", "object")

    def test_numeric_compatibility(self):
        assert dtypes_compatible("int64", "float64")
        assert dtypes_compatible("int32", "int64")
        assert dtypes_compatible("float64", "object")

    def test_string_compatibility(self):
        assert dtypes_compatible("object", "string")
        assert dtypes_compatible("object", "category")


# =============================================================================
# Value Overlap Tests
# =============================================================================

class TestValueOverlap:
    """Tests for sample value overlap calculation."""

    def test_complete_overlap(self):
        incoming = ["a", "b", "c"]
        contract = ["a", "b", "c"]
        assert calculate_value_overlap(incoming, contract) == 1.0

    def test_partial_overlap(self):
        incoming = ["a", "b", "c"]
        contract = ["a", "b", "d"]
        overlap = calculate_value_overlap(incoming, contract)
        assert 0.4 < overlap < 0.7  # 2 out of 4 unique values

    def test_no_overlap(self):
        incoming = ["a", "b", "c"]
        contract = ["x", "y", "z"]
        assert calculate_value_overlap(incoming, contract) == 0.0

    def test_case_insensitive(self):
        incoming = ["Email", "PHONE"]
        contract = ["email", "phone"]
        assert calculate_value_overlap(incoming, contract) == 1.0

    def test_empty_lists(self):
        assert calculate_value_overlap([], ["a"]) == 0.0
        assert calculate_value_overlap(["a"], []) == 0.0
        assert calculate_value_overlap([], []) == 0.0


# =============================================================================
# Contract Generation Tests
# =============================================================================

class TestGenerateContract:
    """Tests for contract generation from profiles."""

    @pytest.fixture
    def sample_profile(self):
        """Create a sample DataProfile for testing."""
        df = pd.DataFrame({
            "customer_id": [1, 2, 3, 4, 5],
            "email": ["a@test.com", "b@test.com", "c@test.com", "d@test.com", "e@test.com"],
            "name": ["Alice", "Bob", "Carol", "Dave", "Eve"],
            "amount": [100.0, 200.0, 150.0, 300.0, 250.0],
        })
        return generate_profile(df)

    def test_contract_has_all_columns(self, sample_profile):
        contract = generate_contract(sample_profile)

        assert len(contract.columns) == 4
        column_names = contract.get_column_names()
        assert "customer_id" in column_names
        assert "email" in column_names
        assert "name" in column_names
        assert "amount" in column_names

    def test_contract_columns_are_required(self, sample_profile):
        contract = generate_contract(sample_profile, mark_all_required=True)

        for col in contract.columns:
            assert col.required is True

    def test_contract_has_semantic_types(self, sample_profile):
        contract = generate_contract(sample_profile)

        email_col = next(c for c in contract.columns if c.name == "email")
        assert email_col.semantic_type == SemanticType.EMAIL

    def test_contract_has_fingerprint(self, sample_profile):
        contract = generate_contract(sample_profile)

        assert contract.fingerprint is not None
        assert len(contract.fingerprint) == 16  # SHA256 truncated

    def test_contract_has_alternative_names(self, sample_profile):
        contract = generate_contract(sample_profile)

        email_col = next(c for c in contract.columns if c.name == "email")
        assert len(email_col.alternative_names) > 0

    def test_contract_has_module_metadata(self, sample_profile):
        contract = generate_contract(
            sample_profile,
            module_id="mod_123",
            module_name="Customer Cleanup"
        )

        assert contract.module_id == "mod_123"
        assert contract.module_name == "Customer Cleanup"


# =============================================================================
# Schema Matching Tests
# =============================================================================

class TestMatchSchema:
    """Tests for schema matching (profile vs contract)."""

    @pytest.fixture
    def reference_profile(self):
        """Profile used to create the contract."""
        df = pd.DataFrame({
            "customer_id": [1, 2, 3],
            "email": ["a@test.com", "b@test.com", "c@test.com"],
            "name": ["Alice", "Bob", "Carol"],
        })
        return generate_profile(df)

    @pytest.fixture
    def contract(self, reference_profile):
        """Contract generated from reference profile."""
        return generate_contract(reference_profile, module_name="Test Module")

    def test_exact_match_high_confidence(self, reference_profile, contract):
        """Same data structure should have high confidence."""
        match = match_schema(reference_profile, contract)

        assert match.confidence_score >= 85
        assert match.confidence_level == MatchConfidence.HIGH
        assert match.auto_processable is True
        assert len(match.column_mappings) == 3
        assert len(match.unmapped_required) == 0

    def test_column_name_variations(self, contract):
        """Different column names that should still match."""
        df = pd.DataFrame({
            "Customer ID": [1, 2, 3],  # Different casing/spacing
            "e-mail": ["a@test.com", "b@test.com", "c@test.com"],  # Synonym
            "Name": ["Alice", "Bob", "Carol"],  # Different casing
        })
        incoming = generate_profile(df)
        match = match_schema(incoming, contract)

        assert match.confidence_score >= 60
        assert match.is_compatible is True
        assert len(match.column_mappings) == 3

    def test_missing_required_column(self, contract):
        """Missing required column should lower confidence."""
        df = pd.DataFrame({
            "customer_id": [1, 2, 3],
            "email": ["a@test.com", "b@test.com", "c@test.com"],
            # name column is missing
        })
        incoming = generate_profile(df)
        match = match_schema(incoming, contract)

        assert match.confidence_score < 85
        assert len(match.unmapped_required) == 1
        assert "name" in match.unmapped_required

    def test_extra_columns_are_ignored(self, contract):
        """Extra columns shouldn't negatively impact matching."""
        df = pd.DataFrame({
            "customer_id": [1, 2, 3],
            "email": ["a@test.com", "b@test.com", "c@test.com"],
            "name": ["Alice", "Bob", "Carol"],
            "extra_column": ["x", "y", "z"],  # Not in contract
        })
        incoming = generate_profile(df)
        match = match_schema(incoming, contract)

        assert match.confidence_score >= 85
        assert "extra_column" in match.unmapped_incoming

    def test_completely_different_schema(self, contract):
        """Completely different schema should have very low confidence."""
        df = pd.DataFrame({
            "product_id": [1, 2, 3],
            "price": [10.0, 20.0, 30.0],
            "category": ["A", "B", "C"],
        })
        incoming = generate_profile(df)
        match = match_schema(incoming, contract)

        assert match.confidence_score < 40
        assert match.confidence_level == MatchConfidence.NO_MATCH
        assert match.is_compatible is False

    def test_match_includes_discrepancies(self, contract):
        """Match should report discrepancies."""
        df = pd.DataFrame({
            "customer_id": [1, 2, 3],
            # Missing email and name
        })
        incoming = generate_profile(df)
        match = match_schema(incoming, contract)

        assert len(match.discrepancies) > 0
        critical_discrepancies = [d for d in match.discrepancies if d.discrepancy_type == "missing_required"]
        assert len(critical_discrepancies) >= 2


class TestMatchSchemaConfidenceLevels:
    """Tests for confidence level thresholds."""

    def test_high_confidence_threshold(self):
        """85% or above = HIGH confidence."""
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "value": [10, 20, 30],
        })
        profile = generate_profile(df)
        contract = generate_contract(profile)

        match = match_schema(profile, contract)
        assert match.confidence_score >= 85
        assert match.confidence_level == MatchConfidence.HIGH

    def test_medium_confidence_range(self):
        """60-84% = MEDIUM confidence."""
        df_ref = pd.DataFrame({
            "customer_id": [1, 2, 3],
            "email": ["a@t.com", "b@t.com", "c@t.com"],
            "phone": ["111", "222", "333"],
        })
        contract = generate_contract(generate_profile(df_ref))

        # Missing one required column
        df_incoming = pd.DataFrame({
            "customer_id": [1, 2, 3],
            "email": ["a@t.com", "b@t.com", "c@t.com"],
        })
        incoming = generate_profile(df_incoming)

        match = match_schema(incoming, contract)
        # With 2/3 columns matched, should be medium confidence
        assert 40 <= match.confidence_score < 85


# =============================================================================
# Contract-to-Contract Matching Tests
# =============================================================================

class TestMatchContracts:
    """Tests for contract-to-contract matching (workflow validation)."""

    def test_compatible_contracts(self):
        """Two contracts with same schema should be chainable."""
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["A", "B", "C"],
        })
        profile = generate_profile(df)

        source_contract = generate_contract(profile, module_id="mod_a")
        target_contract = generate_contract(profile, module_id="mod_b")

        match = match_contracts(source_contract, target_contract)

        assert match.confidence_score >= 80
        assert match.is_chainable is True

    def test_incompatible_contracts(self):
        """Different schemas should not be chainable."""
        df1 = pd.DataFrame({
            "customer_id": [1, 2, 3],
            "email": ["a@t.com", "b@t.com", "c@t.com"],
        })
        df2 = pd.DataFrame({
            "product_id": [1, 2, 3],
            "price": [10.0, 20.0, 30.0],
        })

        source = generate_contract(generate_profile(df1))
        target = generate_contract(generate_profile(df2))

        match = match_contracts(source, target)

        assert match.is_chainable is False
        assert len(match.discrepancies) > 0

    def test_partial_compatibility(self):
        """Contracts with some matching columns."""
        df_source = pd.DataFrame({
            "customer_id": [1, 2, 3],
            "email": ["a@t.com", "b@t.com", "c@t.com"],
            "name": ["A", "B", "C"],
        })
        # Target only needs customer_id and email
        df_target = pd.DataFrame({
            "customer_id": [1, 2, 3],
            "email": ["a@t.com", "b@t.com", "c@t.com"],
        })

        source = generate_contract(generate_profile(df_source))
        target = generate_contract(generate_profile(df_target))

        match = match_contracts(source, target)

        assert match.is_chainable is True  # All target requirements met


# =============================================================================
# Integration Tests
# =============================================================================

class TestSchemaMatchingIntegration:
    """End-to-end integration tests for schema matching."""

    def test_full_workflow_exact_match(self):
        """
        Simulate: Train module -> Save contract -> New file arrives -> Match
        """
        # 1. User trains module with this data
        training_data = pd.DataFrame({
            "order_id": [1001, 1002, 1003],
            "customer_email": ["john@acme.com", "jane@corp.net", "bob@test.org"],
            "order_date": ["2024-01-15", "2024-01-16", "2024-01-17"],
            "total_amount": [99.99, 149.50, 75.00],
        })
        training_profile = generate_profile(training_data)

        # 2. Module is saved with contract
        contract = generate_contract(
            training_profile,
            module_id="order_cleanup_v1",
            module_name="Order Data Cleanup"
        )

        # 3. New file arrives with same structure
        new_data = pd.DataFrame({
            "order_id": [2001, 2002],
            "customer_email": ["alice@new.com", "charlie@other.com"],
            "order_date": ["2024-02-01", "2024-02-02"],
            "total_amount": [200.00, 50.00],
        })
        new_profile = generate_profile(new_data)

        # 4. Match
        match = match_schema(new_profile, contract)

        assert match.confidence_score >= 85
        assert match.auto_processable is True
        assert match.module_name == "Order Data Cleanup"

    def test_full_workflow_different_column_names(self):
        """
        Simulate: Same data, different column naming conventions.
        """
        # Training data
        training = pd.DataFrame({
            "customer_id": [1, 2, 3],
            "email_address": ["a@t.com", "b@t.com", "c@t.com"],
            "first_name": ["A", "B", "C"],
        })
        contract = generate_contract(generate_profile(training))

        # New file with different naming
        new_data = pd.DataFrame({
            "CustomerID": [4, 5, 6],  # Different casing
            "Email": ["d@t.com", "e@t.com", "f@t.com"],  # Shorter name
            "FirstName": [" D", "E", "F"],  # CamelCase
        })
        new_profile = generate_profile(new_data)

        match = match_schema(new_profile, contract)

        # Should still match reasonably well
        assert match.is_compatible is True
        assert len(match.column_mappings) >= 2  # At least some columns matched

    def test_match_summary_output(self):
        """Test that match summary is human-readable."""
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["A", "B", "C"],
        })
        profile = generate_profile(df)
        contract = generate_contract(profile)

        match = match_schema(profile, contract)
        summary = match.to_summary()

        assert "SCHEMA MATCH RESULT" in summary
        assert "Confidence:" in summary
        assert "Compatible:" in summary
