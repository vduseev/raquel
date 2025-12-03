"""Tests for serialization failure detection."""

from unittest.mock import Mock
from sqlalchemy.exc import OperationalError, DBAPIError, IntegrityError

from raquel.core.common import is_serialization_failure


def test_cockroachdb_serialization_failure_restart_transaction():
    """Test detection of CockroachDB 'restart transaction' error."""
    error = OperationalError(
        "statement", 
        "params", 
        "restart transaction: TransactionRetryWithProtoRefreshError: TransactionAbortedError"
    )
    assert is_serialization_failure(error) is True


def test_cockroachdb_serialization_failure_with_details():
    """Test detection of CockroachDB error with full details."""
    error_msg = (
        'restart transaction: TransactionRetryWithProtoRefreshError: '
        'TransactionAbortedError(ABORT_REASON_CLIENT_REJECT): "sql txn" '
        'meta={id=ca9f5d8b key=/Table/117/1/0 pri=0.01747562}'
    )
    error = OperationalError("statement", "params", error_msg)
    assert is_serialization_failure(error) is True


def test_cockroachdb_serialization_failure_keyword():
    """Test detection of CockroachDB 'SerializationFailure' error."""
    error = DBAPIError(
        "statement",
        "params",
        "SerializationFailure: could not commit transaction"
    )
    assert is_serialization_failure(error) is True


def test_cockroachdb_serialization_failure_lowercase():
    """Test detection with lowercase error message."""
    error = OperationalError(
        "statement",
        "params",
        "serializationfailure: transaction aborted"
    )
    assert is_serialization_failure(error) is True


def test_postgres_serialization_failure_could_not_serialize():
    """Test detection of PostgreSQL 'could not serialize access' error."""
    error = OperationalError(
        "statement",
        "params",
        "could not serialize access due to concurrent update"
    )
    assert is_serialization_failure(error) is True


def test_postgres_serialization_failure_read_write_dependencies():
    """Test detection of PostgreSQL serialization error with dependencies."""
    error = OperationalError(
        "statement",
        "params",
        "could not serialize access due to read/write dependencies among transactions"
    )
    assert is_serialization_failure(error) is True


def test_serialization_failure_via_orig_attribute():
    """Test detection when error message is in orig attribute (psycopg2 style)."""
    error = OperationalError("statement", "params", "wrapper error")
    
    # Mock the orig attribute
    orig_error = Mock()
    orig_error.__str__ = Mock(return_value="restart transaction: retry error")
    error.orig = orig_error
    
    assert is_serialization_failure(error) is True


def test_serialization_failure_via_orig_cockroach():
    """Test CockroachDB error detection via orig attribute."""
    error = DBAPIError("statement", "params", "outer error")
    
    orig_error = Mock()
    orig_error.__str__ = Mock(return_value="SerializationFailure: ABORT_REASON_CLIENT_REJECT")
    error.orig = orig_error
    
    assert is_serialization_failure(error) is True


def test_serialization_failure_via_orig_postgres():
    """Test PostgreSQL error detection via orig attribute."""
    error = DBAPIError("statement", "params", "outer error")
    
    orig_error = Mock()
    orig_error.__str__ = Mock(return_value="could not serialize access")
    error.orig = orig_error
    
    assert is_serialization_failure(error) is True


def test_non_serialization_operational_error():
    """Test that other OperationalErrors are not detected as serialization failures."""
    error = OperationalError(
        "statement",
        "params",
        "connection refused"
    )
    assert is_serialization_failure(error) is False


def test_non_serialization_dbapi_error():
    """Test that other DBAPIErrors are not detected as serialization failures."""
    error = DBAPIError(
        "statement",
        "params",
        "invalid SQL syntax"
    )
    assert is_serialization_failure(error) is False


def test_connection_timeout_not_serialization():
    """Test that connection timeout errors are not serialization failures."""
    error = OperationalError(
        "statement",
        "params",
        "connection timeout"
    )
    assert is_serialization_failure(error) is False


def test_integrity_error_not_serialization():
    """Test that IntegrityError is not a serialization failure."""
    error = IntegrityError(
        "statement",
        "params",
        "unique constraint violation"
    )
    assert is_serialization_failure(error) is False


def test_generic_exception_not_serialization():
    """Test that generic exceptions are not serialization failures."""
    error = Exception("some random error")
    assert is_serialization_failure(error) is False


def test_value_error_not_serialization():
    """Test that ValueError is not a serialization failure."""
    error = ValueError("invalid value")
    assert is_serialization_failure(error) is False


def test_none_orig_attribute():
    """Test handling when orig attribute is None."""
    error = OperationalError("statement", "params", "some error")
    error.orig = None
    assert is_serialization_failure(error) is False


def test_mixed_case_restart_transaction():
    """Test detection with mixed case 'Restart Transaction'."""
    error = OperationalError(
        "statement",
        "params",
        "Restart Transaction: some error details"
    )
    assert is_serialization_failure(error) is True


def test_mixed_case_serialization_failure():
    """Test detection with mixed case 'SerializationFailure'."""
    error = DBAPIError(
        "statement",
        "params",
        "SerializationFailure: error details"
    )
    assert is_serialization_failure(error) is True


def test_uppercase_could_not_serialize():
    """Test detection with uppercase 'COULD NOT SERIALIZE ACCESS'."""
    error = OperationalError(
        "statement",
        "params",
        "COULD NOT SERIALIZE ACCESS DUE TO CONCURRENT UPDATE"
    )
    assert is_serialization_failure(error) is True
