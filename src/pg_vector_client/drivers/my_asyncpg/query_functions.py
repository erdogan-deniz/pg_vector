""""""

import structlog

from typing import Any
from structlog import BoundLogger
from asyncpg.connection import Connection
from asyncpg import (
    DataError,
    GroupingError,
    InterfaceError,
    SQLRoutineError,
    InvalidNameError,
    TooManyRowsError,
    CannotCoerceError,
    AdminShutdownError,
    TooManyColumnsError,
    DuplicateTableError,
    InternalServerError,
    CheckViolationError,
    UndefinedColumnError,
    DuplicateObjectError,
    UniqueViolationError,
    DatatypeMismatchError,
    NotNullViolationError,
    CollationMismatchError,
    DuplicateDatabaseError,
    RestrictViolationError,
    IdleSessionTimeoutError,
    ForeignKeyViolationError,
    NullValueNotAllowedError,
    CardinalityViolationError,
    ActiveSQLTransactionError,
    InvalidDatetimeFormatError,
    InsufficientPrivilegeError,
    NumericValueOutOfRangeError,
    IndeterminateCollationError,
    InvalidColumnReferenceError,
    StringDataLengthMismatchError,
    InvalidRegularExpressionError,
    ZeroLengthCharacterStringError,
    InvalidTextRepresentationError,
    DependentObjectsStillExistError,
    InvalidCharacterValueForCastError,
    IntegrityConstraintViolationError,
    InvalidTransactionInitiationError,

    PostgresError,
    PostgresSyntaxError,
    PostgresSystemError,
    UnknownPostgresError,
)

logger: BoundLogger = structlog.get_logger(__name__)


async def execute_query(
    conn: Connection,
    sql_query: str
) -> Any | None:
    """"""

    try:
        res: Any = await conn.execute(sql_query)
    except (
        DataError,
        DatatypeMismatchError,
        InvalidDatetimeFormatError,
    ) as data_err:
        await logger.aerror(
            f"\nDataError: {str(data_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {execute_query.__name__}.\n"
        )
    except GroupingError as group_err:
        await logger.aerror(
            f"\nGroupingError: {str(group_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {execute_query.__name__}.\n"
        )
    except InterfaceError as interf_err:
        await logger.aerror(
            f"\nInterfaceError: {str(interf_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {execute_query.__name__}.\n"
        )
    except SQLRoutineError as sql_rout_err:
        await logger.aerror(
            f"\nSQLRoutineError: {str(sql_rout_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {execute_query.__name__}.\n"
        )
    except (
        InvalidNameError,
        UndefinedColumnError,
    ) as inv_name_err:
        await logger.aerror(
            f"\nInvalidNameError: {str(inv_name_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {execute_query.__name__}.\n"
        )
    except (
        TooManyRowsError,
        TooManyColumnsError,
    ) as many_elems_err:
        await logger.aerror(
            f"\nManyElemsError: {str(many_elems_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {execute_query.__name__}.\n"
        )
    except (
        CannotCoerceError,
        InvalidTextRepresentationError,
        InvalidCharacterValueForCastError,
    ) as conver_err:
        await logger.aerror(
            f"\nConversion Error: {str(conver_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {execute_query.__name__}.\n"
        )
    except AdminShutdownError as adm_shutd_err:
        await logger.aerror(
            f"\nAdminShutdownError: {str(adm_shutd_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {execute_query.__name__}.\n"
        )
    except (
        DuplicateTableError,
        DuplicateObjectError,
        DuplicateDatabaseError,
    ) as dublic_err:
        await logger.aerror(
            f"\nDublicateError: {str(dublic_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {execute_query.__name__}.\n"
        )
    except InternalServerError as intern_err:
        await logger.aerror(
            f"\nInternalServerError: {str(intern_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {execute_query.__name__}.\n"
        )
    except (
        CheckViolationError,
        UniqueViolationError,
        NotNullViolationError,
        RestrictViolationError,
        ForeignKeyViolationError,
        CardinalityViolationError,
        IntegrityConstraintViolationError,
    ) as limitation_err:
        await logger.aerror(
            f"\nLimitationError: {str(limitation_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {execute_query.__name__}.\n"
        )
    except (
        InvalidNameError,
        UndefinedColumnError,
    ) as n_found_err:
        await logger.aerror(
            f"\nNotFoundError: {str(n_found_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {execute_query.__name__}.\n"
        )
    except (
        CollationMismatchError,
        IndeterminateCollationError,
    ) as sort_err:
        await logger.aerror(
            f"\nSortingError: {str(sort_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {execute_query.__name__}.\n"
        )
    except IdleSessionTimeoutError as idle_sess_err:
        await logger.aerror(
            f"\nIdleSessionTimeoutError: {str(idle_sess_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {execute_query.__name__}.\n"
        )
    except NullValueNotAllowedError as null_val__allow_err:
        await logger.aerror(
            "\nNullValueNotAllowedError: "
            f"{str(null_val__allow_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {execute_query.__name__}.\n"
        )
    except (
        ActiveSQLTransactionError,
        InvalidTransactionInitiationError,
    ) as trans_err:
        await logger.aerror(
            f"\nTransactionError: {str(trans_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {execute_query.__name__}.\n"
        )
    except InsufficientPrivilegeError as rules_err:
        await logger.aerror(
            f"\nInsufficientPrivilegeError: {str(rules_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {execute_query.__name__}.\n"
        )
    except (
        NumericValueOutOfRangeError,
        StringDataLengthMismatchError,
        ZeroLengthCharacterStringError,
    ) as size_err:
        await logger.aerror(
            f"\nOutOfRangeError: {str(size_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {execute_query.__name__}.\n"
        )
    except InvalidColumnReferenceError as inv_col_ref_err:
        await logger.aerror(
            "\nInvalidColumnReferenceError: "
            f"{str(inv_col_ref_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {execute_query.__name__}.\n"
        )
    except InvalidRegularExpressionError as inv_reg_express_err:
        await logger.aerror(
            "\nInvalidRegularExpressionError: "
            f"{str(inv_reg_express_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {execute_query.__name__}.\n"
        )
    except DependentObjectsStillExistError as dep_obj_still_exist_err:
        await logger.aerror(
            "\nDependentObjectsStillExistError: "
            f"{str(dep_obj_still_exist_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {execute_query.__name__}.\n"
        )
    except (
        PostgresError,
        PostgresSyntaxError,
        PostgresSystemError,
        UnknownPostgresError,
    ) as postgr_err:
        await logger.aerror(
            f"\nPostgresError: {str(postgr_err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {execute_query.__name__}.\n"
        )
    except Exception as err:
        await logger.awarning(
            "\nException: "
            f"{str(err).capitalize()}.\n"
            f"File name is: {__file__}.\n"
            f"Function name is: {execute_query.__name__}.\n"
        )
    else:
        await logger.ainfo(f"The query '{sql_query}' was completed.\n")

        return res
