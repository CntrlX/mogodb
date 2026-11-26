//! Server error type mirroring MongoDB's `{ok:0, code, codeName, errmsg}` shape.

use bson::{Document, doc};
use std::fmt;

#[derive(Debug, Clone)]
pub struct Error {
    pub code: i32,
    pub msg: String,
    /// Error labels such as `TransientTransactionError`.
    pub labels: Vec<&'static str>,
    /// Extra fields merged into the error reply (e.g. `keyPattern`, `keyValue`).
    pub extra: Option<Document>,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl Error {
    pub fn new(code: i32, msg: impl Into<String>) -> Self {
        Error { code, msg: msg.into(), labels: Vec::new(), extra: None }
    }
    pub fn with_label(mut self, l: &'static str) -> Self {
        self.labels.push(l);
        self
    }
    pub fn with_extra(mut self, d: Document) -> Self {
        self.extra = Some(d);
        self
    }
    /// code 1
    pub fn internal(msg: impl Into<String>) -> Self { Self::new(1, msg) }
    /// code 2
    pub fn bad_value(msg: impl Into<String>) -> Self { Self::new(2, msg) }
    /// code 9
    pub fn failed_to_parse(msg: impl Into<String>) -> Self { Self::new(9, msg) }
    /// code 14
    pub fn type_mismatch(msg: impl Into<String>) -> Self { Self::new(14, msg) }
    /// code 26
    pub fn ns_not_found(msg: impl Into<String>) -> Self { Self::new(26, msg) }
    /// code 59
    pub fn command_not_found(name: &str) -> Self {
        Self::new(59, format!("no such command: '{name}'"))
    }
    /// code 40324 etc: aggregation "Location<N>" errors.
    pub fn location(code: i32, msg: impl Into<String>) -> Self { Self::new(code, msg) }

    pub fn code_name(&self) -> String {
        code_name(self.code).map(str::to_string).unwrap_or_else(|| format!("Location{}", self.code))
    }

    pub fn to_doc(&self) -> Document {
        let mut d = doc! { "ok": 0.0, "errmsg": self.msg.clone(), "code": self.code, "codeName": self.code_name() };
        if !self.labels.is_empty() {
            d.insert("errorLabels", self.labels.iter().map(|s| bson::Bson::String(s.to_string())).collect::<Vec<_>>());
        }
        if let Some(x) = &self.extra {
            for (k, v) in x {
                d.insert(k.clone(), v.clone());
            }
        }
        d
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({}): {}", self.code_name(), self.code, self.msg)
    }
}

impl std::error::Error for Error {}

impl From<fjall::Error> for Error {
    fn from(e: fjall::Error) -> Self { Error::internal(format!("storage error: {e}")) }
}
impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self { Error::internal(format!("io error: {e}")) }
}
impl From<bson::ser::Error> for Error {
    fn from(e: bson::ser::Error) -> Self { Error::bad_value(format!("bson serialization: {e}")) }
}
impl From<bson::de::Error> for Error {
    fn from(e: bson::de::Error) -> Self { Error::bad_value(format!("invalid bson: {e}")) }
}
impl From<bson::raw::Error> for Error {
    fn from(e: bson::raw::Error) -> Self { Error::bad_value(format!("invalid bson: {e}")) }
}

/// Canonical MongoDB code names (subset actually emitted by this server).
pub fn code_name(code: i32) -> Option<&'static str> {
    Some(match code {
        1 => "InternalError",
        2 => "BadValue",
        4 => "NoSuchKey",
        6 => "HostUnreachable",
        8 => "UnknownError",
        9 => "FailedToParse",
        11 => "UserNotFound",
        13 => "Unauthorized",
        14 => "TypeMismatch",
        17 => "AuthenticationFailed",
        18 => "AuthenticationFailed",
        20 => "IllegalOperation",
        22 => "InvalidBSON",
        26 => "NamespaceNotFound",
        27 => "IndexNotFound",
        28 => "PathNotViable",
        40 => "ConflictingUpdateOperators",
        43 => "CursorNotFound",
        48 => "NamespaceExists",
        50 => "MaxTimeMSExpired",
        51 => "DuplicateKey",
        52 => "DollarPrefixedFieldName",
        53 => "InvalidIdField",
        54 => "NotSingleValueField",
        55 => "InvalidDBRef",
        56 => "EmptyFieldName",
        57 => "DottedFieldName",
        59 => "CommandNotFound",
        66 => "ImmutableField",
        67 => "CannotCreateIndex",
        68 => "IndexAlreadyLevel",
        72 => "InvalidOptions",
        73 => "InvalidNamespace",
        85 => "IndexOptionsConflict",
        86 => "IndexKeySpecsConflict",
        96 => "OperationFailed",
        112 => "WriteConflict",
        115 => "CommandNotSupported",
        121 => "DocumentValidationFailure",
        125 => "CommandFailed",
        136 => "CappedPositionLost",
        166 => "CommandNotSupportedOnView",
        197 => "InvalidIndexSpecificationOption",
        224 => "QueryFeatureNotAllowed",
        225 => "TransactionTooOld",
        236 => "CursorKilled",
        251 => "NoSuchTransaction",
        256 => "TransactionCommitted",
        263 => "OperationNotSupportedInTransaction",
        280 => "ChangeStreamFatalError",
        286 => "ChangeStreamHistoryLost",
        292 => "QueryExceededMemoryLimitNoDiskUseAllowed",
        11000 => "DuplicateKey",
        139 => "JSInterpreterFailure",
        31082 => "SearchNotEnabled",
        161 => "IncompatibleCollationVersion",
        146 => "ExceededMemoryLimit",
        167 => "OptionNotSupportedOnView",
        509 => "TooManyOpenTransactions",
        510 => "GeoKeyExtractionFailed",
        511 => "GeoKeyExtractionFailedTimeseries",
        11602 => "InterruptedDueToReplStateChange",
        40414 => "IDLFailedToParse",
        40415 => "IDLUnknownField",
        16 => "InvalidLength",
        23 => "AlreadyInitialized",
        31 => "RoleNotFound",
        76 => "NoReplicationEnabled",
        168 => "InvalidPipelineOperator",
        241 => "ConversionFailure",
        291 => "NoQueryExecutionPlans",
        352 => "UnsupportedOpQueryCommand",
        10334 => "BSONObjectTooLarge",
        11601 => "Interrupted",
        11600 => "InterruptedAtShutdown",
        _ => return None,
    })
}
