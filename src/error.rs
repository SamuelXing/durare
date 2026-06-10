use thiserror::Error;

/// The crate-wide error type.
///
/// Step closures and workflow functions return `Result<T>`; application errors
/// should use [`Error::app`].
#[derive(Debug, Error)]
pub enum Error {
    #[error("database error: {0}")]
    Db(#[from] sqlx::Error),

    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("no workflow registered under name `{0}`")]
    UnknownWorkflow(String),

    /// An error raised by user code inside a step or workflow.
    #[error("{0}")]
    App(String),
}

impl Error {
    /// Construct an application-level error from anything string-like.
    pub fn app(msg: impl Into<String>) -> Self {
        Error::App(msg.into())
    }
}

pub type Result<T> = std::result::Result<T, Error>;
