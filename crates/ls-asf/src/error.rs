use std::fmt;

#[derive(Debug, Clone)]
pub struct ServiceException {
    pub code: String,
    pub message: String,
    pub status_code: u16,
    pub sender_fault: bool,
}

impl fmt::Display for ServiceException {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for ServiceException {}

impl ServiceException {
    pub fn new(code: impl Into<String>, message: impl Into<String>, status_code: u16) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            status_code,
            sender_fault: false,
        }
    }

    pub fn with_sender_fault(mut self) -> Self {
        self.sender_fault = true;
        self
    }
}

pub type CommonServiceException = ServiceException;
