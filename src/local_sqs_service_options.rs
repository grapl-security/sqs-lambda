use std::time::Duration;

use crate::sqs_completion_handler::CompletionPolicy;

#[derive(Default)]
pub struct LocalSqsServiceOptionsBuilder {
    completion_policy: Option<CompletionPolicy>,
}
impl LocalSqsServiceOptionsBuilder {
    pub fn with_completion_policy(&mut self, arg: CompletionPolicy) -> &Self {
        self.completion_policy = Some(arg);
        self
    }

    pub fn with_minimal_buffer_completion_policy(&mut self) -> &Self {
        self.with_completion_policy(CompletionPolicy::new(
            1,                      // Buffer up to 1 message
            Duration::from_secs(1), // Buffer for up to 1 second
        ))
    }

    pub fn build(self) -> LocalSqsServiceOptions {
        LocalSqsServiceOptions {
            completion_policy: self.completion_policy.unwrap_or_else(|| {
                CompletionPolicy::new(
                    10,                     // Buffer up to 10 messages
                    Duration::from_secs(3), // Buffer for up to 3 seconds
                )
            }),
        }
    }
}

pub struct LocalSqsServiceOptions {
    pub completion_policy: CompletionPolicy,
}
