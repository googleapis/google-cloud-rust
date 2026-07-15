use crate::model::Job;
use google_cloud_lro::internal::DiscoveryOperation;

impl DiscoveryOperation for Job {
    fn done(&self) -> bool {
        self.status
            .as_ref()
            .map(|s| s.state == "DONE")
            .unwrap_or(false)
    }

    fn name(&self) -> Option<&String> {
        self.job_reference.as_ref().map(|r| &r.job_id)
    }

    fn error(&self) -> Option<google_cloud_gax::error::rpc::Status> {
        self.status
            .as_ref()
            .and_then(|s| s.error_result.as_ref())
            .map(|err| {
                let mut status = google_cloud_gax::error::rpc::Status::default();
                status.code = (google_cloud_gax::error::rpc::Code::Unknown as i32).into();
                status.message = err.message.clone();
                status
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ErrorProto, JobReference, JobStatus};

    #[test]
    fn test_done() {
        let mut job = Job::default();
        assert!(!job.done(), "missing status should not be done");

        job.status = Some(JobStatus {
            state: "RUNNING".to_string(),
            ..Default::default()
        });
        assert!(!job.done(), "RUNNING should not be done");

        job.status = Some(JobStatus {
            state: "DONE".to_string(),
            ..Default::default()
        });
        assert!(job.done(), "DONE should be done");
    }

    #[test]
    fn test_name() {
        let mut job = Job::default();
        assert_eq!(job.name(), None, "missing job_reference should yield None");

        job.job_reference = Some(JobReference {
            job_id: "my_job".to_string(),
            ..Default::default()
        });
        assert_eq!(job.name(), Some(&"my_job".to_string()), "should return job_id");
    }

    #[test]
    fn test_error() {
        let mut job = Job::default();
        assert!(job.error().is_none(), "missing status should yield no error");

        job.status = Some(JobStatus::default());
        assert!(job.error().is_none(), "missing error_result should yield no error");

        job.status = Some(JobStatus {
            error_result: Some(ErrorProto {
                message: "some error".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        });

        let err = job.error().expect("should have error");
        assert_eq!(err.message, "some error");
        // Unknown code is 2.
        assert_eq!(err.code, (google_cloud_gax::error::rpc::Code::Unknown as i32).into());
    }
}

