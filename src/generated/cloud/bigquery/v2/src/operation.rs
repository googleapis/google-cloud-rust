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
