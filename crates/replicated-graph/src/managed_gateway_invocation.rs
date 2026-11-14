//! Shared managed invocation ownership. Transport adapters must retain this
//! object until delivery completes or fails; dropping it cleans up new opens.
use super::*;

// An Open is not retained on cancellation, encode failure or a known send
// failure. Successful kernel write is not proof the peer received the descriptor;
// those unknowable lost replies are reclaimed by the ordinary TTL.
pub(super) struct OpenDelivery {
    pub(super) state: Arc<State>,
    pub(super) token: Option<[u8; 32]>,
    pub(super) local_token: Option<[u8; 32]>,
}
impl Drop for OpenDelivery {
    fn drop(&mut self) {
        if let Some(token) = self.local_token {
            self.state.local_sessions.lock().unwrap().remove(&token);
        }
        if let Some(token) = self.token {
            self.state.sessions.lock().unwrap().remove(&token);
        }
    }
}

pub(super) fn operation_end(operation: &Operation, end: Instant) -> Instant {
    if matches!(
        operation,
        Operation::StartLocalQueryJob { .. }
            | Operation::GetLocalQueryJob { .. }
            | Operation::RenewLocalQueryJob { .. }
            | Operation::FetchLocalQueryJob { .. }
            | Operation::CancelLocalQueryJob { .. }
    ) {
        end.min(Instant::now() + Duration::from_secs(5))
    } else if matches!(
        operation,
        Operation::Read { .. }
            | Operation::Traverse { .. }
            | Operation::TraversePage { .. }
            | Operation::OpenLocalQuery { .. }
            | Operation::OpenLocalQueryWithProfile { .. }
            | Operation::OpenLocalQueryAfter { .. }
            | Operation::FetchLocalQuery { .. }
    ) {
        end.min(Instant::now() + velagraph::GREMLIN_DEADLINE)
    } else {
        end
    }
}

pub(super) struct Invocation {
    pub(super) response: Response,
    delivery: OpenDelivery,
    local_retained: Option<Arc<local::Retained>>,
    job_retained: Option<Arc<jobs::Job>>,
    expiry: Option<Instant>,
    end: Instant,
    _work: Arc<OwnedSemaphorePermit>,
}
impl Invocation {
    pub(super) async fn begin(
        state: Arc<State>,
        request: Request,
        work: Arc<OwnedSemaphorePermit>,
        end: Instant,
    ) -> Result<Self> {
        let end = operation_end(&request.operation, end);
        if Instant::now() >= end {
            return Err(Error::Unknown);
        }
        let result = if request.version != VERSION || request.request_id == [0; 16] {
            Err(Error::Protocol)
        } else if request.binding != state.config.binding {
            Err(Error::Identity)
        } else {
            tokio::time::timeout_at(
                tokio::time::Instant::from_std(end),
                state.execute(request.operation, work.clone(), end),
            )
            .await
            .unwrap_or(Err(Error::Unknown))
        };
        let delivery = OpenDelivery {
            state: state.clone(),
            local_token: match &result {
                Ok(
                    Reply::OpenLocalQuery { session, .. }
                    | Reply::OpenLocalQueryWithProfile { session, .. },
                ) => Some(session.token),
                _ => None,
            },
            token: match &result {
                Ok(Reply::OpenSession(s)) => Some(s.token),
                _ => None,
            },
        };
        let response = Response {
            version: VERSION,
            request_id: request.request_id,
            binding: state.config.binding.clone(),
            result,
        };
        // Retain the result reservation through encoding and the complete send,
        // even when another request closes/expires the registry entry meanwhile.
        let local_retained = match &response.result {
            Ok(
                Reply::OpenLocalQuery { session, .. }
                | Reply::OpenLocalQueryWithProfile { session, .. }
                | Reply::FetchLocalQuery { session, .. },
            ) => Some(state.get_local(session)?),
            _ => None,
        };
        let job_retained = match &response.result {
            Ok(Reply::FetchLocalQueryJob { job, .. }) => Some(state.get_job(job)?),
            _ => None,
        };
        let expiry = match &response.result {
            Ok(
                Reply::OpenSession(s)
                | Reply::Read { session: s, .. }
                | Reply::Traverse { session: s, .. }
                | Reply::TraversePage { session: s, .. },
            ) => Some(state.get(s)?.expires),
            Ok(
                Reply::OpenLocalQuery { session, .. }
                | Reply::OpenLocalQueryWithProfile { session, .. }
                | Reply::FetchLocalQuery { session, .. },
            ) => Some(state.get_local(session)?.expires),
            Ok(Reply::RenewLocalQueryJob { job, .. }) => Some(state.get_job(job)?.deadline()?),
            _ => None,
        };

        let invocation = Self {
            response,
            delivery,
            local_retained,
            job_retained,
            expiry,
            end,
            _work: work,
        };
        invocation.live()?;
        Ok(invocation)
    }
    pub(super) fn live(&self) -> Result<()> {
        if self.expiry.is_some_and(|expiry| Instant::now() >= expiry)
            || self
                .local_retained
                .as_ref()
                .is_some_and(|retained| retained.closed.load(std::sync::atomic::Ordering::Acquire))
        {
            return Err(Error::SessionUnavailable);
        }
        if Instant::now() >= self.end {
            return Err(Error::Unknown);
        }
        if let Some(retained) = &self.local_retained {
            local::live(retained, self.end)?;
        }
        if let Some(job) = &self.job_retained {
            job.live_ready()?;
        }
        Ok(())
    }
    pub(super) fn delivery_end(&self) -> Result<Instant> {
        let end = self.expiry.map_or(self.end, |expiry| self.end.min(expiry));
        Ok(if let Some(job) = &self.job_retained {
            end.min(job.deadline()?)
        } else {
            end
        })
    }
    pub(super) fn preflight_http(&self) -> Result<()> {
        if let (Some(retained), Ok(Reply::OpenLocalQuery { .. } | Reply::FetchLocalQuery { .. })) =
            (&self.local_retained, &self.response.result)
        {
            retained.preflight_http(self.end)?;
        }
        self.live()
    }
    pub(super) fn preflight_grpc(&self) -> Result<()> {
        if let (Some(retained), Ok(Reply::OpenLocalQuery { .. } | Reply::OpenLocalQueryWithProfile { .. } | Reply::FetchLocalQuery { .. })) =
            (&self.local_retained, &self.response.result)
        {
            retained.preflight_grpc(&self.response.binding, self.end)?;
        }
        self.live()
    }
    pub(super) fn delivered(&mut self) {
        if self.response.result.is_ok() {
            self.delivery.token = None;
            self.delivery.local_token = None;
        }
    }
}
