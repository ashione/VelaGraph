//! Bounded sequential Metadata exchange. Each message performs the original
//! local applied-epoch check; the response stream never caches Metadata results.
use super::{
    grpc,
    operations_grpc::{bounded::METADATA_MESSAGE_BYTES, codec, wire},
    Operation, RemoteError, RemoteIdentity, Reply, MAX_DEADLINE,
};
use std::{
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    sync::{mpsc, OwnedSemaphorePermit, Semaphore},
    task::JoinHandle,
    time::{timeout_at, Instant},
};
use tokio_stream::Stream;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status, Streaming};
use wire::metadata_service_server::{MetadataService, MetadataServiceServer};

// Queue includes one response; the sequential actor owns at most one additional
// decoded request. Stream codec buffers yield by 1024 B (+one <=512 B message).
// Pinned Hyper 1.12 caps queued HTTP/2 send bytes per stream at 400 KiB,
// plus one body chunk. Its stream and connection receive windows default to
// 1 MiB, not the base HTTP/2 65535 B default; max DATA frame is 16384 B. These
// transport buffers are additional to this queue, not counted as zero memory.
const OUTPUT_CAPACITY: usize = 1;
#[derive(Clone)]
pub(in crate::remote) struct Handler {
    handler: grpc::Handler,
    operations: Arc<Semaphore>,
    streams: Arc<Semaphore>,
    deadline: Duration,
    stop: CancellationToken,
}
pub(in crate::remote) fn service(
    handler: grpc::Handler,
    operations: Arc<Semaphore>,
    streams: Arc<Semaphore>,
    deadline: Duration,
    stop: CancellationToken,
) -> MetadataServiceServer<Handler> {
    MetadataServiceServer::new(Handler {
        handler,
        operations,
        streams,
        deadline: deadline.min(MAX_DEADLINE),
        stop,
    })
    .max_decoding_message_size(METADATA_MESSAGE_BYTES)
    .max_encoding_message_size(METADATA_MESSAGE_BYTES)
}
struct Envelope {
    result: Result<wire::MetadataResponse, Status>,
    end: Instant,
}
pub(in crate::remote) struct Output {
    receiver: mpsc::Receiver<Envelope>,
    _permit: OwnedSemaphorePermit,
    cancel: CancellationToken,
    terminal: Arc<Mutex<Option<Status>>>,
    finished: bool,
    task: JoinHandle<()>,
}
impl Stream for Output {
    type Item = Result<wire::MetadataResponse, Status>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.finished {
            return Poll::Ready(None);
        }
        match self.receiver.poll_recv(cx) {
            Poll::Ready(Some(item)) => {
                if Instant::now() >= item.end {
                    self.finished = true;
                    self.cancel.cancel();
                    Poll::Ready(Some(Err(Status::deadline_exceeded(
                        "metadata response deadline",
                    ))))
                } else {
                    Poll::Ready(Some(item.result))
                }
            }
            Poll::Ready(None) => {
                self.finished = true;
                let terminal = self
                    .terminal
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .take();
                Poll::Ready(terminal.map(Err))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}
impl Drop for Output {
    fn drop(&mut self) {
        self.cancel.cancel();
        self.task.abort();
    }
}
#[tonic::async_trait]
impl MetadataService for Handler {
    type ExchangeStream = Output;
    async fn exchange(
        &self,
        request: Request<Streaming<wire::MetadataRequest>>,
    ) -> Result<Response<Output>, Status> {
        let permit = self
            .streams
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("metadata stream capacity"))?;
        let cancel = self.stop.child_token();
        let (sender, receiver) = mpsc::channel(OUTPUT_CAPACITY);
        let handler = self.clone();
        let stop = cancel.clone();
        let terminal = Arc::new(Mutex::new(None));
        let task_terminal = terminal.clone();
        let task = tokio::spawn(async move {
            if let Err(error) = handler.run(request.into_inner(), &sender, stop).await {
                *task_terminal.lock().unwrap_or_else(|e| e.into_inner()) = Some(error);
            }
            // Keep sender alive until terminal status is visible to the stream.
            drop(sender);
        });
        Ok(Response::new(Output {
            receiver,
            _permit: permit,
            cancel,
            terminal,
            finished: false,
            task,
        }))
    }
}
impl Handler {
    async fn run(
        &self,
        mut input: Streaming<wire::MetadataRequest>,
        output: &mpsc::Sender<Envelope>,
        stop: CancellationToken,
    ) -> Result<(), Status> {
        let mut last = 0;
        loop {
            // Includes idle and partial-frame time. Stop only this RPC, never
            // abort a multiplexed HTTP/2 connection. A non-reading peer may hold
            // the independent stream slot until cancel/read/disconnect/shutdown.
            let idle_end = Instant::now() + self.deadline;
            let request = tokio::select! {
                _=stop.cancelled()=>break,
                read=timeout_at(idle_end,input.message())=>match read {
                    Ok(Ok(Some(request)))=>request,
                    Ok(Ok(None))=>break,
                    Ok(Err(error))=>return Err(error),
                    Err(_)=>return Err(Status::deadline_exceeded("metadata message deadline")),
                }
            };
            let received = Instant::now();
            if request.id == 0 || request.id <= last {
                return Err(Status::invalid_argument("metadata id must increase"));
            }
            last = request.id;
            let valid_budget = request.timeout_millis > 0 && request.timeout_millis <= 60_000;
            let end = received
                + if valid_budget {
                    self.deadline
                        .min(Duration::from_millis(request.timeout_millis))
                } else {
                    self.deadline
                };
            // Reserve output before admission/execution: a slow reader cannot
            // accumulate operations or completed results behind an unbounded queue.
            let reserved = tokio::select! {
                _=stop.cancelled()=>break,
                slot=timeout_at(end,output.reserve())=>match slot {
                    Ok(Ok(slot))=>slot,
                    Ok(Err(_))=>break,
                    Err(_)=>return Err(Status::deadline_exceeded("metadata message deadline")),
                }
            };
            let result = if request.version != 1 {
                Err(RemoteError::Protocol)
            } else if !valid_budget || request.identity.is_none() {
                Err(RemoteError::InvalidArgument)
            } else {
                match self.operations.clone().try_acquire_owned() {
                    Err(_) => Err(RemoteError::ResourceExhausted),
                    Ok(_operation) => {
                        let id = request.identity.unwrap();
                        let context = grpc::wire::Context {
                            version: 1,
                            identity: Some(
                                RemoteIdentity {
                                    group: id.group,
                                    node: id.node,
                                }
                                .into(),
                            ),
                            timeout_millis: request.timeout_millis,
                        };
                        tokio::select! {
                            _=stop.cancelled()=>break,
                            result=self.handler.execute_until(Some(context),Operation::Metadata{shard:request.shard},end)=>result.map(|(reply,_)|reply),
                        }
                    }
                }
            };
            if Instant::now() >= end {
                return Err(Status::deadline_exceeded("metadata execution deadline"));
            }
            let result = match result {
                Ok(Reply::Metadata(shard, epoch, sequence)) => {
                    wire::metadata_response::Result::Metadata(wire::Metadata {
                        shard,
                        epoch,
                        sequence,
                    })
                }
                Ok(_) => {
                    wire::metadata_response::Result::Error(codec::failure(RemoteError::Protocol))
                }
                Err(error) => wire::metadata_response::Result::Error(codec::failure(error)),
            };
            reserved.send(Envelope {
                result: Ok(wire::MetadataResponse {
                    version: 1,
                    id: request.id,
                    result: Some(result),
                }),
                end,
            });
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "metadata_stream_server_tests.rs"]
mod tests;
