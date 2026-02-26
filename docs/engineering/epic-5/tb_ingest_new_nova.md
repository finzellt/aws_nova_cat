| State                            |        Type | Lambda family    | `op`                                   | Input model                                     | Output model |
| -------------------------------- | ----------: | ---------------- | -------------------------------------- | ----------------------------------------------- | ------------ |
| ValidateInput                    |        Pass | —                | —                                      | IngestNewNovaEvent (validated at first Task)    | —            |
| EnsureCorrelationId              | Choice/Pass | —                | —                                      | —                                               | —            |
| BeginJobRun                      |        Task | governance_task  | `begin_jobrun`                         | TaskInvocation(payload=IngestNewNovaEvent dict) | TaskResult   |
| AcquireIdempotencyLock           |        Task | idempotency_task | `acquire_lock`                         | TaskInvocation                                  | TaskResult   |
| LaunchDownstream                 |    Parallel | —                | —                                      | —                                               | —            |
| ├─ LaunchRefreshReferences       |        Task | dispatch_task    | `publish_event` *(EPIC5_STUB allowed)* | TaskInvocation → RefreshReferencesEvent         | TaskResult   |
| └─ LaunchDiscoverSpectraProducts |        Task | dispatch_task    | `publish_event`                        | TaskInvocation → DiscoverSpectraProductsEvent   | TaskResult   |
| SummarizeLaunch                  |        Task | dispatch_task    | `summarize_launch`                     | TaskInvocation                                  | TaskResult   |
| FinalizeJobRunSuccess            |        Task | governance_task  | `finalize_success`                     | TaskInvocation                                  | TaskResult   |
| TerminalFailHandler              |        Task | governance_task  | `terminal_fail`                        | TaskInvocation                                  | TaskResult   |
| FinalizeJobRunFailed             |        Task | governance_task  | `finalize_failed`                      | TaskInvocation                                  | TaskResult   |
