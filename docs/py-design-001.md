# Python sys.monitoring Tracer Design

## Overview

This document outlines the design for integrating Python's `sys.monitoring` API with the `runtime_tracing` format. The goal is to produce CodeTracer-compatible traces for Python programs without modifying the interpreter.

The tracer collects `sys.monitoring` events, converts them to `runtime_tracing` events, and streams them to `trace.json`/`trace.bin` along with metadata and source snapshots.

## Architecture

### Tool Initialization
- Acquire a tool identifier via `sys.monitoring.use_tool_id`; store it for the lifetime of the tracer.
- Register one callback per event using `sys.monitoring.register_callback`.
- Enable all desired events by bitmask with `sys.monitoring.set_events`.

### Writer Management
- Open a `runtime_tracing` writer (`trace.json` or `trace.bin`) during `start_tracing`.
- Expose methods to append metadata and file copies using existing `runtime_tracing` helpers.
- Flush and close the writer when tracing stops.

### Frame and Thread Tracking
- Maintain a per-thread stack of frame identifiers to correlate `CALL`, `PY_START`, and returns.
- Map `frame` objects to internal IDs for cross-referencing events.
- Record thread start/end events when a new thread registers callbacks.

## Event Handling

Each bullet below represents a low-level operation translating a single `sys.monitoring` event into the `runtime_tracing` stream.

### Control Flow
- **PY_START** – Create a `Function` event for the code object and push a new frame ID onto the thread's stack.
- **PY_RESUME** – Emit an `Event` log noting resumption and update the current frame's state.
- **PY_RETURN** – Pop the frame ID, write a `Return` event with the value (if retrievable), and link to the caller.
- **PY_YIELD** – Record a `Return` event flagged as a yield and keep the frame on the stack for later resumes.
- **STOP_ITERATION** – Emit an `Event` indicating iteration exhaustion for the current frame.
- **PY_UNWIND** – Mark the beginning of stack unwinding and note the target handler in an `Event`.
- **PY_THROW** – Emit an `Event` describing the thrown value and the target generator/coroutine.
- **RERAISE** – Log a re-raise event referencing the original exception.

### Call and Line Tracking
- **CALL** – Record a `Call` event, capturing argument values and the callee's `Function` ID.
- **LINE** – Write a `Step` event with current path and line number; ensure the path is registered.
- **INSTRUCTION** – Optionally emit a fine-grained `Event` containing the opcode name for detailed traces.
- **JUMP** – Append an `Event` describing the jump target offset for control-flow visualization.
- **BRANCH** – Record an `Event` with branch outcome (taken or not) to aid coverage analysis.

### Exception Lifecycle
- **RAISE** – Emit an `Event` containing exception type and message when raised.
- **EXCEPTION_HANDLED** – Log an `Event` marking when an exception is caught.

### C API Boundary
- **C_RETURN** – On returning from a C function, emit a `Return` event tagged as foreign and include result summary.
- **C_RAISE** – When a C function raises, record an `Event` with the exception info and current frame ID.

### No Events
- **NO_EVENTS** – Special constant; used only to disable monitoring. No runtime event is produced.

## Metadata and File Capture
- Collect the working directory, program name, and arguments and store them in `trace_metadata.json`.
- Track every file path referenced; copy each into the trace directory under `files/`.
- Record `VariableName`, `Type`, and `Value` entries when variables are inspected or logged.

## Shutdown
- On `stop_tracing`, call `sys.monitoring.set_events` with `NO_EVENTS` for the tool ID.
- Unregister callbacks and free the tool ID with `sys.monitoring.free_tool_id`.
- Close the writer and ensure all buffered events are flushed to disk.

## Current Limitations
- **No structured support for threads or async tasks** – the trace format lacks explicit identifiers for concurrent execution.
  Distinguishing events emitted by different Python threads or `asyncio` tasks requires ad hoc `Event` entries, complicating
  analysis and preventing downstream tools from reasoning about scheduling.
- **Generic `Event` log** – several `sys.monitoring` notifications like resume, unwind, and branch outcomes have no dedicated
  `runtime_tracing` variant. They must be encoded as free‑form `Event` logs, which reduces machine readability and hinders
  automation.
- **Heavy value snapshots** – arguments and returns expect full `ValueRecord` structures. Serializing arbitrary Python objects is
  expensive and often degrades to lossy string dumps, limiting the visibility of rich runtime state.
- **Append‑only path and function tables** – `runtime_tracing` assumes files and functions are discovered once and never change.
  Dynamically generated code (`eval`, REPL snippets) forces extra bookkeeping and cannot update earlier entries, making
  dynamic features awkward to trace.
- **No built‑in compression or streaming** – traces are written as monolithic JSON or binary files. Long sessions quickly grow in
  size and cannot be streamed to remote consumers without additional tooling.

## Future Extensions
- Add filtering to enable subsets of events for performance-sensitive scenarios.
- Support streaming traces over a socket for live debugging.
