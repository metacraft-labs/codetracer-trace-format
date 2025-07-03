# CodeTracer Trace JSON Format

This document describes the JSON files produced by the `runtime_tracing` library. These files store recorded program execution data for the CodeTracer omniscient debugger.

## Files in a Trace

A trace directory typically contains the following entries:

* `trace.json` – array of program events forming the execution trace.
* `trace_metadata.json` – metadata about the recorded program.
* `trace_paths.json` – list of file paths referenced by the trace.
* `files/` – copies of the program source files for offline debugging.

Each file is encoded in UTF‑8 and uses pretty standard JSON produced by [Serde](https://serde.rs/). The structures below correspond to Rust types from `src/types.rs`.

## Trace Metadata

The file `trace_metadata.json` is a single JSON object with the following fields:

```json
{
  "workdir": "path to the working directory",
  "program": "name of the traced program",
  "args": ["list", "of", "command", "line", "arguments"]
}
```

`workdir` and `program` are strings. `args` is an array of strings representing the arguments supplied to the program when tracing started.

## Path List

`trace_paths.json` contains an array of strings. Each element is a path that was referenced in the trace. Paths are stored in the order they were discovered so that other events can refer to them by numeric identifier.

Example:

```json
["/path/to/main.rs", "/path/to/lib.rs"]
```

The index of a path within this array is used as the `PathId` elsewhere in the event stream.

## Event Stream (`trace.json`)

`trace.json` is an array. Each element represents one `TraceLowLevelEvent` value serialized as a JSON object. The outer object contains a single key naming the event variant. The value associated with the key holds the fields specific to that variant.

Example (simplified):

```json
[
  {"Path": "/path/to/main.rs"},
  {"Function": {"path_id": 0, "line": 1, "name": "main"}},
  {"Step": {"path_id": 0, "line": 1}},
  {"Call": {"function_id": 0, "args": []}},
  {"Return": {"return_value": {"kind": "None", "type_id": 0}}}
]
```

The recognized event variants and their payloads are listed below. Integer wrapper types such as `PathId`, `StepId`, `VariableId`, `FunctionId`, `Line`, and `Place` are encoded simply as numbers.

### `Path`
```json
{"Path": "absolute/or/relative/path"}
```
Registers a new file path. The numeric identifier of the path is its position within `trace_paths.json`.

### `VariableName`
```json
{"VariableName": "name"}
```
Introduces a variable name and assigns it a `VariableId` based on the order of appearance.

### `Type`
```json
{"Type": {
  "kind": <numeric TypeKind>,
  "lang_type": "language specific name",
  "specific_info": {
    "kind": "None" | "Struct" | "Pointer",
    ...
  }
}}
```
Describes a new type. `TypeKind` values are encoded as numbers. When `specific_info.kind` is `Struct`, the object also contains `fields` which is an array of `{ "name": String, "type_id": TypeId }`. When `Pointer`, it contains `dereference_type_id`.

### `Value`
```json
{"Value": {"variable_id": <id>, "value": <ValueRecord>}}
```
Stores the full value of a variable. `ValueRecord` objects use the representation `{"kind": "Variant", ...}` as shown below in the **Value Records** section.

### `Function`
```json
{"Function": {"path_id": <id>, "line": <line>, "name": "function name"}}
```
Registers a function so that subsequent `Call` events can reference it.

### `Step`
```json
{"Step": {"path_id": <id>, "line": <line>}}
```
Marks execution of a particular line in a file.

### `Call`
```json
{"Call": {"function_id": <id>, "args": [<FullValueRecord>, ...]}}
```
Signals the start of a function call. Each argument is represented as a `FullValueRecord` (the same structure used by `Value`).

### `Return`
```json
{"Return": {"return_value": <ValueRecord>}}
```
Signals function return and provides the return value.

### `Event`
```json
{"Event": {"kind": <numeric EventLogKind>, "metadata": "", "content": "text"}}
```
A general‑purpose log entry. `EventLogKind` is encoded as a number. `metadata` is currently a free‑form string and may be empty.

### `Asm`
```json
{"Asm": ["instruction", ...]}
```
Embeds raw assembly or bytecode instructions relevant to the step.

### `BindVariable`
```json
{"BindVariable": {"variable_id": <id>, "place": <place>}}
```
Associates a variable with an opaque `Place` identifier. Places can be used to track mutations of complex values.

### `Assignment`
```json
{"Assignment": {"to": <variable_id>, "pass_by": "Value" | "Reference", "from": <RValue>}}
```
Records a by‑value or by‑reference assignment. `RValue` is described below.

### `DropVariables`
```json
{"DropVariables": [<variable_id>, ...]}
```
Signals that a set of variables went out of scope.

### `CompoundValue`
```json
{"CompoundValue": {"place": <place>, "value": <ValueRecord>}}
```
Defines a value located at a `Place` that consists of multiple parts (for example, the elements of a collection).

### `CellValue`
```json
{"CellValue": {"place": <place>, "value": <ValueRecord>}}
```
Stores the current value of a mutable cell located at a `Place`.

### `AssignCompoundItem`
```json
{"AssignCompoundItem": {"place": <place>, "index": <number>, "item_place": <place>}}
```
Connects an index within a compound value to a new `Place` containing the item.

### `AssignCell`
```json
{"AssignCell": {"place": <place>, "new_value": <ValueRecord>}}
```
Updates the value stored at a `Place`.

### `VariableCell`
```json
{"VariableCell": {"variable_id": <id>, "place": <place>}}
```
Binds a variable directly to a `Place`.

### `DropVariable`
```json
{"DropVariable": <variable_id>}
```
Removes the association of a variable with any value.

### `DropLastStep`
```json
{"DropLastStep": null}
```
A special marker used when a previously emitted `Step` should be ignored. It keeps the trace append‑only.

## Value Records

Many events embed `ValueRecord` objects. They all use an internally tagged representation with a `kind` field. The possible variants are:

* `Int` – `{ "kind": "Int", "i": number, "type_id": TypeId }`
* `Float` – `{ "kind": "Float", "f": number, "type_id": TypeId }`
* `Bool` – `{ "kind": "Bool", "b": true|false, "type_id": TypeId }`
* `String` – `{ "kind": "String", "text": "...", "type_id": TypeId }`
* `Sequence` – `{ "kind": "Sequence", "elements": [<ValueRecord>], "is_slice": bool, "type_id": TypeId }`
* `Tuple` – `{ "kind": "Tuple", "elements": [<ValueRecord>], "type_id": TypeId }`
* `Struct` – `{ "kind": "Struct", "field_values": [<ValueRecord>], "type_id": TypeId }`
* `Variant` – `{ "kind": "Variant", "discriminator": "name", "contents": <ValueRecord>, "type_id": TypeId }`
* `Reference` – `{ "kind": "Reference", "dereferenced": <ValueRecord>, "address": number, "mutable": bool, "type_id": TypeId }`
* `Raw` – `{ "kind": "Raw", "r": "text", "type_id": TypeId }`
* `Error` – `{ "kind": "Error", "msg": "description", "type_id": TypeId }`
* `None` – `{ "kind": "None", "type_id": TypeId }`
* `Cell` – `{ "kind": "Cell", "place": <place> }`

## RValue

`RValue` is used inside `Assignment` events to describe the right‑hand side of an assignment.

* `{"kind": "Simple", "0": <variable_id>}` – reference to a single variable.
* `{"kind": "Compound", "0": [<variable_id>, ...]}` – a composite value built from several variables.

## Numeric Enumerations

`TypeKind` and `EventLogKind` are serialized as numbers. Their numeric values correspond to the order of variants defined in `src/types.rs`.

Example: the default `TypeKind::Seq` has value `0`, `TypeKind::Set` has value `1`, and so on. Consumers should be prepared to handle unknown values gracefully as the enumeration may evolve.

### `TypeKind` values

| Value | Variant |
| -----:| ------- |
| 0 | Seq |
| 1 | Set |
| 2 | HashSet |
| 3 | OrderedSet |
| 4 | Array |
| 5 | Varargs |
| 6 | Struct |
| 7 | Int |
| 8 | Float |
| 9 | String |
| 10 | CString |
| 11 | Char |
| 12 | Bool |
| 13 | Literal |
| 14 | Ref |
| 15 | Recursion |
| 16 | Raw |
| 17 | Enum |
| 18 | Enum16 |
| 19 | Enum32 |
| 20 | C |
| 21 | TableKind |
| 22 | Union |
| 23 | Pointer |
| 24 | Error |
| 25 | FunctionKind |
| 26 | TypeValue |
| 27 | Tuple |
| 28 | Variant |
| 29 | Html |
| 30 | None |
| 31 | NonExpanded |
| 32 | Any |
| 33 | Slice |

### `EventLogKind` values

| Value | Variant |
| -----:| ------- |
| 0 | Write |
| 1 | WriteFile |
| 2 | WriteOther |
| 3 | Read |
| 4 | ReadFile |
| 5 | ReadOther |
| 6 | ReadDir |
| 7 | OpenDir |
| 8 | CloseDir |
| 9 | Socket |
| 10 | Open |
| 11 | Error |
| 12 | TraceLogEvent |
| 13 | EvmEvent |

## Summary

The JSON format is intentionally simple. Events are appended to `trace.json` in the order they occur. Auxiliary files (`trace_metadata.json`, `trace_paths.json`, and the `files/` directory) provide context so that the trace is completely self contained.
