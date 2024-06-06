use std::cmp::Ord;
use std::ops;
use std::path::PathBuf;

use num_derive::FromPrimitive;
use serde::{Deserialize, Serialize};
use serde_repr::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TraceLowLevelEvent {
    Step(StepRecord),
    Path(PathBuf), // should be always generated before usage, so we can stop stream at random n
    Variable(String), // interning new name for it
    Type(TypeRecord), // should be always generated before Value referencing it
    Value(FullValueRecord), // full values: simpler case working even without modification support
    Function(FunctionRecord), // should be always generated before CallRecord referencing it
    Call(CallRecord),
    Return(ReturnRecord),
    Event(RecordEvent),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FullValueRecord {
    pub variable_id: VariableId,
    pub value: ValueRecord,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceLowLevelRecord {
    pub workdir: PathBuf,
    pub program: String,
    pub args: Vec<String>,
    pub events: Vec<TraceLowLevelEvent>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct TraceRecord {
    pub workdir: PathBuf,
    pub paths: Vec<String>,
    pub calls: Vec<DbCall>,
    pub steps: Vec<DbStep>,
    pub variables: Vec<Vec<VariableRecord>>,
    pub types: Vec<TypeRecord>,
    pub events: Vec<DbRecordEvent>,
}

// call keys:

#[derive(Debug, Default, Copy, Clone, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CallKey(pub i64);

impl CallKey {
    pub fn to_usize(self) -> usize {
        self.0 as usize
    }
}

impl ops::Add<usize> for CallKey {
    type Output = CallKey;

    fn add(self, arg: usize) -> Self::Output {
        CallKey(self.0 + arg as i64)
    }
}

impl ops::AddAssign<usize> for CallKey {
    fn add_assign(&mut self, arg: usize) {
        self.0 += arg as i64;
    }
}

pub const NO_KEY: CallKey = CallKey(-1);

// end of call keys code

#[derive(Debug, Default, Copy, Clone, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Line(pub i64);

impl Line {
    pub fn to_usize(self) -> usize {
        self.0 as usize
    }

    pub fn as_i64(&self) -> i64 {
        self.0
    }
}

#[derive(
    Hash, Debug, Default, Copy, Clone, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq,
)]
#[serde(transparent)]
pub struct PathId(pub usize);

#[derive(Debug, Default, Copy, Clone, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq)]
#[serde(transparent)]
pub struct StepId(pub i64);

impl StepId {
    pub fn to_usize(self) -> usize {
        self.0 as usize
    }
}

impl ops::Add<usize> for StepId {
    type Output = StepId;

    fn add(self, arg: usize) -> Self::Output {
        StepId(self.0 + arg as i64)
    }
}

impl ops::Sub<usize> for StepId {
    type Output = StepId;

    fn sub(self, arg: usize) -> Self::Output {
        StepId(self.0 - arg as i64)
    }
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct VariableId(pub usize);

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct FunctionId(pub usize);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbCall {
    pub key: CallKey,
    pub function_id: FunctionId,
    pub args: Vec<ArgRecord>,
    pub return_value: ValueRecord,
    pub step_id: StepId,
    pub depth: usize,
    pub parent_key: CallKey,
    pub children_keys: Vec<CallKey>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CallRecord {
    // pub key: CallKey,
    pub function_id: FunctionId,
    pub args: Vec<ArgRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReturnRecord {
    // implicit by order or explicit in some cases? pub call_key: CallKey
    pub return_value: ValueRecord,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionRecord {
    pub path_id: PathId,
    pub line: Line,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BreakpointRecord {
    pub is_active: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArgRecord {
    pub name: String,
    pub value: ValueRecord,
}

#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
pub struct DbStep {
    pub step_id: StepId,
    pub path_id: PathId,
    pub line: Line,
    pub call_key: CallKey,
}

#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
pub struct StepRecord {
    pub path_id: PathId,
    pub line: Line,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VariableRecord {
    pub name: String,
    pub value: ValueRecord,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TypeRecord {
    pub kind: TypeKind,
    pub lang_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordEvent {
    pub kind: EventLogKind,
    pub content: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbRecordEvent {
    pub kind: EventLogKind,
    pub content: String,
    pub step_id: StepId,
}

#[derive(Debug, Default, Copy, Clone, Serialize, Deserialize, PartialEq)]
#[serde(transparent)]
pub struct TypeIndex(pub usize);

// use ValueRecord for recording custom languages
// use value::Value for interaction with existing frontend
// TODO: convert between them or
// serialize ValueRecord in a compatible way?
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum ValueRecord {
    Int {
        i: i64,
        ti: TypeIndex,
    },
    Float {
        f: f64,
        ti: TypeIndex,
    },
    Bool {
        b: bool,
        ti: TypeIndex,
    },
    String {
        text: String,
        ti: TypeIndex,
    },
    Sequence {
        elements: Vec<ValueRecord>,
        ti: TypeIndex,
    },
    Raw {
        r: String,
        ti: TypeIndex,
    },
    Error {
        msg: String,
        ti: TypeIndex,
    },
    None {
        ti: TypeIndex,
    },
}

#[derive(
    Debug, Default, Copy, Clone, FromPrimitive, Serialize_repr, Deserialize_repr, PartialEq,
)]
#[repr(u8)]
pub enum TypeKind {
    #[default]
    Seq,
    Set,
    HashSet,
    OrderedSet,
    Array,
    Varargs,

    Instance,

    Int,
    Float,
    String,
    CString,
    Char,
    Bool,

    Literal,

    Ref,

    Recursion,

    Raw,

    Enum,
    Enum16,
    Enum32,

    C,

    TableKind,

    Union,

    Pointer,

    Error,

    FunctionKind,

    TypeValue,

    Tuple,

    Variant,

    Html,

    None,
    NonExpanded,
    Any,
    Slice,
}

#[derive(
    Debug, Default, Copy, Clone, FromPrimitive, Serialize_repr, Deserialize_repr, PartialEq,
)]
#[repr(u8)]
pub enum EventLogKind {
    #[default]
    Write,
    WriteFile,
    Read,
    ReadFile,
    // not used for now
    ReadDir,
    OpenDir,
    CloseDir,
    Socket,
    Open,
    // used for trace events
    TraceLogEvent,
}

#[cfg(test)]
mod tests {
    use super::*;

    // An example of how the data structures can be put together to represent the execution of a
    // program.
    //
    // This fictional program looks like this:
    //
    // ```
    //  1: use std::env;
    //  2:
    //  3: fn main() {
    //  4:     let args: Vec<String> = env::args().collect();
    //  5:     let x: usize = args[1].parse().unwrap();
    //  6:     let factors = factorize(x);
    //  7:     println!("{factors:?}");
    //  8: }
    //  9:
    // 10: fn factorize(x: usize) -> Vec<usize> {
    // 11:     assert_eq!(x, 1337);
    // 12:     let result = vec![7, 191];
    // 13:     return result;
    // 14: }
    // ```
    #[test]
    fn sample_trace_of_a_few_steps() {
        let trace = TraceRecord {
            workdir: PathBuf::from("/tmp/"),
            paths: vec![String::from("/tmp/factorize.rs")],
            steps: vec![
                DbStep {
                    step_id: StepId(0),
                    path_id: PathId(0),
                    line: Line(3),
                    call_key: CallKey(0),
                },
                DbStep {
                    step_id: StepId(1),
                    path_id: PathId(0),
                    line: Line(4),
                    call_key: CallKey(1),
                },
                DbStep {
                    step_id: StepId(2),
                    path_id: PathId(0),
                    line: Line(5),
                    call_key: CallKey(1),
                },
                DbStep {
                    step_id: StepId(3),
                    path_id: PathId(0),
                    line: Line(6),
                    call_key: CallKey(1),
                },
                DbStep {
                    step_id: StepId(4),
                    path_id: PathId(0),
                    line: Line(11),
                    call_key: CallKey(2),
                },
                DbStep {
                    step_id: StepId(5),
                    path_id: PathId(0),
                    line: Line(12),
                    call_key: CallKey(2),
                },
                DbStep {
                    step_id: StepId(6),
                    path_id: PathId(0),
                    line: Line(13),
                    call_key: CallKey(2),
                },
                DbStep {
                    step_id: StepId(7),
                    path_id: PathId(0),
                    line: Line(7),
                    call_key: CallKey(1),
                },
            ],
            calls: vec![
                DbCall {
                    key: CallKey(0),
                    function_id: FunctionId(0),
                    args: vec![],
                    return_value: ValueRecord::None { ti: TypeIndex(0) },
                    step_id: StepId(0),
                    depth: 0,
                    parent_key: CallKey(-1),
                    children_keys: vec![CallKey(0)],
                },
                DbCall {
                    key: CallKey(1),
                    function_id: FunctionId(1),
                    args: vec![],
                    return_value: ValueRecord::None { ti: TypeIndex(0) },
                    step_id: StepId(1),
                    depth: 1,
                    parent_key: CallKey(0),
                    children_keys: vec![CallKey(1)],
                },
                DbCall {
                    key: CallKey(2),
                    function_id: FunctionId(2),
                    args: vec![ArgRecord {
                        name: String::from("x"),
                        value: ValueRecord::Int {
                            i: 1337,
                            ti: TypeIndex(1),
                        },
                    }],
                    return_value: ValueRecord::Sequence {
                        elements: vec![
                            ValueRecord::Int {
                                i: 7,
                                ti: TypeIndex(1),
                            },
                            ValueRecord::Int {
                                i: 191,
                                ti: TypeIndex(1),
                            },
                        ],
                        ti: TypeIndex(3),
                    },
                    step_id: StepId(4),
                    depth: 2,
                    parent_key: CallKey(1),
                    children_keys: vec![],
                },
            ],
            variables: vec![
                vec![],
                vec![],
                vec![VariableRecord {
                    name: String::from("args"),
                    value: ValueRecord::Sequence {
                        elements: vec![
                            ValueRecord::String {
                                text: String::from("/tmp/factorize.rs"),
                                ti: TypeIndex(2),
                            },
                            ValueRecord::String {
                                text: String::from("1337"),
                                ti: TypeIndex(2),
                            },
                        ],
                        ti: TypeIndex(3),
                    },
                }],
                vec![
                    VariableRecord {
                        name: String::from("args"),
                        value: ValueRecord::Sequence {
                            elements: vec![
                                ValueRecord::String {
                                    text: String::from("/tmp/factorize.rs"),
                                    ti: TypeIndex(2),
                                },
                                ValueRecord::String {
                                    text: String::from("1337"),
                                    ti: TypeIndex(2),
                                },
                            ],
                            ti: TypeIndex(3),
                        },
                    },
                    VariableRecord {
                        name: String::from("x"),
                        value: ValueRecord::Int {
                            i: 1337,
                            ti: TypeIndex(1),
                        },
                    },
                ],
                vec![VariableRecord {
                    name: String::from("x"),
                    value: ValueRecord::Int {
                        i: 1337,
                        ti: TypeIndex(1),
                    },
                }],
                vec![VariableRecord {
                    name: String::from("x"),
                    value: ValueRecord::Int {
                        i: 1337,
                        ti: TypeIndex(1),
                    },
                }],
                vec![
                    VariableRecord {
                        name: String::from("x"),
                        value: ValueRecord::Int {
                            i: 1337,
                            ti: TypeIndex(1),
                        },
                    },
                    VariableRecord {
                        name: String::from("result"),
                        value: ValueRecord::Sequence {
                            elements: vec![
                                ValueRecord::Int {
                                    i: 7,
                                    ti: TypeIndex(1),
                                },
                                ValueRecord::Int {
                                    i: 191,
                                    ti: TypeIndex(1),
                                },
                            ],
                            ti: TypeIndex(3),
                        },
                    },
                ],
                vec![
                    VariableRecord {
                        name: String::from("args"),
                        value: ValueRecord::Sequence {
                            elements: vec![
                                ValueRecord::String {
                                    text: String::from("/tmp/factorize.rs"),
                                    ti: TypeIndex(2),
                                },
                                ValueRecord::String {
                                    text: String::from("1337"),
                                    ti: TypeIndex(2),
                                },
                            ],
                            ti: TypeIndex(3),
                        },
                    },
                    VariableRecord {
                        name: String::from("x"),
                        value: ValueRecord::Int {
                            i: 1337,
                            ti: TypeIndex(1),
                        },
                    },
                ],
            ],
            events: vec![DbRecordEvent {
                kind: EventLogKind::Write,
                content: String::from("[7, 191]\n"),
                step_id: StepId(7),
            }],
            types: vec![
                TypeRecord {
                    kind: TypeKind::None,
                    lang_type: String::from("None"),
                },
                TypeRecord {
                    kind: TypeKind::Int,
                    lang_type: String::from("usize"),
                },
                TypeRecord {
                    kind: TypeKind::String,
                    lang_type: String::from("String"),
                },
                TypeRecord {
                    kind: TypeKind::Seq,
                    lang_type: String::from("Vec"),
                },
            ],
        };

        assert_eq!(serde_json::to_string(&trace).unwrap().len(), 2046);
    }
}
