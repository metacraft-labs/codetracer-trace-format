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
pub struct ArgRecord {
    pub name: String,
    pub value: ValueRecord,
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

#[derive(Debug, Default, Copy, Clone, Serialize, Deserialize, PartialEq)]
#[serde(transparent)]
pub struct TypeId(pub usize);

// use ValueRecord for recording custom languages
// use value::Value for interaction with existing frontend
// TODO: convert between them or
// serialize ValueRecord in a compatible way?
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum ValueRecord {
    Int {
        i: i64,
        type_id: TypeId,
    },
    Float {
        f: f64,
        type_id: TypeId,
    },
    Bool {
        b: bool,
        type_id: TypeId,
    },
    String {
        text: String,
        type_id: TypeId,
    },
    Sequence {
        elements: Vec<ValueRecord>,
        type_id: TypeId,
    },
    Raw {
        r: String,
        type_id: TypeId,
    },
    Error {
        msg: String,
        type_id: TypeId,
    },
    None {
        type_id: TypeId,
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
