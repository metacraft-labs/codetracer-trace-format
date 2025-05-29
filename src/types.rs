//! Serializable record types for the trace format used by CodeTracer.
//!
//! All structures derive [`serde::Serialize`] and [`serde::Deserialize`].

use std::cmp::Ord;
use std::ops;
use std::path::PathBuf;

use num_derive::FromPrimitive;
use serde::{Deserialize, Serialize};
use serde_repr::*;

// currently, we do assume that we record the whole program
// so, we try to include minimal amount of data,
// as we can reconstruct some things like depth, id-s etc
// afterwards in postprocessing
// this assumption can change in the future

/// Low level building blocks that make up a recorded trace.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TraceLowLevelEvent {
    Step(StepRecord),
    Path(PathBuf),            // should be always generated before usage, so we can stop stream at random n
    VariableName(String),     // interning new name for variables
    Variable(String),         // interning new name for variables: backward compat
    Type(TypeRecord),         // should be always generated before Value referencing it
    Value(FullValueRecord),   // full values: simpler case working even without modification support
    Function(FunctionRecord), // should be always generated before CallRecord referencing it
    Call(CallRecord),
    Return(ReturnRecord),
    Event(RecordEvent),
    Asm(Vec<String>),

    // events useful for history
    BindVariable(BindVariableRecord), // bind a variable to a certain place
    Assignment(AssignmentRecord),     // assigning or passing by params
    DropVariables(Vec<VariableId>),   // dropping variables e.g. in the end of scope/heap lifetime

    // experimental modification value tracking events
    // probably will be reworked or replaced by the newer
    // history events with some additions
    // for now here for backward compatibility/experiments
    CompoundValue(CompoundValueRecord),
    CellValue(CellValueRecord),
    AssignCompoundItem(AssignCompoundItemRecord),
    AssignCell(AssignCellRecord),
    VariableCell(VariableCellRecord),
    DropVariable(VariableId),

    // normal event, workaround for cases when we need to drop
    // a step event, but the trace needs to be append-only
    DropLastStep,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BindVariableRecord {
    pub variable_id: VariableId,
    pub place: Place,
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub enum PassBy {
    #[default]
    Value,
    Reference,
    // TODO: languages with more special ways of passing
}

// used for all kinds of by value/by ref assignment/passing
//   * assignments
//   * arg(parameter) passing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssignmentRecord {
    pub to: VariableId,
    pub pass_by: PassBy,
    pub from: RValue,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum RValue {
    Simple(VariableId),
    // eventually in future:
    // discuss more: Const(String, ValueRecord),
    Compound(Vec<VariableId>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompoundValueRecord {
    pub place: Place,
    pub value: ValueRecord,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CellValueRecord {
    pub place: Place,
    pub value: ValueRecord,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct AssignCompoundItemRecord {
    pub place: Place,
    pub index: usize,
    pub item_place: Place,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssignCellRecord {
    pub place: Place,
    pub new_value: ValueRecord,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VariableCellRecord {
    pub variable_id: VariableId,
    pub place: Place,
}

// opaque(?) id:
//   can be anything, depending on the lang
//   and its implementation
//   usually we expects it's
//     * some kind of pointer/address
//     * some kind of internal index(interpreter or stack)
//     * some other kind of id which somehow
//       uniquely represents the "place" of this variable
//  it's useful to let us track things on the more direct value
//    level/things like aliasing/mutable variables in different frames
//    history of mutations to a value etc
#[derive(Hash, Debug, Default, Copy, Clone, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq)]
pub struct Place(pub i64);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FullValueRecord {
    pub variable_id: VariableId,
    pub value: ValueRecord,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceMetadata {
    pub workdir: PathBuf,
    pub program: String,
    pub args: Vec<String>,
}

// call keys:

#[derive(Debug, Default, Copy, Clone, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CallKey(pub i64);

impl Into<usize> for CallKey {
    fn into(self) -> usize {
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

#[derive(Debug, Default, Copy, Clone, Serialize, Deserialize, PartialEq)]
#[serde(transparent)]
pub struct Line(pub i64);

impl Into<usize> for Line {
    fn into(self) -> usize {
        self.0 as usize
    }
}

impl Into<i64> for Line {
    fn into(self) -> i64 {
        self.0
    }
}

#[derive(Hash, Debug, Default, Copy, Clone, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq)]
#[serde(transparent)]
pub struct PathId(pub usize);

impl Into<usize> for PathId {
    fn into(self) -> usize {
        self.0
    }
}

#[derive(Debug, Default, Copy, Clone, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq)]
#[serde(transparent)]
pub struct StepId(pub i64);

impl Into<usize> for StepId {
    fn into(self) -> usize {
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

#[derive(Hash, Debug, Default, Copy, Clone, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq)]
pub struct VariableId(pub usize);

impl Into<usize> for VariableId {
    fn into(self) -> usize {
        self.0
    }
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq)]
pub struct FunctionId(pub usize);

impl Into<usize> for FunctionId {
    fn into(self) -> usize {
        self.0
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CallRecord {
    // pub key: CallKey,
    pub function_id: FunctionId,
    pub args: Vec<FullValueRecord>,
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

// #[derive(Debug, Clone, Serialize, Deserialize)]
// pub struct ArgRecord {
//     pub name: String,
//     pub value: ValueRecord,
// }

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
    // for now only for Struct and Pointer: TODO eventually
    // replace with an enum for TypeRecord, or with more cases
    // in TypeSpecificInfo for collections, etc
    pub specific_info: TypeSpecificInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldTypeRecord {
    pub name: String,
    pub type_id: TypeId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum TypeSpecificInfo {
    None,
    Struct { fields: Vec<FieldTypeRecord> },
    Pointer { dereference_type_id: TypeId },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordEvent {
    pub kind: EventLogKind,
    pub metadata: String,
    pub content: String,
}

#[derive(Debug, Default, Copy, Clone, Serialize, Deserialize, PartialEq)]
#[serde(transparent)]
pub struct TypeId(pub usize);

impl Into<usize> for TypeId {
    fn into(self) -> usize {
        self.0
    }
}

// use ValueRecord for recording custom languages
// use value::Value for interaction with existing frontend
// TODO: convert between them or
// serialize ValueRecord in a compatible way?
/// Representation of a runtime value captured in a trace.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind")]
pub enum ValueRecord {
    Int {
        i: i64,
        type_id: TypeId,
    },
    Int128 {
        i: i128,
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
        is_slice: bool,
        type_id: TypeId,
    },
    Tuple {
        elements: Vec<ValueRecord>,
        type_id: TypeId,
    },
    Struct {
        field_values: Vec<ValueRecord>,
        type_id: TypeId, // must point to Type with STRUCT kind and TypeSpecificInfo::Struct
    },
    Variant {
        discriminator: String,      // TODO: eventually a more specific kind of value/type
        contents: Box<ValueRecord>, // usually a Struct or a Tuple
        type_id: TypeId,
    },
    // TODO: eventually add more pointer-like variants
    // or more fields (address?)
    Reference {
        dereferenced: Box<ValueRecord>,
        address: u64,
        mutable: bool,
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
    Cell {
        place: Place,
    },
}

/// Categories of types recorded in the trace.
#[derive(Debug, Default, Copy, Clone, FromPrimitive, Serialize_repr, Deserialize_repr, PartialEq)]
#[repr(u8)]
pub enum TypeKind {
    #[default]
    Seq,
    Set,
    HashSet,
    OrderedSet,
    Array,
    Varargs,

    Struct,

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

/// Kinds of I/O or log events that can appear in a trace.
#[derive(Debug, Default, Copy, Clone, FromPrimitive, Serialize_repr, Deserialize_repr, PartialEq)]
#[repr(u8)]
pub enum EventLogKind {
    #[default]
    Write,
    WriteFile,
    WriteOther,
    Read,
    ReadFile,
    ReadOther,
    // not used for now
    ReadDir,
    OpenDir,
    CloseDir,
    Socket,
    Open,
    Error,
    // used for trace events
    TraceLogEvent,
}
