use crate::trace_capnp::trace;
use codetracer_trace_types::{TraceLowLevelEvent, VariableId};
use capnp::serialize_packed;
use std::str::FromStr;

/// The first 5 bytes identify the file as a CodeTracer file (hex l33tsp33k - C0DE72ACE2 for "CodeTracer").
/// The next 3 bytes are reserved/version info. In the initial version, they are zero. Non-zero values might
/// indicate incompatible future versions.
/// The header is 8 bytes in size, ensuring 64-bit alignment for the rest of the file.
pub const HEADER: &[u8] = &[0xC0, 0xDE, 0x72, 0xAC, 0xE2, 0x00, 0x00, 0x00];

impl From<codetracer_trace_types::TypeKind> for trace::TypeKind {
    fn from(item: codetracer_trace_types::TypeKind) -> Self {
        match item {
            codetracer_trace_types::TypeKind::Seq => trace::TypeKind::Seq,
            codetracer_trace_types::TypeKind::Set => trace::TypeKind::Set,
            codetracer_trace_types::TypeKind::HashSet => trace::TypeKind::HashSet,
            codetracer_trace_types::TypeKind::OrderedSet => trace::TypeKind::OrderedSet,
            codetracer_trace_types::TypeKind::Array => trace::TypeKind::Array,
            codetracer_trace_types::TypeKind::Varargs => trace::TypeKind::Varargs,
            codetracer_trace_types::TypeKind::Struct => trace::TypeKind::Struct,
            codetracer_trace_types::TypeKind::Int => trace::TypeKind::Int,
            codetracer_trace_types::TypeKind::Float => trace::TypeKind::Float,
            codetracer_trace_types::TypeKind::String => trace::TypeKind::String,
            codetracer_trace_types::TypeKind::CString => trace::TypeKind::Cstring,
            codetracer_trace_types::TypeKind::Char => trace::TypeKind::Char,
            codetracer_trace_types::TypeKind::Bool => trace::TypeKind::Bool,
            codetracer_trace_types::TypeKind::Literal => trace::TypeKind::Literal,
            codetracer_trace_types::TypeKind::Ref => trace::TypeKind::Ref,
            codetracer_trace_types::TypeKind::Recursion => trace::TypeKind::Recursion,
            codetracer_trace_types::TypeKind::Raw => trace::TypeKind::Raw,
            codetracer_trace_types::TypeKind::Enum => trace::TypeKind::Enum,
            codetracer_trace_types::TypeKind::Enum16 => trace::TypeKind::Enum16,
            codetracer_trace_types::TypeKind::Enum32 => trace::TypeKind::Enum32,
            codetracer_trace_types::TypeKind::C => trace::TypeKind::C,
            codetracer_trace_types::TypeKind::TableKind => trace::TypeKind::TableKind,
            codetracer_trace_types::TypeKind::Union => trace::TypeKind::Union,
            codetracer_trace_types::TypeKind::Pointer => trace::TypeKind::Pointer,
            codetracer_trace_types::TypeKind::Error => trace::TypeKind::Error,
            codetracer_trace_types::TypeKind::FunctionKind => trace::TypeKind::FunctionKind,
            codetracer_trace_types::TypeKind::TypeValue => trace::TypeKind::TypeValue,
            codetracer_trace_types::TypeKind::Tuple => trace::TypeKind::Tuple,
            codetracer_trace_types::TypeKind::Variant => trace::TypeKind::Variant,
            codetracer_trace_types::TypeKind::Html => trace::TypeKind::Html,
            codetracer_trace_types::TypeKind::None => trace::TypeKind::None,
            codetracer_trace_types::TypeKind::NonExpanded => trace::TypeKind::NonExpanded,
            codetracer_trace_types::TypeKind::Any => trace::TypeKind::Any,
            codetracer_trace_types::TypeKind::Slice => trace::TypeKind::Slice,
        }
    }
}

impl From<trace::TypeKind> for codetracer_trace_types::TypeKind {
    fn from(item: trace::TypeKind) -> Self {
        match item {
            trace::TypeKind::Seq => codetracer_trace_types::TypeKind::Seq,
            trace::TypeKind::Set => codetracer_trace_types::TypeKind::Set,
            trace::TypeKind::HashSet => codetracer_trace_types::TypeKind::HashSet,
            trace::TypeKind::OrderedSet => codetracer_trace_types::TypeKind::OrderedSet,
            trace::TypeKind::Array => codetracer_trace_types::TypeKind::Array,
            trace::TypeKind::Varargs => codetracer_trace_types::TypeKind::Varargs,
            trace::TypeKind::Struct => codetracer_trace_types::TypeKind::Struct,
            trace::TypeKind::Int => codetracer_trace_types::TypeKind::Int,
            trace::TypeKind::Float => codetracer_trace_types::TypeKind::Float,
            trace::TypeKind::String => codetracer_trace_types::TypeKind::String,
            trace::TypeKind::Cstring => codetracer_trace_types::TypeKind::CString,
            trace::TypeKind::Char => codetracer_trace_types::TypeKind::Char,
            trace::TypeKind::Bool => codetracer_trace_types::TypeKind::Bool,
            trace::TypeKind::Literal => codetracer_trace_types::TypeKind::Literal,
            trace::TypeKind::Ref => codetracer_trace_types::TypeKind::Ref,
            trace::TypeKind::Recursion => codetracer_trace_types::TypeKind::Recursion,
            trace::TypeKind::Raw => codetracer_trace_types::TypeKind::Raw,
            trace::TypeKind::Enum => codetracer_trace_types::TypeKind::Enum,
            trace::TypeKind::Enum16 => codetracer_trace_types::TypeKind::Enum16,
            trace::TypeKind::Enum32 => codetracer_trace_types::TypeKind::Enum32,
            trace::TypeKind::C => codetracer_trace_types::TypeKind::C,
            trace::TypeKind::TableKind => codetracer_trace_types::TypeKind::TableKind,
            trace::TypeKind::Union => codetracer_trace_types::TypeKind::Union,
            trace::TypeKind::Pointer => codetracer_trace_types::TypeKind::Pointer,
            trace::TypeKind::Error => codetracer_trace_types::TypeKind::Error,
            trace::TypeKind::FunctionKind => codetracer_trace_types::TypeKind::FunctionKind,
            trace::TypeKind::TypeValue => codetracer_trace_types::TypeKind::TypeValue,
            trace::TypeKind::Tuple => codetracer_trace_types::TypeKind::Tuple,
            trace::TypeKind::Variant => codetracer_trace_types::TypeKind::Variant,
            trace::TypeKind::Html => codetracer_trace_types::TypeKind::Html,
            trace::TypeKind::None => codetracer_trace_types::TypeKind::None,
            trace::TypeKind::NonExpanded => codetracer_trace_types::TypeKind::NonExpanded,
            trace::TypeKind::Any => codetracer_trace_types::TypeKind::Any,
            trace::TypeKind::Slice => codetracer_trace_types::TypeKind::Slice,
        }
    }
}

impl From<trace::EventLogKind> for codetracer_trace_types::EventLogKind {
    fn from(value: trace::EventLogKind) -> Self {
        match value {
            trace::EventLogKind::Write => codetracer_trace_types::EventLogKind::Write,
            trace::EventLogKind::WriteFile => codetracer_trace_types::EventLogKind::WriteFile,
            trace::EventLogKind::WriteOther => codetracer_trace_types::EventLogKind::WriteOther,
            trace::EventLogKind::Read => codetracer_trace_types::EventLogKind::Read,
            trace::EventLogKind::ReadFile => codetracer_trace_types::EventLogKind::ReadFile,
            trace::EventLogKind::ReadOther => codetracer_trace_types::EventLogKind::ReadOther,
            trace::EventLogKind::ReadDir => codetracer_trace_types::EventLogKind::ReadDir,
            trace::EventLogKind::OpenDir => codetracer_trace_types::EventLogKind::OpenDir,
            trace::EventLogKind::CloseDir => codetracer_trace_types::EventLogKind::CloseDir,
            trace::EventLogKind::Socket => codetracer_trace_types::EventLogKind::Socket,
            trace::EventLogKind::Open => codetracer_trace_types::EventLogKind::Open,
            trace::EventLogKind::Error => codetracer_trace_types::EventLogKind::Error,
            trace::EventLogKind::TraceLogEvent => codetracer_trace_types::EventLogKind::TraceLogEvent,
            trace::EventLogKind::EvmEvent => codetracer_trace_types::EventLogKind::EvmEvent,
        }
    }
}

impl From<codetracer_trace_types::EventLogKind> for trace::EventLogKind {
    fn from(value: codetracer_trace_types::EventLogKind) -> Self {
        match value {
            codetracer_trace_types::EventLogKind::Write => trace::EventLogKind::Write,
            codetracer_trace_types::EventLogKind::WriteFile => trace::EventLogKind::WriteFile,
            codetracer_trace_types::EventLogKind::WriteOther => trace::EventLogKind::WriteOther,
            codetracer_trace_types::EventLogKind::Read => trace::EventLogKind::Read,
            codetracer_trace_types::EventLogKind::ReadFile => trace::EventLogKind::ReadFile,
            codetracer_trace_types::EventLogKind::ReadOther => trace::EventLogKind::ReadOther,
            codetracer_trace_types::EventLogKind::ReadDir => trace::EventLogKind::ReadDir,
            codetracer_trace_types::EventLogKind::OpenDir => trace::EventLogKind::OpenDir,
            codetracer_trace_types::EventLogKind::CloseDir => trace::EventLogKind::CloseDir,
            codetracer_trace_types::EventLogKind::Socket => trace::EventLogKind::Socket,
            codetracer_trace_types::EventLogKind::Open => trace::EventLogKind::Open,
            codetracer_trace_types::EventLogKind::Error => trace::EventLogKind::Error,
            codetracer_trace_types::EventLogKind::TraceLogEvent => trace::EventLogKind::TraceLogEvent,
            codetracer_trace_types::EventLogKind::EvmEvent => trace::EventLogKind::EvmEvent,
        }
    }
}

impl From<codetracer_trace_types::PassBy> for trace::PassBy {
    fn from(value: codetracer_trace_types::PassBy) -> Self {
        match value {
            codetracer_trace_types::PassBy::Value => trace::PassBy::Value,
            codetracer_trace_types::PassBy::Reference => trace::PassBy::Reference,
        }
    }
}

fn conv_valuerecord(bldr: crate::trace_capnp::trace::value_record::Builder, vr: &codetracer_trace_types::ValueRecord) {
    match vr {
        codetracer_trace_types::ValueRecord::Int { i, type_id } => {
            let mut qi = bldr.init_int();
            qi.set_i(*i);
            let mut q_typ_id = qi.init_type_id();
            q_typ_id.set_i(type_id.0.try_into().unwrap());
        }
        codetracer_trace_types::ValueRecord::Float { f, type_id } => {
            let mut qf = bldr.init_float();
            qf.set_f(*f);
            let mut q_typ_id = qf.init_type_id();
            q_typ_id.set_i(type_id.0.try_into().unwrap());
        }
        codetracer_trace_types::ValueRecord::Bool { b, type_id } => {
            let mut qb = bldr.init_bool();
            qb.set_b(*b);
            let mut q_typ_id = qb.init_type_id();
            q_typ_id.set_i(type_id.0.try_into().unwrap());
        }
        codetracer_trace_types::ValueRecord::String { text, type_id } => {
            let mut qs = bldr.init_string();
            qs.set_text(text);
            let mut q_typ_id = qs.init_type_id();
            q_typ_id.set_i(type_id.0.try_into().unwrap());
        }
        codetracer_trace_types::ValueRecord::Sequence { elements, is_slice, type_id } => {
            let mut qseq = bldr.init_sequence();
            let mut elems = qseq.reborrow().init_elements(elements.len().try_into().unwrap());
            for i in 0..elements.len() {
                let ele = &elements[i];
                let bele = elems.reborrow().get(i.try_into().unwrap());
                conv_valuerecord(bele, ele);
            }
            qseq.set_is_slice(*is_slice);
            let mut q_typ_id = qseq.init_type_id();
            q_typ_id.set_i(type_id.0.try_into().unwrap());
        }
        codetracer_trace_types::ValueRecord::Tuple { elements, type_id } => {
            let mut qtup = bldr.init_tuple();
            let mut elems = qtup.reborrow().init_elements(elements.len().try_into().unwrap());
            for i in 0..elements.len() {
                let ele = &elements[i];
                let bele = elems.reborrow().get(i.try_into().unwrap());
                conv_valuerecord(bele, ele);
            }
            let mut q_typ_id = qtup.init_type_id();
            q_typ_id.set_i(type_id.0.try_into().unwrap());
        }
        codetracer_trace_types::ValueRecord::Struct { field_values, type_id } => {
            let mut qstruc = bldr.init_struct();
            let mut elems = qstruc.reborrow().init_field_values(field_values.len().try_into().unwrap());
            for i in 0..field_values.len() {
                let ele = &field_values[i];
                let bele = elems.reborrow().get(i.try_into().unwrap());
                conv_valuerecord(bele, ele);
            }
            let mut q_typ_id = qstruc.init_type_id();
            q_typ_id.set_i(type_id.0.try_into().unwrap());
        }
        codetracer_trace_types::ValueRecord::Variant {
            discriminator,
            contents,
            type_id,
        } => {
            let mut qvariant = bldr.init_variant();
            qvariant.set_discriminator(discriminator);
            let bcontents = qvariant.reborrow().init_contents();
            conv_valuerecord(bcontents, contents);
            let mut q_typ_id = qvariant.init_type_id();
            q_typ_id.set_i(type_id.0.try_into().unwrap());
        }
        codetracer_trace_types::ValueRecord::Reference {
            dereferenced,
            address,
            mutable,
            type_id,
        } => {
            let mut qreference = bldr.init_reference();
            let bdereferenced = qreference.reborrow().init_dereferenced();
            conv_valuerecord(bdereferenced, dereferenced);
            qreference.set_address(*address);
            qreference.set_mutable(*mutable);
            let mut q_typ_id = qreference.init_type_id();
            q_typ_id.set_i(type_id.0.try_into().unwrap());
        }
        codetracer_trace_types::ValueRecord::Raw { r, type_id } => {
            let mut qraw = bldr.init_raw();
            qraw.set_r(r);
            let mut q_typ_id = qraw.init_type_id();
            q_typ_id.set_i(type_id.0.try_into().unwrap());
        }
        codetracer_trace_types::ValueRecord::Error { msg, type_id } => {
            let mut qerr = bldr.init_error();
            qerr.set_msg(msg);
            let mut q_typ_id = qerr.init_type_id();
            q_typ_id.set_i(type_id.0.try_into().unwrap());
        }
        codetracer_trace_types::ValueRecord::None { type_id } => {
            let qnone = bldr.init_none();
            let mut q_typ_id = qnone.init_type_id();
            q_typ_id.set_i(type_id.0.try_into().unwrap());
        }
        codetracer_trace_types::ValueRecord::Cell { place } => {
            let qcell = bldr.init_cell();
            let mut q_place = qcell.init_place();
            q_place.set_p(place.0);
        }
        codetracer_trace_types::ValueRecord::BigInt { b, negative, type_id } => {
            let mut qbigint = bldr.init_bigint();
            let mut bigint_b = qbigint.reborrow().init_b(b.len().try_into().unwrap());
            for i in 0..=b.len() {
                bigint_b.set(i.try_into().unwrap(), b[i]);
            }
            qbigint.set_negative(*negative);
            let mut q_typ_id = qbigint.init_type_id();
            q_typ_id.set_i(type_id.0.try_into().unwrap());
        }
        // The legacy capnp schema does not have a dedicated Char union member,
        // so we serialize it as Raw with the char's string representation.
        codetracer_trace_types::ValueRecord::Char { c, type_id } => {
            let mut qraw = bldr.init_raw();
            qraw.set_r(&c.to_string());
            let mut q_typ_id = qraw.init_type_id();
            q_typ_id.set_i(type_id.0.try_into().unwrap());
        }
    }
}

pub fn write_trace(q: &[codetracer_trace_types::TraceLowLevelEvent], output: &mut impl std::io::Write) -> ::capnp::Result<()> {
    let mut message = ::capnp::message::Builder::new_default();

    let trace = message.init_root::<trace::Builder>();
    let mut events = trace.init_events(q.len().try_into().unwrap());

    for i in 0..q.len() {
        let qq = &q[i];
        let mut event = events.reborrow().get(i.try_into().unwrap());
        match qq {
            TraceLowLevelEvent::Type(type_record) => {
                let mut typ = event.init_type();

                typ.set_kind(type_record.kind.into());
                typ.set_lang_type(type_record.lang_type.clone());
                let mut specific_info = typ.init_specific_info();
                match &type_record.specific_info {
                    codetracer_trace_types::TypeSpecificInfo::None => {
                        specific_info.set_none(());
                    }
                    codetracer_trace_types::TypeSpecificInfo::Struct { fields } => {
                        let strct = specific_info.init_struct();
                        let mut flds = strct.init_fields(fields.len().try_into().unwrap());
                        for i in 0..fields.len() {
                            let ftr = &fields[i];
                            let mut fld = flds.reborrow().get(i.try_into().unwrap());
                            fld.set_name(ftr.name.clone());
                            let mut typ_id = fld.init_type_id();
                            typ_id.set_i(ftr.type_id.0.try_into().unwrap());
                        }
                    }
                    codetracer_trace_types::TypeSpecificInfo::Pointer { dereference_type_id } => {
                        let ptr = specific_info.init_pointer();
                        let mut deref_typ_id = ptr.init_dereference_type_id();
                        deref_typ_id.set_i(dereference_type_id.0.try_into().unwrap());
                    }
                }
            }
            TraceLowLevelEvent::Path(pathbuf) => {
                let mut path_buf = event.init_path();
                path_buf.set_p(pathbuf.to_str().unwrap_or_default());
            }
            TraceLowLevelEvent::Function(functionrecord) => {
                let mut function_record = event.init_function();
                let mut path_id = function_record.reborrow().init_path_id();
                path_id.set_i(functionrecord.path_id.0.try_into().unwrap());
                let mut line = function_record.reborrow().init_line();
                line.set_l(functionrecord.line.0);
                function_record.set_name(functionrecord.name.clone());
            }
            TraceLowLevelEvent::Call(callrecord) => {
                let mut call_record = event.init_call();
                let mut function_id = call_record.reborrow().init_function_id();
                function_id.set_i(callrecord.function_id.0.try_into().unwrap());
                let mut function_args = call_record.init_args(callrecord.args.len().try_into().unwrap());
                for i in 0..callrecord.args.len() {
                    let farg = &callrecord.args[i];
                    let mut arg = function_args.reborrow().get(i.try_into().unwrap());
                    let mut var_id = arg.reborrow().init_variable_id();
                    var_id.set_i(farg.variable_id.0.try_into().unwrap());
                    let val_rec = arg.init_value();
                    conv_valuerecord(val_rec, &farg.value);
                }
            }
            TraceLowLevelEvent::Step(steprecord) => {
                let mut step_record = event.init_step();
                let mut path_id = step_record.reborrow().init_path_id();
                path_id.set_i(steprecord.path_id.0.try_into().unwrap());
                let mut line = step_record.init_line();
                line.set_l(steprecord.line.0);
            }
            TraceLowLevelEvent::VariableName(varname) => {
                event.set_variable_name(varname);
            }
            TraceLowLevelEvent::Value(fullvaluerecord) => {
                let mut value = event.init_value();
                let mut var_id = value.reborrow().init_variable_id();
                var_id.set_i(fullvaluerecord.variable_id.0.try_into().unwrap());
                let value_value = value.init_value();
                conv_valuerecord(value_value, &fullvaluerecord.value);
            }
            TraceLowLevelEvent::Return(returnrecord) => {
                let ret = event.init_return();
                let ret_value = ret.init_return_value();
                conv_valuerecord(ret_value, &returnrecord.return_value);
            }
            TraceLowLevelEvent::Event(recordevent) => {
                let mut ret = event.init_event();
                ret.set_metadata(&recordevent.metadata);
                ret.set_content(&recordevent.content);
                ret.set_kind(recordevent.kind.into());
            }
            TraceLowLevelEvent::Variable(str) => {
                event.set_variable(str);
            }
            TraceLowLevelEvent::Asm(strs) => {
                let mut ret = event.init_asm(strs.len().try_into().unwrap());
                for i in 0..strs.len() {
                    ret.set(i.try_into().unwrap(), &strs[i]);
                }
            }
            TraceLowLevelEvent::BindVariable(bindvarrec) => {
                let mut ret = event.init_bind_variable();
                let mut ret_var_id = ret.reborrow().init_variable_id();
                ret_var_id.set_i(bindvarrec.variable_id.0.try_into().unwrap());
                let mut ret_place = ret.init_place();
                ret_place.set_p(bindvarrec.place.0.try_into().unwrap());
            }
            TraceLowLevelEvent::Assignment(assrec) => {
                let mut ret = event.init_assignment();
                let mut ret_to = ret.reborrow().init_to();
                ret_to.set_i(assrec.to.0.try_into().unwrap());
                ret.set_pass_by(assrec.pass_by.clone().into());
                let ret_from = ret.init_from();
                match &assrec.from {
                    codetracer_trace_types::RValue::Simple(variable_id) => {
                        let mut ret_from_simple = ret_from.init_simple();
                        ret_from_simple.set_i(variable_id.0.try_into().unwrap());
                    }
                    codetracer_trace_types::RValue::Compound(variable_ids) => {
                        let mut ret_from_compound = ret_from.init_compound(variable_ids.len().try_into().unwrap());
                        for i in 0..variable_ids.len() {
                            let mut r = ret_from_compound.reborrow().get(i.try_into().unwrap());
                            r.set_i(variable_ids[i].0.try_into().unwrap());
                        }
                    }
                }
            }
            TraceLowLevelEvent::DropLastStep => {
                event.set_drop_last_step(());
            }
            TraceLowLevelEvent::DropVariables(vars) => {
                let mut ret_vars = event.init_drop_variables(vars.len().try_into().unwrap());
                for i in 0..vars.len() {
                    let mut q = ret_vars.reborrow().get(i.try_into().unwrap());
                    q.set_i(vars[i].0.try_into().unwrap());
                }
            }
            TraceLowLevelEvent::DropVariable(varid) => {
                let mut ret = event.init_drop_variable();
                ret.set_i(varid.0.try_into().unwrap());
            }
            TraceLowLevelEvent::CompoundValue(cvr) => {
                let mut ret = event.init_compound_value();
                let mut ret_place = ret.reborrow().init_place();
                ret_place.set_p(cvr.place.0.try_into().unwrap());
                let ret_value = ret.init_value();
                conv_valuerecord(ret_value, &cvr.value);
            }
            TraceLowLevelEvent::CellValue(cvr) => {
                let mut ret = event.init_cell_value();
                let mut ret_place = ret.reborrow().init_place();
                ret_place.set_p(cvr.place.0.try_into().unwrap());
                let ret_value = ret.init_value();
                conv_valuerecord(ret_value, &cvr.value);
            }
            TraceLowLevelEvent::AssignCell(acr) => {
                let mut ret = event.init_assign_cell();
                let mut ret_place = ret.reborrow().init_place();
                ret_place.set_p(acr.place.0.try_into().unwrap());
                let ret_new_value = ret.init_new_value();
                conv_valuerecord(ret_new_value, &acr.new_value);
            }
            TraceLowLevelEvent::AssignCompoundItem(aci) => {
                let mut ret = event.init_assign_compound_item();
                let mut ret_place = ret.reborrow().init_place();
                ret_place.set_p(aci.place.0.try_into().unwrap());
                ret.set_index(aci.index.try_into().unwrap());
                let mut ret_item_place = ret.init_item_place();
                ret_item_place.set_p(aci.item_place.0.try_into().unwrap());
            }
            TraceLowLevelEvent::VariableCell(vcr) => {
                let mut ret = event.init_variable_cell();
                let mut ret_variable_id = ret.reborrow().init_variable_id();
                ret_variable_id.set_i(vcr.variable_id.0.try_into().unwrap());
                let mut ret_place = ret.init_place();
                ret_place.set_p(vcr.place.0.try_into().unwrap());
            }
            TraceLowLevelEvent::ThreadStart(tid) => {
                let mut ret = event.init_thread_start();
                ret.set_i(tid.0);
            }
            TraceLowLevelEvent::ThreadExit(tid) => {
                let mut ret = event.init_thread_exit();
                ret.set_i(tid.0);
            }
            TraceLowLevelEvent::ThreadSwitch(tid) => {
                let mut ret = event.init_thread_switch();
                ret.set_i(tid.0);
            }
        }
    }

    output.write_all(HEADER)?;

    serialize_packed::write_message(output, &message)
}

fn get_value_records(r: capnp::struct_list::Reader<trace::value_record::Owned>) -> Result<Vec<codetracer_trace_types::ValueRecord>, capnp::Error> {
    let mut res: Vec<codetracer_trace_types::ValueRecord> = Vec::with_capacity(r.len().try_into().unwrap());
    for i in 0..r.len() {
        res.push(get_value_record(r.get(i))?);
    }
    Ok(res)
}

fn get_value_record(r: trace::value_record::Reader) -> Result<codetracer_trace_types::ValueRecord, capnp::Error> {
    match r.which() {
        Ok(trace::value_record::Which::Int(q)) => Ok(codetracer_trace_types::ValueRecord::Int {
            i: q.get_i(),
            type_id: codetracer_trace_types::TypeId(q.get_type_id()?.get_i().try_into().unwrap()),
        }),
        Ok(trace::value_record::Which::Float(q)) => Ok(codetracer_trace_types::ValueRecord::Float {
            f: q.get_f(),
            type_id: codetracer_trace_types::TypeId(q.get_type_id()?.get_i().try_into().unwrap()),
        }),
        Ok(trace::value_record::Which::Bool(q)) => Ok(codetracer_trace_types::ValueRecord::Bool {
            b: q.get_b(),
            type_id: codetracer_trace_types::TypeId(q.get_type_id()?.get_i().try_into().unwrap()),
        }),
        Ok(trace::value_record::Which::String(q)) => Ok(codetracer_trace_types::ValueRecord::String {
            text: q.get_text()?.to_string()?,
            type_id: codetracer_trace_types::TypeId(q.get_type_id()?.get_i().try_into().unwrap()),
        }),
        Ok(trace::value_record::Which::Sequence(q)) => Ok(codetracer_trace_types::ValueRecord::Sequence {
            elements: get_value_records(q.get_elements()?)?,
            is_slice: q.get_is_slice(),
            type_id: codetracer_trace_types::TypeId(q.get_type_id()?.get_i().try_into().unwrap()),
        }),
        Ok(trace::value_record::Which::Tuple(q)) => Ok(codetracer_trace_types::ValueRecord::Tuple {
            elements: get_value_records(q.get_elements()?)?,
            type_id: codetracer_trace_types::TypeId(q.get_type_id()?.get_i().try_into().unwrap()),
        }),
        Ok(trace::value_record::Which::Struct(q)) => Ok(codetracer_trace_types::ValueRecord::Struct {
            field_values: get_value_records(q.get_field_values()?)?,
            type_id: codetracer_trace_types::TypeId(q.get_type_id()?.get_i().try_into().unwrap()),
        }),
        Ok(trace::value_record::Which::Variant(q)) => Ok(codetracer_trace_types::ValueRecord::Variant {
            discriminator: q.get_discriminator()?.to_string()?,
            contents: Box::new(get_value_record(q.get_contents()?)?),
            type_id: codetracer_trace_types::TypeId(q.get_type_id()?.get_i().try_into().unwrap()),
        }),
        Ok(trace::value_record::Which::Reference(q)) => Ok(codetracer_trace_types::ValueRecord::Reference {
            dereferenced: Box::new(get_value_record(q.get_dereferenced()?)?),
            address: q.get_address(),
            mutable: q.get_mutable(),
            type_id: codetracer_trace_types::TypeId(q.get_type_id()?.get_i().try_into().unwrap()),
        }),
        Ok(trace::value_record::Which::Raw(q)) => Ok(codetracer_trace_types::ValueRecord::Raw {
            r: q.get_r()?.to_string()?,
            type_id: codetracer_trace_types::TypeId(q.get_type_id()?.get_i().try_into().unwrap()),
        }),
        Ok(trace::value_record::Which::Error(q)) => Ok(codetracer_trace_types::ValueRecord::Error {
            msg: q.get_msg()?.to_string()?,
            type_id: codetracer_trace_types::TypeId(q.get_type_id()?.get_i().try_into().unwrap()),
        }),
        Ok(trace::value_record::Which::None(q)) => Ok(codetracer_trace_types::ValueRecord::None {
            type_id: codetracer_trace_types::TypeId(q.get_type_id()?.get_i().try_into().unwrap()),
        }),
        Ok(trace::value_record::Which::Cell(q)) => Ok(codetracer_trace_types::ValueRecord::Cell {
            place: codetracer_trace_types::Place(q.get_place()?.get_p()),
        }),
        Ok(trace::value_record::Which::Bigint(q)) => Ok(codetracer_trace_types::ValueRecord::BigInt {
            b: q.get_b()?.as_slice().unwrap().to_vec(),
            negative: q.get_negative(),
            type_id: codetracer_trace_types::TypeId(q.get_type_id()?.get_i().try_into().unwrap()),
        }),
        Err(_) => panic!(),
    }
}

fn get_full_value_record(r: trace::full_value_record::Reader) -> Result<codetracer_trace_types::FullValueRecord, capnp::Error> {
    Ok(codetracer_trace_types::FullValueRecord {
        variable_id: codetracer_trace_types::VariableId(r.get_variable_id()?.get_i().try_into().unwrap()),
        value: get_value_record(r.get_value()?)?,
    })
}

pub fn read_trace(input: &mut impl std::io::BufRead) -> ::capnp::Result<Vec<codetracer_trace_types::TraceLowLevelEvent>> {
    let mut header_buf = [0; 8];
    input.read_exact(&mut header_buf)?;
    if header_buf != HEADER {
        panic!("Invalid file header (wrong file format or incompatible version)");
    }
    // The default traversal limit (8 M words = 64 MB) is too small for
    // traces from real-world programs (e.g. the Python recorder can produce
    // 96 MB+ capnproto files with ~100 K events).  Disable the limit so
    // that arbitrarily large traces can be loaded.
    let mut reader_opts = ::capnp::message::ReaderOptions::new();
    reader_opts.traversal_limit_in_words(None);
    let message_reader = serialize_packed::read_message(input, reader_opts)?;

    let trace = message_reader.get_root::<trace::Reader>()?;

    let mut res: Vec<codetracer_trace_types::TraceLowLevelEvent> = Vec::with_capacity(trace.get_events()?.len().try_into().unwrap());

    for event in trace.get_events()? {
        let q = match event.which() {
            Ok(trace::trace_low_level_event::Which::Step(step_record)) => {
                let step_record = step_record?;
                TraceLowLevelEvent::Step(codetracer_trace_types::StepRecord {
                    path_id: codetracer_trace_types::PathId(step_record.get_path_id()?.get_i().try_into().unwrap()),
                    line: codetracer_trace_types::Line(step_record.get_line()?.get_l()),
                })
            }
            Ok(trace::trace_low_level_event::Which::Path(path_buf)) => {
                TraceLowLevelEvent::Path(std::path::PathBuf::from_str(path_buf?.get_p()?.to_str()?).unwrap())
            }
            Ok(trace::trace_low_level_event::Which::VariableName(variable_name)) => TraceLowLevelEvent::VariableName(variable_name?.to_string()?),
            Ok(trace::trace_low_level_event::Which::Variable(variable)) => TraceLowLevelEvent::Variable(variable?.to_string()?),
            Ok(trace::trace_low_level_event::Which::Type(type_record)) => {
                let type_record = type_record?;
                TraceLowLevelEvent::Type(codetracer_trace_types::TypeRecord {
                    kind: type_record.get_kind()?.into(),
                    lang_type: type_record.get_lang_type()?.to_string()?,
                    specific_info: match type_record.get_specific_info()?.which() {
                        Ok(trace::type_specific_info::Which::None(())) => codetracer_trace_types::TypeSpecificInfo::None,
                        Ok(trace::type_specific_info::Which::Struct(s)) => {
                            let s_fields = s.get_fields()?;
                            let mut fields: Vec<codetracer_trace_types::FieldTypeRecord> = Vec::with_capacity(s_fields.len().try_into().unwrap());
                            for s_field in s_fields {
                                fields.push(codetracer_trace_types::FieldTypeRecord {
                                    name: s_field.get_name()?.to_string()?,
                                    type_id: codetracer_trace_types::TypeId(s_field.get_type_id()?.get_i().try_into().unwrap()),
                                });
                            }
                            codetracer_trace_types::TypeSpecificInfo::Struct { fields }
                        }
                        Ok(trace::type_specific_info::Which::Pointer(p)) => codetracer_trace_types::TypeSpecificInfo::Pointer {
                            dereference_type_id: codetracer_trace_types::TypeId(p.get_dereference_type_id()?.get_i().try_into().unwrap()),
                        },
                        Err(_) => {
                            panic!()
                        }
                    },
                })
            }
            Ok(trace::trace_low_level_event::Which::Value(fvr)) => TraceLowLevelEvent::Value(get_full_value_record(fvr?)?),
            Ok(trace::trace_low_level_event::Which::Function(function_record)) => {
                let function_record = function_record?;
                TraceLowLevelEvent::Function(codetracer_trace_types::FunctionRecord {
                    path_id: codetracer_trace_types::PathId(function_record.get_path_id()?.get_i().try_into().unwrap()),
                    line: codetracer_trace_types::Line(function_record.get_line()?.get_l()),
                    name: function_record.get_name()?.to_string()?,
                })
            }
            Ok(trace::trace_low_level_event::Which::Call(call_record)) => {
                let call_record = call_record?;
                let sargs = call_record.get_args()?;
                let mut args: Vec<codetracer_trace_types::FullValueRecord> = Vec::with_capacity(sargs.len().try_into().unwrap());
                for sarg in sargs {
                    args.push(codetracer_trace_types::FullValueRecord {
                        variable_id: codetracer_trace_types::VariableId(sarg.get_variable_id()?.get_i().try_into().unwrap()),
                        value: get_value_record(sarg.get_value()?)?,
                    });
                }
                TraceLowLevelEvent::Call(codetracer_trace_types::CallRecord {
                    function_id: codetracer_trace_types::FunctionId(call_record.get_function_id()?.get_i().try_into().unwrap()),
                    args,
                })
            }
            Ok(trace::trace_low_level_event::Which::Return(return_record)) => TraceLowLevelEvent::Return(codetracer_trace_types::ReturnRecord {
                return_value: get_value_record(return_record?.get_return_value()?)?,
            }),
            Ok(trace::trace_low_level_event::Which::Event(record_event)) => {
                let record_event = record_event?;
                TraceLowLevelEvent::Event(codetracer_trace_types::RecordEvent {
                    kind: record_event.get_kind()?.into(),
                    metadata: record_event.get_metadata()?.to_string()?,
                    content: record_event.get_content()?.to_string()?,
                })
            }
            Ok(trace::trace_low_level_event::Which::Asm(asm_strings)) => {
                let asm_strings = asm_strings?;
                let mut strs: Vec<String> = Vec::with_capacity(asm_strings.len().try_into().unwrap());
                for s in asm_strings {
                    strs.push(s?.to_string()?);
                }
                TraceLowLevelEvent::Asm(strs)
            }
            Ok(trace::trace_low_level_event::Which::BindVariable(bind_variable_record)) => {
                let bind_variable_record = bind_variable_record?;
                TraceLowLevelEvent::BindVariable(codetracer_trace_types::BindVariableRecord {
                    variable_id: codetracer_trace_types::VariableId(bind_variable_record.get_variable_id()?.get_i().try_into().unwrap()),
                    place: codetracer_trace_types::Place(bind_variable_record.get_place()?.get_p()),
                })
            }
            Ok(trace::trace_low_level_event::Which::Assignment(assignment_record)) => {
                let assignment_record = assignment_record?;
                TraceLowLevelEvent::Assignment(codetracer_trace_types::AssignmentRecord {
                    to: codetracer_trace_types::VariableId(assignment_record.get_to()?.get_i().try_into().unwrap()),
                    pass_by: match assignment_record.get_pass_by()? {
                        trace::PassBy::Value => codetracer_trace_types::PassBy::Value,
                        trace::PassBy::Reference => codetracer_trace_types::PassBy::Reference,
                    },
                    from: match assignment_record.get_from()?.which()? {
                        trace::r_value::Which::Simple(variable_id) => {
                            codetracer_trace_types::RValue::Simple(codetracer_trace_types::VariableId(variable_id?.get_i().try_into().unwrap()))
                        }
                        trace::r_value::Which::Compound(variables) => {
                            let variables = variables?;
                            let mut v: Vec<VariableId> = Vec::with_capacity(variables.len().try_into().unwrap());
                            for vv in variables {
                                v.push(codetracer_trace_types::VariableId(vv.get_i().try_into().unwrap()));
                            }
                            codetracer_trace_types::RValue::Compound(v)
                        }
                    },
                })
            }
            Ok(trace::trace_low_level_event::Which::DropVariables(variables)) => {
                let variables = variables?;
                let mut v: Vec<codetracer_trace_types::VariableId> = Vec::with_capacity(variables.len().try_into().unwrap());
                for vv in variables {
                    v.push(codetracer_trace_types::VariableId(vv.get_i().try_into().unwrap()))
                }
                TraceLowLevelEvent::DropVariables(v)
            }
            Ok(trace::trace_low_level_event::Which::CompoundValue(compound_value_record)) => {
                let compound_value_record = compound_value_record?;
                TraceLowLevelEvent::CompoundValue(codetracer_trace_types::CompoundValueRecord {
                    place: codetracer_trace_types::Place(compound_value_record.get_place()?.get_p()),
                    value: get_value_record(compound_value_record.get_value()?)?,
                })
            }
            Ok(trace::trace_low_level_event::Which::CellValue(cell_value_record)) => {
                let cell_value_record = cell_value_record?;
                TraceLowLevelEvent::CellValue(codetracer_trace_types::CellValueRecord {
                    place: codetracer_trace_types::Place(cell_value_record.get_place()?.get_p()),
                    value: get_value_record(cell_value_record.get_value()?)?,
                })
            }
            Ok(trace::trace_low_level_event::Which::AssignCompoundItem(assign_compound_item_record)) => {
                let assign_compound_item_record = assign_compound_item_record?;
                TraceLowLevelEvent::AssignCompoundItem(codetracer_trace_types::AssignCompoundItemRecord {
                    place: codetracer_trace_types::Place(assign_compound_item_record.get_place()?.get_p()),
                    index: assign_compound_item_record.get_index().try_into().unwrap(),
                    item_place: codetracer_trace_types::Place(assign_compound_item_record.get_item_place()?.get_p()),
                })
            }
            Ok(trace::trace_low_level_event::Which::AssignCell(assign_cell_record)) => {
                let assign_cell_record = assign_cell_record?;
                TraceLowLevelEvent::AssignCell(codetracer_trace_types::AssignCellRecord {
                    place: codetracer_trace_types::Place(assign_cell_record.get_place()?.get_p()),
                    new_value: get_value_record(assign_cell_record.get_new_value()?)?,
                })
            }
            Ok(trace::trace_low_level_event::Which::VariableCell(variable_cell_record)) => {
                let variable_cell_record = variable_cell_record?;
                TraceLowLevelEvent::VariableCell(codetracer_trace_types::VariableCellRecord {
                    variable_id: codetracer_trace_types::VariableId(variable_cell_record.get_variable_id()?.get_i().try_into().unwrap()),
                    place: codetracer_trace_types::Place(variable_cell_record.get_place()?.get_p()),
                })
            }
            Ok(trace::trace_low_level_event::Which::DropVariable(variable_id)) => {
                TraceLowLevelEvent::DropVariable(codetracer_trace_types::VariableId(variable_id?.get_i().try_into().unwrap()))
            }
            Ok(trace::trace_low_level_event::Which::ThreadStart(thread_id)) => {
                TraceLowLevelEvent::ThreadStart(codetracer_trace_types::ThreadId(thread_id?.get_i()))
            }
            Ok(trace::trace_low_level_event::Which::ThreadExit(thread_id)) => {
                TraceLowLevelEvent::ThreadExit(codetracer_trace_types::ThreadId(thread_id?.get_i()))
            }
            Ok(trace::trace_low_level_event::Which::ThreadSwitch(thread_id)) => {
                TraceLowLevelEvent::ThreadSwitch(codetracer_trace_types::ThreadId(thread_id?.get_i()))
            }
            Ok(trace::trace_low_level_event::Which::DropLastStep(())) => TraceLowLevelEvent::DropLastStep,
            Err(_) => {
                panic!()
            }
        };
        res.push(q);
    }

    Ok(res)
}
