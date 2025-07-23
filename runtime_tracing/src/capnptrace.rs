use crate::trace_capnp::trace;
use crate::{TraceLowLevelEvent, VariableId};
use capnp::serialize_packed;
use std::str::FromStr;

/// The first 5 bytes identify the file as a CodeTracer file (hex l33tsp33k - C0DE72ACE2 for "CodeTracer").
/// The next 3 bytes are reserved/version info. In the initial version, they are zero. Non-zero values might
/// indicate incompatible future versions.
/// The header is 8 bytes in size, ensuring 64-bit alignment for the rest of the file.
pub const HEADER: &[u8] = &[0xC0, 0xDE, 0x72, 0xAC, 0xE2, 0x00, 0x00, 0x00];

impl From<crate::TypeKind> for trace::TypeKind {
    fn from(item: crate::TypeKind) -> Self {
        match item {
            crate::TypeKind::Seq => trace::TypeKind::Seq,
            crate::TypeKind::Set => trace::TypeKind::Set,
            crate::TypeKind::HashSet => trace::TypeKind::HashSet,
            crate::TypeKind::OrderedSet => trace::TypeKind::OrderedSet,
            crate::TypeKind::Array => trace::TypeKind::Array,
            crate::TypeKind::Varargs => trace::TypeKind::Varargs,
            crate::TypeKind::Struct => trace::TypeKind::Struct,
            crate::TypeKind::Int => trace::TypeKind::Int,
            crate::TypeKind::Float => trace::TypeKind::Float,
            crate::TypeKind::String => trace::TypeKind::String,
            crate::TypeKind::CString => trace::TypeKind::Cstring,
            crate::TypeKind::Char => trace::TypeKind::Char,
            crate::TypeKind::Bool => trace::TypeKind::Bool,
            crate::TypeKind::Literal => trace::TypeKind::Literal,
            crate::TypeKind::Ref => trace::TypeKind::Ref,
            crate::TypeKind::Recursion => trace::TypeKind::Recursion,
            crate::TypeKind::Raw => trace::TypeKind::Raw,
            crate::TypeKind::Enum => trace::TypeKind::Enum,
            crate::TypeKind::Enum16 => trace::TypeKind::Enum16,
            crate::TypeKind::Enum32 => trace::TypeKind::Enum32,
            crate::TypeKind::C => trace::TypeKind::C,
            crate::TypeKind::TableKind => trace::TypeKind::TableKind,
            crate::TypeKind::Union => trace::TypeKind::Union,
            crate::TypeKind::Pointer => trace::TypeKind::Pointer,
            crate::TypeKind::Error => trace::TypeKind::Error,
            crate::TypeKind::FunctionKind => trace::TypeKind::FunctionKind,
            crate::TypeKind::TypeValue => trace::TypeKind::TypeValue,
            crate::TypeKind::Tuple => trace::TypeKind::Tuple,
            crate::TypeKind::Variant => trace::TypeKind::Variant,
            crate::TypeKind::Html => trace::TypeKind::Html,
            crate::TypeKind::None => trace::TypeKind::None,
            crate::TypeKind::NonExpanded => trace::TypeKind::NonExpanded,
            crate::TypeKind::Any => trace::TypeKind::Any,
            crate::TypeKind::Slice => trace::TypeKind::Slice,
        }
    }
}

impl From<trace::TypeKind> for crate::TypeKind {
    fn from(item: trace::TypeKind) -> Self {
        match item {
            trace::TypeKind::Seq => crate::TypeKind::Seq,
            trace::TypeKind::Set => crate::TypeKind::Set,
            trace::TypeKind::HashSet => crate::TypeKind::HashSet,
            trace::TypeKind::OrderedSet => crate::TypeKind::OrderedSet,
            trace::TypeKind::Array => crate::TypeKind::Array,
            trace::TypeKind::Varargs => crate::TypeKind::Varargs,
            trace::TypeKind::Struct => crate::TypeKind::Struct,
            trace::TypeKind::Int => crate::TypeKind::Int,
            trace::TypeKind::Float => crate::TypeKind::Float,
            trace::TypeKind::String => crate::TypeKind::String,
            trace::TypeKind::Cstring => crate::TypeKind::CString,
            trace::TypeKind::Char => crate::TypeKind::Char,
            trace::TypeKind::Bool => crate::TypeKind::Bool,
            trace::TypeKind::Literal => crate::TypeKind::Literal,
            trace::TypeKind::Ref => crate::TypeKind::Ref,
            trace::TypeKind::Recursion => crate::TypeKind::Recursion,
            trace::TypeKind::Raw => crate::TypeKind::Raw,
            trace::TypeKind::Enum => crate::TypeKind::Enum,
            trace::TypeKind::Enum16 => crate::TypeKind::Enum16,
            trace::TypeKind::Enum32 => crate::TypeKind::Enum32,
            trace::TypeKind::C => crate::TypeKind::C,
            trace::TypeKind::TableKind => crate::TypeKind::TableKind,
            trace::TypeKind::Union => crate::TypeKind::Union,
            trace::TypeKind::Pointer => crate::TypeKind::Pointer,
            trace::TypeKind::Error => crate::TypeKind::Error,
            trace::TypeKind::FunctionKind => crate::TypeKind::FunctionKind,
            trace::TypeKind::TypeValue => crate::TypeKind::TypeValue,
            trace::TypeKind::Tuple => crate::TypeKind::Tuple,
            trace::TypeKind::Variant => crate::TypeKind::Variant,
            trace::TypeKind::Html => crate::TypeKind::Html,
            trace::TypeKind::None => crate::TypeKind::None,
            trace::TypeKind::NonExpanded => crate::TypeKind::NonExpanded,
            trace::TypeKind::Any => crate::TypeKind::Any,
            trace::TypeKind::Slice => crate::TypeKind::Slice,
        }
    }
}

impl From<trace::EventLogKind> for crate::EventLogKind {
    fn from(value: trace::EventLogKind) -> Self {
        match value {
            trace::EventLogKind::Write => crate::EventLogKind::Write,
            trace::EventLogKind::WriteFile => crate::EventLogKind::WriteFile,
            trace::EventLogKind::WriteOther => crate::EventLogKind::WriteOther,
            trace::EventLogKind::Read => crate::EventLogKind::Read,
            trace::EventLogKind::ReadFile => crate::EventLogKind::ReadFile,
            trace::EventLogKind::ReadOther => crate::EventLogKind::ReadOther,
            trace::EventLogKind::ReadDir => crate::EventLogKind::ReadDir,
            trace::EventLogKind::OpenDir => crate::EventLogKind::OpenDir,
            trace::EventLogKind::CloseDir => crate::EventLogKind::CloseDir,
            trace::EventLogKind::Socket => crate::EventLogKind::Socket,
            trace::EventLogKind::Open => crate::EventLogKind::Open,
            trace::EventLogKind::Error => crate::EventLogKind::Error,
            trace::EventLogKind::TraceLogEvent => crate::EventLogKind::TraceLogEvent,
            trace::EventLogKind::EvmEvent => crate::EventLogKind::EvmEvent,
        }
    }
}

impl From<crate::EventLogKind> for trace::EventLogKind {
    fn from(value: crate::EventLogKind) -> Self {
        match value {
            crate::EventLogKind::Write => trace::EventLogKind::Write,
            crate::EventLogKind::WriteFile => trace::EventLogKind::WriteFile,
            crate::EventLogKind::WriteOther => trace::EventLogKind::WriteOther,
            crate::EventLogKind::Read => trace::EventLogKind::Read,
            crate::EventLogKind::ReadFile => trace::EventLogKind::ReadFile,
            crate::EventLogKind::ReadOther => trace::EventLogKind::ReadOther,
            crate::EventLogKind::ReadDir => trace::EventLogKind::ReadDir,
            crate::EventLogKind::OpenDir => trace::EventLogKind::OpenDir,
            crate::EventLogKind::CloseDir => trace::EventLogKind::CloseDir,
            crate::EventLogKind::Socket => trace::EventLogKind::Socket,
            crate::EventLogKind::Open => trace::EventLogKind::Open,
            crate::EventLogKind::Error => trace::EventLogKind::Error,
            crate::EventLogKind::TraceLogEvent => trace::EventLogKind::TraceLogEvent,
            crate::EventLogKind::EvmEvent => trace::EventLogKind::EvmEvent,
        }
    }
}

impl From<crate::PassBy> for trace::PassBy {
    fn from(value: crate::PassBy) -> Self {
        match value {
            crate::PassBy::Value => trace::PassBy::Value,
            crate::PassBy::Reference => trace::PassBy::Reference,
        }
    }
}

fn conv_valuerecord(bldr: crate::trace_capnp::trace::value_record::Builder, vr: &crate::ValueRecord) {
    match vr {
        crate::ValueRecord::Int { i, type_id } => {
            let mut qi = bldr.init_int();
            qi.set_i(*i);
            let mut q_typ_id = qi.init_type_id();
            q_typ_id.set_i(type_id.0.try_into().unwrap());
        }
        crate::ValueRecord::Float { f, type_id } => {
            let mut qf = bldr.init_float();
            qf.set_f(*f);
            let mut q_typ_id = qf.init_type_id();
            q_typ_id.set_i(type_id.0.try_into().unwrap());
        }
        crate::ValueRecord::Bool { b, type_id } => {
            let mut qb = bldr.init_bool();
            qb.set_b(*b);
            let mut q_typ_id = qb.init_type_id();
            q_typ_id.set_i(type_id.0.try_into().unwrap());
        }
        crate::ValueRecord::String { text, type_id } => {
            let mut qs = bldr.init_string();
            qs.set_text(text);
            let mut q_typ_id = qs.init_type_id();
            q_typ_id.set_i(type_id.0.try_into().unwrap());
        }
        crate::ValueRecord::Sequence { elements, is_slice, type_id } => {
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
        crate::ValueRecord::Tuple { elements, type_id } => {
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
        crate::ValueRecord::Struct { field_values, type_id } => {
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
        crate::ValueRecord::Variant {
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
        crate::ValueRecord::Reference {
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
        crate::ValueRecord::Raw { r, type_id } => {
            let mut qraw = bldr.init_raw();
            qraw.set_r(r);
            let mut q_typ_id = qraw.init_type_id();
            q_typ_id.set_i(type_id.0.try_into().unwrap());
        }
        crate::ValueRecord::Error { msg, type_id } => {
            let mut qerr = bldr.init_error();
            qerr.set_msg(msg);
            let mut q_typ_id = qerr.init_type_id();
            q_typ_id.set_i(type_id.0.try_into().unwrap());
        }
        crate::ValueRecord::None { type_id } => {
            let qnone = bldr.init_none();
            let mut q_typ_id = qnone.init_type_id();
            q_typ_id.set_i(type_id.0.try_into().unwrap());
        }
        crate::ValueRecord::Cell { place } => {
            let qcell = bldr.init_cell();
            let mut q_place = qcell.init_place();
            q_place.set_p(place.0);
        }
        crate::ValueRecord::BigInt { b, negative, type_id } => {
            let mut qbigint = bldr.init_bigint();
            let mut bigint_b = qbigint.reborrow().init_b(b.len().try_into().unwrap());
            for i in 0..=b.len() {
                bigint_b.set(i.try_into().unwrap(), b[i]);
            }
            qbigint.set_negative(*negative);
            let mut q_typ_id = qbigint.init_type_id();
            q_typ_id.set_i(type_id.0.try_into().unwrap());
        }
    }
}

pub fn write_trace(q: &[crate::TraceLowLevelEvent], output: &mut impl std::io::Write) -> ::capnp::Result<()> {
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
                    crate::TypeSpecificInfo::None => {
                        specific_info.set_none(());
                    }
                    crate::TypeSpecificInfo::Struct { fields } => {
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
                    crate::TypeSpecificInfo::Pointer { dereference_type_id } => {
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
                    crate::RValue::Simple(variable_id) => {
                        let mut ret_from_simple = ret_from.init_simple();
                        ret_from_simple.set_i(variable_id.0.try_into().unwrap());
                    }
                    crate::RValue::Compound(variable_ids) => {
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
        }
    }

    output.write_all(HEADER)?;

    serialize_packed::write_message(output, &message)
}

fn get_value_records(r: capnp::struct_list::Reader<trace::value_record::Owned>) -> Result<Vec<crate::ValueRecord>, capnp::Error> {
    let mut res: Vec<crate::ValueRecord> = Vec::with_capacity(r.len().try_into().unwrap());
    for i in 0..r.len() {
        res.push(get_value_record(r.get(i))?);
    }
    Ok(res)
}

fn get_value_record(r: trace::value_record::Reader) -> Result<crate::ValueRecord, capnp::Error> {
    match r.which() {
        Ok(trace::value_record::Which::Int(q)) => Ok(crate::ValueRecord::Int {
            i: q.get_i(),
            type_id: crate::TypeId(q.get_type_id()?.get_i().try_into().unwrap()),
        }),
        Ok(trace::value_record::Which::Float(q)) => Ok(crate::ValueRecord::Float {
            f: q.get_f(),
            type_id: crate::TypeId(q.get_type_id()?.get_i().try_into().unwrap()),
        }),
        Ok(trace::value_record::Which::Bool(q)) => Ok(crate::ValueRecord::Bool {
            b: q.get_b(),
            type_id: crate::TypeId(q.get_type_id()?.get_i().try_into().unwrap()),
        }),
        Ok(trace::value_record::Which::String(q)) => Ok(crate::ValueRecord::String {
            text: q.get_text()?.to_string()?,
            type_id: crate::TypeId(q.get_type_id()?.get_i().try_into().unwrap()),
        }),
        Ok(trace::value_record::Which::Sequence(q)) => Ok(crate::ValueRecord::Sequence {
            elements: get_value_records(q.get_elements()?)?,
            is_slice: q.get_is_slice(),
            type_id: crate::TypeId(q.get_type_id()?.get_i().try_into().unwrap()),
        }),
        Ok(trace::value_record::Which::Tuple(q)) => Ok(crate::ValueRecord::Tuple {
            elements: get_value_records(q.get_elements()?)?,
            type_id: crate::TypeId(q.get_type_id()?.get_i().try_into().unwrap()),
        }),
        Ok(trace::value_record::Which::Struct(q)) => Ok(crate::ValueRecord::Struct {
            field_values: get_value_records(q.get_field_values()?)?,
            type_id: crate::TypeId(q.get_type_id()?.get_i().try_into().unwrap()),
        }),
        Ok(trace::value_record::Which::Variant(q)) => Ok(crate::ValueRecord::Variant {
            discriminator: q.get_discriminator()?.to_string()?,
            contents: Box::new(get_value_record(q.get_contents()?)?),
            type_id: crate::TypeId(q.get_type_id()?.get_i().try_into().unwrap()),
        }),
        Ok(trace::value_record::Which::Reference(q)) => Ok(crate::ValueRecord::Reference {
            dereferenced: Box::new(get_value_record(q.get_dereferenced()?)?),
            address: q.get_address(),
            mutable: q.get_mutable(),
            type_id: crate::TypeId(q.get_type_id()?.get_i().try_into().unwrap()),
        }),
        Ok(trace::value_record::Which::Raw(q)) => Ok(crate::ValueRecord::Raw {
            r: q.get_r()?.to_string()?,
            type_id: crate::TypeId(q.get_type_id()?.get_i().try_into().unwrap()),
        }),
        Ok(trace::value_record::Which::Error(q)) => Ok(crate::ValueRecord::Error {
            msg: q.get_msg()?.to_string()?,
            type_id: crate::TypeId(q.get_type_id()?.get_i().try_into().unwrap()),
        }),
        Ok(trace::value_record::Which::None(q)) => Ok(crate::ValueRecord::None {
            type_id: crate::TypeId(q.get_type_id()?.get_i().try_into().unwrap()),
        }),
        Ok(trace::value_record::Which::Cell(q)) => Ok(crate::ValueRecord::Cell {
            place: crate::Place(q.get_place()?.get_p()),
        }),
        Ok(trace::value_record::Which::Bigint(q)) => Ok(crate::ValueRecord::BigInt {
            b: q.get_b()?.as_slice().unwrap().to_vec(),
            negative: q.get_negative(),
            type_id: crate::TypeId(q.get_type_id()?.get_i().try_into().unwrap()),
        }),
        Err(_) => panic!(),
    }
}

fn get_full_value_record(r: trace::full_value_record::Reader) -> Result<crate::FullValueRecord, capnp::Error> {
    Ok(crate::FullValueRecord {
        variable_id: crate::VariableId(r.get_variable_id()?.get_i().try_into().unwrap()),
        value: get_value_record(r.get_value()?)?,
    })
}

pub fn read_trace(input: &mut impl std::io::BufRead) -> ::capnp::Result<Vec<crate::TraceLowLevelEvent>> {
    let mut header_buf = [0; 8];
    input.read_exact(&mut header_buf)?;
    if header_buf != HEADER {
        panic!("Invalid file header (wrong file format or incompatible version)");
    }
    let message_reader = serialize_packed::read_message(input, ::capnp::message::ReaderOptions::new())?;

    let trace = message_reader.get_root::<trace::Reader>()?;

    let mut res: Vec<crate::TraceLowLevelEvent> = Vec::with_capacity(trace.get_events()?.len().try_into().unwrap());

    for event in trace.get_events()? {
        let q = match event.which() {
            Ok(trace::trace_low_level_event::Which::Step(step_record)) => {
                let step_record = step_record?;
                TraceLowLevelEvent::Step(crate::StepRecord {
                    path_id: crate::PathId(step_record.get_path_id()?.get_i().try_into().unwrap()),
                    line: crate::Line(step_record.get_line()?.get_l()),
                })
            }
            Ok(trace::trace_low_level_event::Which::Path(path_buf)) => {
                TraceLowLevelEvent::Path(std::path::PathBuf::from_str(path_buf?.get_p()?.to_str()?).unwrap())
            }
            Ok(trace::trace_low_level_event::Which::VariableName(variable_name)) => TraceLowLevelEvent::VariableName(variable_name?.to_string()?),
            Ok(trace::trace_low_level_event::Which::Variable(variable)) => TraceLowLevelEvent::Variable(variable?.to_string()?),
            Ok(trace::trace_low_level_event::Which::Type(type_record)) => {
                let type_record = type_record?;
                TraceLowLevelEvent::Type(crate::TypeRecord {
                    kind: type_record.get_kind()?.into(),
                    lang_type: type_record.get_lang_type()?.to_string()?,
                    specific_info: match type_record.get_specific_info()?.which() {
                        Ok(trace::type_specific_info::Which::None(())) => crate::TypeSpecificInfo::None,
                        Ok(trace::type_specific_info::Which::Struct(s)) => {
                            let s_fields = s.get_fields()?;
                            let mut fields: Vec<crate::FieldTypeRecord> = Vec::with_capacity(s_fields.len().try_into().unwrap());
                            for s_field in s_fields {
                                fields.push(crate::FieldTypeRecord {
                                    name: s_field.get_name()?.to_string()?,
                                    type_id: crate::TypeId(s_field.get_type_id()?.get_i().try_into().unwrap()),
                                });
                            }
                            crate::TypeSpecificInfo::Struct { fields }
                        }
                        Ok(trace::type_specific_info::Which::Pointer(p)) => crate::TypeSpecificInfo::Pointer {
                            dereference_type_id: crate::TypeId(p.get_dereference_type_id()?.get_i().try_into().unwrap()),
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
                TraceLowLevelEvent::Function(crate::FunctionRecord {
                    path_id: crate::PathId(function_record.get_path_id()?.get_i().try_into().unwrap()),
                    line: crate::Line(function_record.get_line()?.get_l()),
                    name: function_record.get_name()?.to_string()?,
                })
            }
            Ok(trace::trace_low_level_event::Which::Call(call_record)) => {
                let call_record = call_record?;
                let sargs = call_record.get_args()?;
                let mut args: Vec<crate::FullValueRecord> = Vec::with_capacity(sargs.len().try_into().unwrap());
                for sarg in sargs {
                    args.push(crate::FullValueRecord {
                        variable_id: crate::VariableId(sarg.get_variable_id()?.get_i().try_into().unwrap()),
                        value: get_value_record(sarg.get_value()?)?,
                    });
                }
                TraceLowLevelEvent::Call(crate::CallRecord {
                    function_id: crate::FunctionId(call_record.get_function_id()?.get_i().try_into().unwrap()),
                    args,
                })
            }
            Ok(trace::trace_low_level_event::Which::Return(return_record)) => TraceLowLevelEvent::Return(crate::ReturnRecord {
                return_value: get_value_record(return_record?.get_return_value()?)?,
            }),
            Ok(trace::trace_low_level_event::Which::Event(record_event)) => {
                let record_event = record_event?;
                TraceLowLevelEvent::Event(crate::RecordEvent {
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
                TraceLowLevelEvent::BindVariable(crate::BindVariableRecord {
                    variable_id: crate::VariableId(bind_variable_record.get_variable_id()?.get_i().try_into().unwrap()),
                    place: crate::Place(bind_variable_record.get_place()?.get_p()),
                })
            }
            Ok(trace::trace_low_level_event::Which::Assignment(assignment_record)) => {
                let assignment_record = assignment_record?;
                TraceLowLevelEvent::Assignment(crate::AssignmentRecord {
                    to: crate::VariableId(assignment_record.get_to()?.get_i().try_into().unwrap()),
                    pass_by: match assignment_record.get_pass_by()? {
                        trace::PassBy::Value => crate::PassBy::Value,
                        trace::PassBy::Reference => crate::PassBy::Reference,
                    },
                    from: match assignment_record.get_from()?.which()? {
                        trace::r_value::Which::Simple(variable_id) => {
                            crate::RValue::Simple(crate::VariableId(variable_id?.get_i().try_into().unwrap()))
                        }
                        trace::r_value::Which::Compound(variables) => {
                            let variables = variables?;
                            let mut v: Vec<VariableId> = Vec::with_capacity(variables.len().try_into().unwrap());
                            for vv in variables {
                                v.push(crate::VariableId(vv.get_i().try_into().unwrap()));
                            }
                            crate::RValue::Compound(v)
                        }
                    },
                })
            }
            Ok(trace::trace_low_level_event::Which::DropVariables(variables)) => {
                let variables = variables?;
                let mut v: Vec<crate::VariableId> = Vec::with_capacity(variables.len().try_into().unwrap());
                for vv in variables {
                    v.push(crate::VariableId(vv.get_i().try_into().unwrap()))
                }
                TraceLowLevelEvent::DropVariables(v)
            }
            Ok(trace::trace_low_level_event::Which::CompoundValue(compound_value_record)) => {
                let compound_value_record = compound_value_record?;
                TraceLowLevelEvent::CompoundValue(crate::CompoundValueRecord {
                    place: crate::Place(compound_value_record.get_place()?.get_p()),
                    value: get_value_record(compound_value_record.get_value()?)?,
                })
            }
            Ok(trace::trace_low_level_event::Which::CellValue(cell_value_record)) => {
                let cell_value_record = cell_value_record?;
                TraceLowLevelEvent::CellValue(crate::CellValueRecord {
                    place: crate::Place(cell_value_record.get_place()?.get_p()),
                    value: get_value_record(cell_value_record.get_value()?)?,
                })
            }
            Ok(trace::trace_low_level_event::Which::AssignCompoundItem(assign_compound_item_record)) => {
                let assign_compound_item_record = assign_compound_item_record?;
                TraceLowLevelEvent::AssignCompoundItem(crate::AssignCompoundItemRecord {
                    place: crate::Place(assign_compound_item_record.get_place()?.get_p()),
                    index: assign_compound_item_record.get_index().try_into().unwrap(),
                    item_place: crate::Place(assign_compound_item_record.get_item_place()?.get_p()),
                })
            }
            Ok(trace::trace_low_level_event::Which::AssignCell(assign_cell_record)) => {
                let assign_cell_record = assign_cell_record?;
                TraceLowLevelEvent::AssignCell(crate::AssignCellRecord {
                    place: crate::Place(assign_cell_record.get_place()?.get_p()),
                    new_value: get_value_record(assign_cell_record.get_new_value()?)?,
                })
            }
            Ok(trace::trace_low_level_event::Which::VariableCell(variable_cell_record)) => {
                let variable_cell_record = variable_cell_record?;
                TraceLowLevelEvent::VariableCell(crate::VariableCellRecord {
                    variable_id: crate::VariableId(variable_cell_record.get_variable_id()?.get_i().try_into().unwrap()),
                    place: crate::Place(variable_cell_record.get_place()?.get_p()),
                })
            }
            Ok(trace::trace_low_level_event::Which::DropVariable(variable_id)) => {
                TraceLowLevelEvent::DropVariable(crate::VariableId(variable_id?.get_i().try_into().unwrap()))
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
