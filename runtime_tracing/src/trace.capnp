@0xef886fa1a670cf6f;

struct Trace {
    struct PathBuf {
        p @0 :Text;
    }

    events @0 :List(TraceLowLevelEvent);
    struct TraceLowLevelEvent {
        union {
            step @0 :StepRecord;
            path @1 :PathBuf;
            variableName @2 :Text;
            variable @3 :Text;
            type @4 :TypeRecord;
            value @5 :FullValueRecord;
            function @6 :FunctionRecord;
            call @7 :CallRecord;
            return @8 :ReturnRecord;
            event @9 :RecordEvent;
            asm @10 :List(Text);

            bindVariable @11 :BindVariableRecord;
            assignment @12 :AssignmentRecord;
            dropVariables @13 :List(VariableId);

            compoundValue @14 :CompoundValueRecord;
            cellValue @15 :CellValueRecord;
            assignCompoundItem @16 :AssignCompoundItemRecord;
            assignCell @17 :AssignCellRecord;
            variableCell @18 :VariableCellRecord;
            dropVariable @19 :VariableId;

            dropLastStep @20 :Void;
        }
    }

    struct BindVariableRecord {
        variableId @0 :VariableId;
        place @1 :Place;
    }

    enum PassBy {
        value @0;
        reference @1;
    }

    struct AssignmentRecord {
        to @0 :VariableId;
        passBy @1 :PassBy;
        from @2 :RValue;
    }

    struct RValue {
        union {
            simple @0 :VariableId;
            compound @1 :List(VariableId);
        }
    }

    struct CompoundValueRecord {
        place @0 :Place;
        value @1 :ValueRecord;
    }

    struct CellValueRecord {
        place @0 :Place;
        value @1 :ValueRecord;
    }

    struct AssignCompoundItemRecord {
        place @0 :Place;
        index @1 :Int64;
        itemPlace @2 :Place;
    }

    struct AssignCellRecord {
        place @0 :Place;
        newValue @1 :ValueRecord;
    }

    struct VariableCellRecord {
        variableId @0 :VariableId;
        place @1 :Place;
    }

    struct Place {
        p @0 :Int64;
    }

    struct FullValueRecord {
        variableId @0 :VariableId;
        value @1 :ValueRecord;
    }

    # TODO: TraceMetadata???

    # TODO: CallKey???

    struct Line {
        l @0 :Int64;
    }

    struct PathId {
        i @0 :Int64;
    }

    # TODO: StepId???

    struct VariableId {
        i @0 :Int64;
    }

    struct FunctionId {
        i @0 :Int64;
    }

    struct CallRecord {
        functionId @0 :FunctionId;
        args @1 :List(FullValueRecord);
    }

    struct ReturnRecord {
        returnValue @0 :ValueRecord;
    }

    struct FunctionRecord {
        pathId @0 :PathId;
        line @1 :Line;
        name @2 :Text;
    }

    struct StepRecord {
        pathId @0 :PathId;
        line @1 :Line;
    }

    # TODO: VariableRecord???

    struct TypeRecord {
        kind @0 :TypeKind;
        langType @1 :Text;
        specificInfo @2 :TypeSpecificInfo;
    }

    struct FieldTypeRecord {
        name @0 :Text;
        typeId @1 :TypeId;
    }

    struct TypeSpecificInfo {
        union {
            none @0 :Void;
            struct :group {
                fields @1 :List(FieldTypeRecord);
            }
            pointer :group {
                dereferenceTypeId @2 :TypeId;
            }
        }
    }

    struct RecordEvent {
        kind @0 :EventLogKind;
        metadata @2 :Text;
        content @1 :Text;
    }

    struct TypeId {
        i @0 :Int64;
    }

    struct ValueRecord {
        union {
            int :group {
                i @0 :Int64;
                typeId @1 :TypeId;
            }
            float :group {
                f @2 :Float64;
                typeId @3 :TypeId;
            }
            bool :group {
                b @4 :Bool;
                typeId @5 :TypeId;
            }
            string :group {
                text @6 :Text;
                typeId @7 :TypeId;
            }
            sequence :group {
                elements @8 :List(ValueRecord);
                isSlice @9 :Bool;
                typeId @10 :TypeId;
            }
            tuple :group {
                elements @11 :List(ValueRecord);
                typeId @12 :TypeId;
            }
            struct :group {
                fieldValues @13 :List(ValueRecord);
                typeId @14 :TypeId;
            }
            variant :group {
                discriminator @15 :Text;
                contents @16 :ValueRecord;  # Box?
                typeId @17 :TypeId;
            }
            reference :group {
                dereferenced @18 :ValueRecord;  # Box?
                address @27 :UInt64;
                mutable @19 :Bool;
                typeId @20 :TypeId;
            }
            raw :group {
                r @21 :Text;
                typeId @22 :TypeId;
            }
            error :group {
                msg @23 :Text;
                typeId @24 :TypeId;
            }
            none :group {
                typeId @25 :TypeId;
            }
            cell :group {
                place @26 :Place;
            }
            bigint :group {
                b @28 :List(UInt8);
                negative @29 :Bool;
                typeId @30 :TypeId;
            }
        }
    }

    enum TypeKind {
        seq @0;
        set @1;
        hashSet @2;
        orderedSet @3;
        array @4;
        varargs @5;

        struct @6;

        int @7;
        float @8;
        string @9;
        cstring @10;
        char @11;
        bool @12;

        literal @13;

        ref @14;

        recursion @15;

        raw @16;

        enum @17;
        enum16 @18;
        enum32 @19;

        c @20;

        tableKind @21;

        union @22;

        pointer @23;

        error @24;

        functionKind @25;

        typeValue @26;

        tuple @27;

        variant @28;

        html @29;

        none @30;
        nonExpanded @31;
        any @32;
        slice @33;
    }

    enum EventLogKind {
        write @0;
        writeFile @1;
        writeOther @11;
        read @2;
        readFile @3;
        readOther @12;
        readDir @4;
        openDir @5;
        closeDir @6;
        socket @7;
        open @8;
        error @9;
        traceLogEvent @10;
        evmEvemt @13;
    }
}
