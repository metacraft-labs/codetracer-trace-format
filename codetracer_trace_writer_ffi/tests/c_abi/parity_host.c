/*
 * One C host for the CodeTracer trace-format C ABI, compiled against the Nim
 * library's header (`codetracer-trace-format-nim/include/codetracer_trace_writer.h`)
 * and linked, separately, against each implementation of it.
 *
 * Each scenario drives a part of the ABI and prints a transcript: every
 * return value, and after every call whether `trace_writer_last_error` was
 * set by it (the buffer is cleared before each call, so a non-empty buffer is
 * that call's). Error TEXT is not printed: the two libraries word their
 * refusals differently, and what a host can rely on is the failure value and
 * the presence of a message. Reader answers are printed verbatim, JSON and
 * bytes included. Writer scenarios leave their containers in the output
 * directory, where the harness compares them byte for byte.
 *
 * Usage: parity_host <scenario> <dir> [file...]
 */
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "codetracer_trace_writer.h"

/* Exported by both libraries beyond the header; declared here so the host can
 * reach the call-argument staging and the column / argv entry points. */
void trace_writer_register_call_arg(trace_writer_t handle, const char* name,
                                    const uint8_t* cbor_data, size_t cbor_len);
void trace_writer_enable_column_aware_steps(trace_writer_t handle);
void trace_writer_enable_column_breakpoints_support(trace_writer_t handle);
void trace_writer_enable_column_motions_support(trace_writer_t handle);
void trace_writer_register_delta_column(trace_writer_t handle, int64_t column_delta);
int trace_writer_register_path_with_line_lengths(trace_writer_t handle, const char* path,
                                                 int line_count, const uint32_t* line_lengths);
void trace_writer_set_args(trace_writer_t handle, const uint8_t* const* args,
                           const size_t* arg_lens, size_t args_count);
int trace_writer_add_filter_provenance(trace_writer_t handle, const uint8_t* path, size_t path_len,
                                       const uint8_t* sha256, size_t sha256_len);
int trace_writer_record_empty_filter_provenance(trace_writer_t handle);
int ct_value_begin_sequence_with_slice(value_encoder_t h, uint64_t type_id, int element_count, int is_slice);
int ct_meta_dat_has_filter_provenance(meta_dat_reader_t h);
size_t ct_meta_dat_filter_provenance_count(meta_dat_reader_t h);
const uint8_t* ct_meta_dat_filter_provenance_path(meta_dat_reader_t h, size_t idx, size_t* out_len);
int ct_meta_dat_filter_provenance_sha256(meta_dat_reader_t h, size_t idx, uint8_t* out);

static const char* g_dir;
static char g_buf[4096];

static const char* in_dir(const char* name) {
    snprintf(g_buf, sizeof g_buf, "%s/%s", g_dir, name);
    return g_buf;
}

/* Clear the error buffer, so whatever is in it after the call is the call's. */
#define CLR() trace_writer_clear_last_error()
static int err_set(void) {
    const char* e = trace_writer_last_error();
    return e != NULL && e[0] != '\0';
}
#define CALL_V(label, expr) do { CLR(); expr; printf("%s -> err=%d\n", label, err_set()); } while (0)
#define CALL_I(label, expr) do { CLR(); long long _r = (long long)(expr); \
    printf("%s -> %lld err=%d\n", label, _r, err_set()); } while (0)
#define CALL_U(label, expr) do { CLR(); unsigned long long _r = (unsigned long long)(expr); \
    printf("%s -> %llu err=%d\n", label, _r, err_set()); } while (0)

static void print_bytes(const char* label, const uint8_t* p, size_t n) {
    printf("%s [%zu]", label, n);
    if (p == NULL) {
        printf(" NULL\n");
        return;
    }
    printf(" ");
    for (size_t i = 0; i < n; i++) printf("%02x", p[i]);
    printf("\n");
}

static void print_text(const char* label, uint8_t* p, size_t n) {
    printf("%s -> ", label);
    if (p == NULL) {
        printf("NULL err=%d\n", err_set());
        return;
    }
    printf("[%zu] ", n);
    fwrite(p, 1, n, stdout);
    printf(" err=%d\n", err_set());
    ct_free_buffer(p);
}

#define TEXT(label, expr) do { CLR(); size_t _n = 77; uint8_t* _p = (expr); print_text(label, _p, _n); } while (0)

static const char* RID = "01890a5d-ac96-774b-bcce-b302099a8057";

/* A writer begun in the binary (split-stream) format with a pinned id, so
 * the two libraries' containers can be compared byte for byte. */
static trace_writer_t open_writer(const char* program) {
    trace_writer_t w;
    CLR();
    w = trace_writer_new(program, FFI_TRACE_FORMAT_BINARY);
    printf("new(%s) -> %s err=%d\n", program, w ? "handle" : "NULL", err_set());
    CALL_I("set_recording_id", trace_writer_set_recording_id(w, RID));
    CALL_I("begin_metadata", trace_writer_begin_metadata(w, in_dir("trace_metadata.json")));
    CALL_I("begin_events", trace_writer_begin_events(w, in_dir("trace.bin")));
    CALL_I("begin_paths", trace_writer_begin_paths(w, in_dir("trace_paths.json")));
    return w;
}

static void close_writer(trace_writer_t w) {
    CALL_I("finish_events", trace_writer_finish_events(w));
    CALL_I("finish_metadata", trace_writer_finish_metadata(w));
    CALL_I("finish_paths", trace_writer_finish_paths(w));
    CALL_I("close", trace_writer_close(w));
    CALL_V("free", trace_writer_free(w));
}

/* ------------------------------------------------------------------------ */

static int scenario_basic(void) {
    trace_writer_t w = open_writer("/src/basic.py");
    CALL_V("set_workdir", trace_writer_set_workdir(w, "/work"));
    CALL_V("start", trace_writer_start(w, "/src/basic.py", 1));
    CALL_U("next_step_index", trace_writer_next_step_index(w));
    CALL_U("ensure_function_id f", trace_writer_ensure_function_id(w, "f", "/src/basic.py", 3));
    CALL_U("ensure_function_id f again", trace_writer_ensure_function_id(w, "f", "/src/other.py", 9));
    CALL_U("ensure_function_id g line0", trace_writer_ensure_function_id(w, "g", "/src/lib.py", 0));
    CALL_U("ensure_type_id int", trace_writer_ensure_type_id(w, FFI_TYPE_INT, "int"));
    CALL_U("ensure_type_id int again", trace_writer_ensure_type_id(w, FFI_TYPE_INT, "int"));
    CALL_U("ensure_type_id float int", trace_writer_ensure_type_id(w, FFI_TYPE_FLOAT, "int"));
    CALL_U("ensure_type_id str", trace_writer_ensure_type_id(w, FFI_TYPE_STRING, "str"));
    CALL_V("register_step 2", trace_writer_register_step(w, "/src/basic.py", 2));
    CALL_V("variable_int x", trace_writer_register_variable_int(w, "x", 42, FFI_TYPE_INT, "int"));
    CALL_V("variable_raw y", trace_writer_register_variable_raw(w, "y", "<obj>", FFI_TYPE_RAW, "object"));
    CALL_V("variable_int_by_type_id z", trace_writer_register_variable_int_by_type_id(w, "z", -7, 0));
    CALL_V("variable_int_by_type_id dangling", trace_writer_register_variable_int_by_type_id(w, "z", 1, 999));
    CALL_V("variable_raw_by_type_id s", trace_writer_register_variable_raw_by_type_id(w, "s", "hi", 2));
    CALL_V("variable_raw_by_type_id dangling", trace_writer_register_variable_raw_by_type_id(w, "s", "hi", 999));
    CALL_V("special_event write", trace_writer_register_special_event(w, FFI_EVENT_WRITE, "meta", "hello\n"));
    CALL_V("special_event error", trace_writer_register_special_event(w, FFI_EVENT_ERROR, "", "oops"));
    CALL_V("special_event kind 99", trace_writer_register_special_event(w, 99, "m", "c"));
    CALL_U("next_step_index", trace_writer_next_step_index(w));
    CALL_V("register_call f", trace_writer_register_call(w, 1));
    CALL_V("register_step 3", trace_writer_register_step(w, "/src/basic.py", 3));
    CALL_V("register_step lib 1", trace_writer_register_step(w, "/src/lib.py", 1));
    CALL_V("variable_int a", trace_writer_register_variable_int(w, "a", 1, FFI_TYPE_INT, "int"));
    CALL_V("register_return_int", trace_writer_register_return_int(w, 5, FFI_TYPE_INT, "int"));
    CALL_V("variable_int r (staged)", trace_writer_register_variable_int(w, "r", 5, FFI_TYPE_INT, "int"));
    CALL_V("register_step 4", trace_writer_register_step(w, "/src/basic.py", 4));
    CALL_V("register_call g", trace_writer_register_call(w, 2));
    CALL_V("register_step lib 2", trace_writer_register_step(w, "/src/lib.py", 2));
    CALL_V("register_return_raw", trace_writer_register_return_raw(w, "<ret>", FFI_TYPE_RAW, "object"));
    CALL_V("register_call g", trace_writer_register_call(w, 2));
    CALL_V("register_step lib 3", trace_writer_register_step(w, "/src/lib.py", 3));
    CALL_V("register_return_int_by_type_id", trace_writer_register_return_int_by_type_id(w, 9, 0));
    CALL_V("register_return_int_by_type_id dangling", trace_writer_register_return_int_by_type_id(w, 9, 999));
    CALL_V("register_call g", trace_writer_register_call(w, 2));
    CALL_V("register_step lib 4", trace_writer_register_step(w, "/src/lib.py", 4));
    CALL_V("register_return", trace_writer_register_return(w));
    CALL_V("register_return (no call open)", trace_writer_register_return(w));
    CALL_V("register_return (underflow)", trace_writer_register_return(w));
    CALL_V("register_step 5", trace_writer_register_step(w, "/src/basic.py", 5));
    CALL_V("variable_int trailing", trace_writer_register_variable_int(w, "t", 3, FFI_TYPE_INT, "int"));
    CALL_I("source_view", trace_writer_register_source_view(w, 1, 1, "pretty\0x", 8, (const uint8_t*)"a\nb", 3,
                                                            (const uint8_t*)"{}", 2));
    CALL_I("source_view no map", trace_writer_register_source_view(w, 0, 200, NULL, 0, NULL, 0, NULL, 0));
    CALL_I("source_view bad path", trace_writer_register_source_view(w, 9, 0, "x", 1, NULL, 0, NULL, 0));
    CALL_U("source_reload_count", trace_writer_source_reload_count(w));
    close_writer(w);
    return 0;
}

static int scenario_threads(void) {
    trace_writer_t w = open_writer("/src/threads.rb");
    CALL_V("start", trace_writer_start(w, "/src/threads.rb", 1));
    CALL_U("ensure_type_id E", trace_writer_ensure_type_id(w, FFI_TYPE_ERROR, "RuntimeError"));
    CALL_V("thread_start 7", trace_writer_register_thread_start(w, 7));
    CALL_V("variable_int v (staged)", trace_writer_register_variable_int(w, "v", 1, FFI_TYPE_INT, "Integer"));
    CALL_V("register_step 2", trace_writer_register_step(w, "/src/threads.rb", 2));
    CALL_V("special_event (pending step)", trace_writer_register_special_event(w, FFI_EVENT_WRITE, "", "x"));
    CALL_V("thread_switch 7", trace_writer_register_thread_switch(w, 7));
    CALL_V("special_event (after switch)", trace_writer_register_special_event(w, FFI_EVENT_WRITE, "", "y"));
    CALL_V("raise", trace_writer_register_raise(w, 0, (const uint8_t*)"boom\0x", 6));
    CALL_V("raise NULL msg len 3", trace_writer_register_raise(w, 0, NULL, 3));
    CALL_V("raise NULL msg len 0", trace_writer_register_raise(w, 0, NULL, 0));
    CALL_V("catch", trace_writer_register_catch(w, 0));
    CALL_V("register_step 3", trace_writer_register_step(w, "/src/threads.rb", 3));
    CALL_V("thread_exit 7", trace_writer_register_thread_exit(w, 7));
    CALL_U("next_step_index", trace_writer_next_step_index(w));
    CALL_V("register_step 4", trace_writer_register_step(w, "/src/threads.rb", 4));
    CALL_U("next_step_index", trace_writer_next_step_index(w));
    close_writer(w);
    return 0;
}

static int scenario_columns(void) {
    trace_writer_t w = open_writer("/src/cols.js");
    static const uint32_t lens[] = { 10, 0, 25, 4 };
    CALL_V("enable_column_aware_steps", trace_writer_enable_column_aware_steps(w));
    CALL_V("enable_column_breakpoints_support", trace_writer_enable_column_breakpoints_support(w));
    CALL_V("enable_column_motions_support", trace_writer_enable_column_motions_support(w));
    CALL_I("path_with_line_lengths", trace_writer_register_path_with_line_lengths(w, "/src/cols.js", 4, lens));
    CALL_I("path_with_line_lengths again same", trace_writer_register_path_with_line_lengths(w, "/src/cols.js", 4, lens));
    CALL_I("path_with_line_lengths again different", trace_writer_register_path_with_line_lengths(w, "/src/cols.js", 2, lens));
    CALL_I("path_with_line_lengths NULL table", trace_writer_register_path_with_line_lengths(w, "/src/conv.js", 3, NULL));
    CALL_V("start", trace_writer_start(w, "/src/cols.js", 1));
    CALL_V("delta_column 3 (pending)", trace_writer_register_delta_column(w, 3));
    CALL_V("variable_int c", trace_writer_register_variable_int(w, "c", 1, FFI_TYPE_INT, "number"));
    CALL_V("register_step 3", trace_writer_register_step(w, "/src/cols.js", 3));
    CALL_V("delta_column 5", trace_writer_register_delta_column(w, 5));
    CALL_V("delta_column 2 (accumulates)", trace_writer_register_delta_column(w, 2));
    CALL_V("register_call 0", trace_writer_register_call(w, 0));
    CALL_V("delta_column 1 (no pending step)", trace_writer_register_delta_column(w, 1));
    CALL_V("register_step conv 2", trace_writer_register_step(w, "/src/conv.js", 2));
    CALL_V("delta_column 5000 (conventional clamp)", trace_writer_register_delta_column(w, 5000));
    CALL_V("register_step 4", trace_writer_register_step(w, "/src/cols.js", 4));
    CALL_V("register_step unregistered", trace_writer_register_step(w, "/src/new.js", 7));
    CALL_U("next_step_index", trace_writer_next_step_index(w));
    close_writer(w);
    return 0;
}

static int scenario_late_columns(void) {
    /* Column awareness asked for after the first record is refused. */
    trace_writer_t w = open_writer("/src/late.js");
    CALL_V("start", trace_writer_start(w, "/src/late.js", 1));
    CALL_V("register_step 2", trace_writer_register_step(w, "/src/late.js", 2));
    CALL_V("enable_column_aware_steps (late)", trace_writer_enable_column_aware_steps(w));
    CALL_V("delta_column (line-only writer)", trace_writer_register_delta_column(w, 1));
    close_writer(w);
    return 0;
}

static int scenario_linecounts(void) {
    trace_writer_t w = open_writer("/src/game.gd");
    ct_tw_source_reload_change ch[2];
    CALL_I("declare_source_reload", trace_writer_declare_source_reload(w));
    CALL_I("enable_line_count_table", trace_writer_enable_line_count_table(w));
    CALL_I("enable_line_count_table again", trace_writer_enable_line_count_table(w));
    CALL_I("path_with_line_count a 10", trace_writer_register_path_with_line_count(w, "/src/a.gd", 10));
    CALL_I("path_with_line_count b 0", trace_writer_register_path_with_line_count(w, "/src/b.gd", 0));
    CALL_I("path_with_line_count b 20", trace_writer_register_path_with_line_count(w, "/src/b.gd", 20));
    CALL_I("path_with_line_count a again 99", trace_writer_register_path_with_line_count(w, "/src/a.gd", 99));
    CALL_U("register_path a", trace_writer_register_path(w, "/src/a.gd"));
    CALL_U("register_path unknown (no count)", trace_writer_register_path(w, "/src/c.gd"));
    CALL_U("current_path_id a", trace_writer_current_path_id(w, "/src/a.gd"));
    CALL_U("current_path_id unknown", trace_writer_current_path_id(w, "/src/zz.gd"));
    CALL_U("register_variable_name hp", trace_writer_register_variable_name(w, "hp"));
    CALL_U("register_variable_name hp again", trace_writer_register_variable_name(w, "hp"));
    CALL_U("register_variable_name mp", trace_writer_register_variable_name(w, "mp"));
    CALL_V("start", trace_writer_start(w, "/src/a.gd", 1));
    CALL_V("register_step a 2", trace_writer_register_step(w, "/src/a.gd", 2));
    CALL_V("register_step a 11 (past count)", trace_writer_register_step(w, "/src/a.gd", 11));
    CALL_V("register_step unregistered", trace_writer_register_step(w, "/src/c.gd", 1));
    CALL_V("register_step b 0", trace_writer_register_step(w, "/src/b.gd", 0));
    CALL_U("path_version a 12", trace_writer_register_path_version(w, "/src/a.gd", 12));
    CALL_U("path_version a 0", trace_writer_register_path_version(w, "/src/a.gd", 0));
    CALL_U("current_path_id a", trace_writer_current_path_id(w, "/src/a.gd"));
    ch[0].old_path_id = 0; ch[0].new_path_id = 2; ch[0].generation = 2;
    CALL_U("source_reload", trace_writer_register_source_reload(w, ch, 1, 1));
    CALL_U("source_reload NULL", trace_writer_register_source_reload(w, NULL, 0, 0));
    ch[0].generation = 1;
    CALL_U("source_reload generation 1", trace_writer_register_source_reload(w, ch, 1, 0));
    ch[0].generation = 3; ch[0].new_path_id = 0;
    CALL_U("source_reload old==new", trace_writer_register_source_reload(w, ch, 1, 0));
    ch[0].new_path_id = 77;
    CALL_U("source_reload unregistered new", trace_writer_register_source_reload(w, ch, 1, 0));
    ch[0].new_path_id = 2; ch[0].generation = 3;
    ch[1].old_path_id = 1; ch[1].new_path_id = 2; ch[1].generation = 4;
    CALL_U("source_reload two", trace_writer_register_source_reload(w, ch, 2, 0));
    CALL_U("source_reload_count", trace_writer_source_reload_count(w));
    CALL_V("register_step a 12 (new version)", trace_writer_register_step(w, "/src/a.gd", 12));
    CALL_V("set_workdir (after first record)", trace_writer_set_workdir(w, "/late"));
    CALL_I("declare_source_reload (late)", trace_writer_declare_source_reload(w));
    close_writer(w);
    return 0;
}

static int scenario_undeclared_reload(void) {
    trace_writer_t w = open_writer("/src/undeclared.gd");
    ct_tw_source_reload_change ch = { 0, 1, 2 };
    CALL_I("enable_line_count_table", trace_writer_enable_line_count_table(w));
    CALL_I("path_with_line_count a 5", trace_writer_register_path_with_line_count(w, "/src/a.gd", 5));
    CALL_U("path_version a 6", trace_writer_register_path_version(w, "/src/a.gd", 6));
    CALL_V("register_step a 1", trace_writer_register_step(w, "/src/a.gd", 1));
    CALL_U("source_reload (undeclared)", trace_writer_register_source_reload(w, &ch, 1, 0));
    close_writer(w);
    return 0;
}

static int scenario_no_line_count_versions(void) {
    trace_writer_t w = open_writer("/src/nolc.py");
    CALL_U("path_version (no table)", trace_writer_register_path_version(w, "/src/a.py", 4));
    CALL_I("path_with_line_count (no table)", trace_writer_register_path_with_line_count(w, "/src/a.py", 4));
    CALL_U("register_path", trace_writer_register_path(w, "/src/b.py"));
    CALL_V("register_step", trace_writer_register_step(w, "/src/b.py", 1));
    CALL_I("enable_line_count_table (late)", trace_writer_enable_line_count_table(w));
    close_writer(w);
    return 0;
}

/* ------------------------------------------------------------------------ */

static void dump_encoder(const char* label, value_encoder_t e) {
    size_t n = 0;
    const uint8_t* p;
    CLR();
    p = ct_value_get_bytes(e, &n);
    print_bytes(label, p, n);
}

static int scenario_values(void) {
    value_encoder_t e;
    trace_writer_t w;
    size_t n = 0;
    const uint8_t* p;
    uint8_t big[] = { 0x01, 0x00, 0xff };
    uint8_t cbor[256];
    size_t cbor_len;
    /* serde-CBOR `RValue::Simple(VariableId(0))`, adjacently tagged. */
    static const uint8_t rvalue_simple[] = { 0xa2, 0x64, 'k', 'i', 'n', 'd', 0x66, 'S', 'i', 'm', 'p', 'l', 'e',
                                             0x64, 'd', 'a', 't', 'a', 0x00 };
    const char* names[] = { "a", "b", "a" };

    CLR();
    e = ct_value_encoder_new();
    printf("encoder_new -> %s err=%d\n", e ? "handle" : "NULL", err_set());
    CALL_I("write_int", ct_value_write_int(e, -123456789012LL, 3));
    dump_encoder("int", e);
    CALL_V("reset", ct_value_encoder_reset(e));
    dump_encoder("empty", e);
    CALL_I("write_float", ct_value_write_float(e, 1.5, 4));
    CALL_I("write_bool", ct_value_write_bool(e, 1));
    CALL_I("write_bool_typed", ct_value_write_bool_typed(e, 0, 9));
    CALL_I("write_string", ct_value_write_string(e, (const uint8_t*)"h\0i", 3, 5));
    CALL_I("write_string NULL", ct_value_write_string(e, NULL, 0, 5));
    CALL_I("write_none", ct_value_write_none(e));
    CALL_I("write_none_typed", ct_value_write_none_typed(e, 6));
    CALL_I("write_raw", ct_value_write_raw(e, (const uint8_t*)"raw", 3, 7));
    CALL_I("write_error", ct_value_write_error(e, (const uint8_t*)"bad", 3, 8));
    CALL_I("write_char", ct_value_write_char(e, 0x41, 1));
    CALL_I("write_char wide", ct_value_write_char(e, 0x263a, 1));
    CALL_I("write_bigint", ct_value_write_bigint(e, big, 3, 1, 2));
    CALL_I("write_bigint NULL data", ct_value_write_bigint(e, NULL, 2, 0, 2));
    dump_encoder("leaves", e);
    CALL_V("reset", ct_value_encoder_reset(e));
    CALL_I("begin_struct", ct_value_begin_struct(e, 10, 2));
    CALL_I("  write_int", ct_value_write_int(e, 1, 3));
    CALL_I("  begin_sequence", ct_value_begin_sequence(e, 11, 1));
    CALL_I("    begin_tuple", ct_value_begin_tuple(e, 12, 2));
    CALL_I("      begin_variant", ct_value_begin_variant(e, (const uint8_t*)"Some", 4, 13));
    CALL_I("        write_bool", ct_value_write_bool_typed(e, 1, 9));
    CALL_I("      end (variant)", ct_value_end_compound(e));
    CALL_I("      begin_reference", ct_value_begin_reference(e, 0xdeadbeef, 1, 14));
    CALL_I("        write_none", ct_value_write_none_typed(e, 6));
    CALL_I("      end (reference)", ct_value_end_compound(e));
    CALL_I("    end (tuple)", ct_value_end_compound(e));
    CALL_I("  end (sequence)", ct_value_end_compound(e));
    CALL_I("  begin_sequence_with_slice", ct_value_begin_sequence_with_slice(e, 15, 0, 1));
    CALL_I("  end (slice)", ct_value_end_compound(e));
    CALL_I("end (struct)", ct_value_end_compound(e));
    CALL_I("end (unbalanced)", ct_value_end_compound(e));
    CALL_I("begin_variant NULL disc", ct_value_begin_variant(e, NULL, 2, 1));
    dump_encoder("compound", e);
    CALL_V("reset", ct_value_encoder_reset(e));
    for (int i = 0; i < 33; i++) {
        CLR();
        int rc = ct_value_begin_sequence(e, 1, 1);
        if (rc != 0 || i >= 31) printf("nest %d -> %d err=%d\n", i, rc, err_set());
    }
    CALL_V("reset", ct_value_encoder_reset(e));
    CALL_I("write NULL handle", ct_value_write_int(NULL, 1, 1));
    CLR();
    p = ct_value_get_bytes(NULL, &n);
    printf("get_bytes NULL -> %s err=%d\n", p ? "ptr" : "NULL", err_set());
    CALL_I("begin_struct NULL", ct_value_begin_struct(NULL, 1, 1));
    CALL_V("reset NULL", ct_value_encoder_reset(NULL));
    CALL_V("free NULL", ct_value_encoder_free(NULL));

    w = open_writer("/src/values.py");
    CALL_V("start", trace_writer_start(w, "/src/values.py", 1));
    CALL_U("type Seq", trace_writer_ensure_type_id(w, FFI_TYPE_SEQ, "list"));
    CALL_V("register_step 2", trace_writer_register_step(w, "/src/values.py", 2));

    ct_value_encoder_reset(e);
    ct_value_begin_sequence(e, 0, 2);
    ct_value_write_int(e, 1, 0);
    ct_value_write_string(e, (const uint8_t*)"two", 3, 0);
    ct_value_end_compound(e);
    p = ct_value_get_bytes(e, &cbor_len);
    memcpy(cbor, p, cbor_len);
    CALL_V("variable_cbor xs", trace_writer_register_variable_cbor(w, "xs", cbor, cbor_len));
    CALL_V("variable_cbor empty", trace_writer_register_variable_cbor(w, "nothing", NULL, 0));
    CALL_I("assignment", trace_writer_register_assignment(w, "xs", 0, rvalue_simple, sizeof rvalue_simple));
    CALL_I("drop_variables", trace_writer_register_drop_variables(w, names, 3));
    CALL_I("drop_variables empty", trace_writer_register_drop_variables(w, NULL, 0));
    CALL_I("drop_variables NULL names", trace_writer_register_drop_variables(w, NULL, 2));
    CALL_I("drop_variable", trace_writer_register_drop_variable(w, "b"));
    CALL_I("bind_variable", trace_writer_bind_variable(w, "xs", 100));
    CALL_I("cell_value", trace_writer_register_cell_value(w, 101, cbor, cbor_len));
    CALL_I("compound_value", trace_writer_register_compound_value(w, -3, cbor, cbor_len));
    CALL_I("assign_cell", trace_writer_assign_cell(w, 101, cbor, cbor_len));
    CALL_I("assign_compound_item", trace_writer_assign_compound_item(w, -3, 1, 101));
    CALL_I("variable_cell", trace_writer_register_variable_cell(w, "c", 101));
    CALL_V("call_arg q", trace_writer_register_call_arg(w, "q", cbor, cbor_len));
    CALL_V("register_call 0", trace_writer_register_call(w, 0));
    CALL_V("register_step 3", trace_writer_register_step(w, "/src/values.py", 3));
    CALL_V("variable_cbor in callee", trace_writer_register_variable_cbor(w, "q", cbor, cbor_len));
    CALL_V("return_cbor", trace_writer_register_return_cbor(w, cbor, cbor_len));
    CALL_V("register_step 4", trace_writer_register_step(w, "/src/values.py", 4));
    CALL_I("drop_variable (no step after)", trace_writer_register_drop_variable(w, "xs"));
    close_writer(w);
    CALL_V("encoder_free", ct_value_encoder_free(e));
    return 0;
}

/* ------------------------------------------------------------------------ */

static int scenario_meta(void) {
    trace_writer_t w = open_writer("/bin/native");
    const char* strategies[] = { "plt", "inline" };
    static const uint8_t fp[] = { 1, 2, 3, 4, 5 };
    static const uint8_t sha[32] = { 0xaa, 0xbb };
    const uint8_t* args[] = { (const uint8_t*)"--x", (const uint8_t*)"a\0b" };
    size_t arg_lens[] = { 3, 3 };
    uint8_t* buf = NULL;
    size_t len = 0;
    meta_dat_reader_t m;
    const uint8_t* p;
    size_t n;

    CALL_V("set_args", trace_writer_set_args(w, args, arg_lens, 2));
    CALL_I("set_mcr_fields bad tick", trace_writer_set_mcr_fields(w, 99, 4, 0, 1000, 3, 77, "linux", "ns",
                                                                    "rdtsc", "seq", "now", "default", strategies, 2));
    CALL_I("set_mcr_fields NULL strategies", trace_writer_set_mcr_fields(w, 0, 4, 0, 1000, 3, 77, "linux", "ns",
                                                                         "rdtsc", "seq", "now", "default", NULL, 2));
    CALL_I("set_mcr_fields", trace_writer_set_mcr_fields(w, 1, 4, 1, 1000, 3, 77, "linux", "ns", "rdtsc", "seq",
                                                         "now", "default", strategies, 2));
    CALL_I("set_replay_launch_fields", trace_writer_set_replay_launch_fields(w, 1));
    CALL_I("set_layout_snapshot NULL", trace_writer_set_layout_snapshot(w, 9, NULL, 3));
    CALL_I("set_layout_snapshot", trace_writer_set_layout_snapshot(w, 0x1122334455667788ULL, fp, sizeof fp));
    CALL_I("add_filter_provenance short", trace_writer_add_filter_provenance(w, (const uint8_t*)"f.toml", 6, sha, 31));
    CALL_I("add_filter_provenance", trace_writer_add_filter_provenance(w, (const uint8_t*)"f.toml", 6, sha, 32));
    CALL_I("ct_write_meta_dat", ct_write_meta_dat(w, (const uint8_t*)"rec", 3));
    CALL_V("start", trace_writer_start(w, "/src/main.c", 1));
    CALL_I("set_replay_launch_fields (late)", trace_writer_set_replay_launch_fields(w, 0));
    close_writer(w);

    CALL_I("meta_to_buffer", ct_write_meta_dat_to_buffer((const uint8_t*)"prog", 4, (const uint8_t*)"/wd", 3, args,
                                                         arg_lens, 2, (const uint8_t*)"rec-id", 6,
                                                         (const uint8_t*)RID, strlen(RID), &buf, &len));
    print_bytes("meta.dat", buf, len);
    CLR();
    m = ct_read_meta_dat(buf, len);
    printf("read_meta_dat -> %s err=%d\n", m ? "handle" : "NULL", err_set());
    p = ct_meta_dat_program(m, &n); print_bytes("program", p, n);
    p = ct_meta_dat_workdir(m, &n); print_bytes("workdir", p, n);
    p = ct_meta_dat_recorder_id(m, &n); print_bytes("recorder_id", p, n);
    p = ct_meta_dat_recording_id(m, &n); print_bytes("recording_id", p, n);
    printf("args_count %zu\n", ct_meta_dat_args_count(m));
    p = ct_meta_dat_arg(m, 1, &n); print_bytes("arg1", p, n);
    n = 55; p = ct_meta_dat_arg(m, 2, &n); printf("arg2 -> %s n=%zu\n", p ? "ptr" : "NULL", n);
    printf("has_filter_provenance %d\n", ct_meta_dat_has_filter_provenance(m));
    CALL_V("meta_dat_free", ct_meta_dat_free(m));
    ct_free_buffer(buf);
    buf = NULL;

    CALL_I("meta_to_buffer bad id", ct_write_meta_dat_to_buffer((const uint8_t*)"p", 1, NULL, 0, NULL, NULL, 0, NULL,
                                                                0, (const uint8_t*)"nope", 4, &buf, &len));
    CALL_I("meta_to_buffer NULL out", ct_write_meta_dat_to_buffer((const uint8_t*)"p", 1, NULL, 0, NULL, NULL, 0, NULL,
                                                                  0, NULL, 0, NULL, &len));
    CALL_I("meta_to_buffer minted", ct_write_meta_dat_to_buffer((const uint8_t*)"p", 1, NULL, 0, NULL, NULL, 0, NULL,
                                                                0, NULL, 0, &buf, &len));
    m = ct_read_meta_dat(buf, len);
    p = ct_meta_dat_recording_id(m, &n);
    printf("minted recording id length %zu\n", n);
    ct_meta_dat_free(m);
    ct_free_buffer(buf);
    CLR();
    m = ct_read_meta_dat((const uint8_t*)"garbage!", 8);
    printf("read_meta_dat garbage -> %s err=%d\n", m ? "handle" : "NULL", err_set());
    CLR();
    m = ct_read_meta_dat(NULL, 0);
    printf("read_meta_dat NULL -> %s err=%d\n", m ? "handle" : "NULL", err_set());
    CALL_V("free_buffer NULL", ct_free_buffer(NULL));
    return 0;
}

static int scenario_in_memory(void) {
    trace_writer_t w;
    FILE* f;
    CLR();
    w = trace_writer_new("/src/mem.py", FFI_TRACE_FORMAT_BINARY);
    CALL_I("set_recording_id", trace_writer_set_recording_id(w, RID));
    CALL_I("ready before", trace_writer_container_ready(w));
    CALL_U("len before", trace_writer_container_len(w));
    CALL_I("set_compact_threshold", trace_writer_set_compact_threshold(w, 1u << 20));
    CALL_I("begin_in_memory", trace_writer_begin_in_memory(w));
    CALL_I("begin_in_memory again", trace_writer_begin_in_memory(w));
    CALL_I("begin_events (after in-memory)", trace_writer_begin_events(w, in_dir("x.bin")));
    CALL_I("set_recording_id (late)", trace_writer_set_recording_id(w, RID));
    CALL_V("start", trace_writer_start(w, "/src/mem.py", 1));
    CALL_V("register_step 2", trace_writer_register_step(w, "/src/mem.py", 2));
    CALL_I("close", trace_writer_close(w));
    CALL_I("ready after", trace_writer_container_ready(w));
    CLR();
    {
        size_t n = trace_writer_container_len(w);
        const uint8_t* p = trace_writer_container_ptr(w);
        printf("container len %zu ptr %s err=%d\n", n, p ? "ptr" : "NULL", err_set());
        f = fopen(in_dir("mem.ct"), "wb");
        if (p) fwrite(p, 1, n, f);
        fclose(f);
    }
    CALL_V("free", trace_writer_free(w));

    CLR();
    w = trace_writer_new("/src/memfull.py", FFI_TRACE_FORMAT_BINARY);
    CALL_I("set_recording_id", trace_writer_set_recording_id(w, RID));
    CALL_I("begin_events", trace_writer_begin_events(w, in_dir("x.bin")));
    CALL_I("begin_in_memory (after file)", trace_writer_begin_in_memory(w));
    CALL_V("free without close", trace_writer_free(w));

    CLR();
    w = trace_writer_new("/src/empty.py", FFI_TRACE_FORMAT_BINARY);
    CALL_I("set_recording_id empty", trace_writer_set_recording_id(w, ""));
    CALL_I("set_recording_id bad", trace_writer_set_recording_id(w, "not-a-uuid"));
    CALL_I("set_recording_id NULL", trace_writer_set_recording_id(w, NULL));
    CALL_I("set_recording_id", trace_writer_set_recording_id(w, RID));
    CALL_I("begin_in_memory", trace_writer_begin_in_memory(w));
    CALL_I("close (no records)", trace_writer_close(w));
    CALL_I("ready", trace_writer_container_ready(w));
    CLR();
    {
        size_t n = trace_writer_container_len(w);
        const uint8_t* p = trace_writer_container_ptr(w);
        printf("container len %zu ptr %s err=%d\n", n, p ? "ptr" : "NULL", err_set());
        f = fopen(in_dir("empty.ct"), "wb");
        if (p) fwrite(p, 1, n, f);
        fclose(f);
    }
    CALL_V("free", trace_writer_free(w));
    return 0;
}

static int scenario_trailing(void) {
    /* Values staged with no step ever recorded fail the close. */
    trace_writer_t w = open_writer("/src/trailing.py");
    CALL_V("variable_int (no step)", trace_writer_register_variable_int(w, "v", 1, FFI_TYPE_INT, "int"));
    CALL_I("close", trace_writer_close(w));
    CALL_V("free", trace_writer_free(w));
    return 0;
}

static int scenario_free_closes(void) {
    trace_writer_t w = open_writer("/src/freed.py");
    CALL_V("start", trace_writer_start(w, "/src/freed.py", 1));
    CALL_V("register_step 2", trace_writer_register_step(w, "/src/freed.py", 2));
    CALL_V("variable_int v", trace_writer_register_variable_int(w, "v", 1, FFI_TYPE_INT, "int"));
    CALL_V("free without close", trace_writer_free(w));
    return 0;
}

static int scenario_nulls(void) {
    uint64_t u = 0;
    CALL_I("begin_metadata NULL", trace_writer_begin_metadata(NULL, "x"));
    CALL_I("finish_metadata NULL", trace_writer_finish_metadata(NULL));
    CALL_I("begin_events NULL", trace_writer_begin_events(NULL, "x"));
    CALL_I("finish_events NULL", trace_writer_finish_events(NULL));
    CALL_I("begin_paths NULL", trace_writer_begin_paths(NULL, "x"));
    CALL_I("finish_paths NULL", trace_writer_finish_paths(NULL));
    CALL_I("begin_in_memory NULL", trace_writer_begin_in_memory(NULL));
    CALL_I("container_ready NULL", trace_writer_container_ready(NULL));
    CALL_U("container_len NULL", trace_writer_container_len(NULL));
    CALL_I("container_ptr NULL", trace_writer_container_ptr(NULL) != NULL);
    CALL_I("set_compact_threshold NULL", trace_writer_set_compact_threshold(NULL, 1));
    CALL_I("set_recording_id NULL", trace_writer_set_recording_id(NULL, RID));
    CALL_I("set_mcr_fields NULL", trace_writer_set_mcr_fields(NULL, 0, 0, 0, 0, 0, 0, "", "", "", "", "", "", NULL, 0));
    CALL_I("set_replay_launch_fields NULL", trace_writer_set_replay_launch_fields(NULL, 0));
    CALL_I("set_layout_snapshot NULL", trace_writer_set_layout_snapshot(NULL, 0, NULL, 0));
    CALL_V("start NULL", trace_writer_start(NULL, "x", 1));
    CALL_V("set_workdir NULL", trace_writer_set_workdir(NULL, "x"));
    CALL_V("set_interning_qualifier NULL", trace_writer_set_interning_qualifier(NULL, "x"));
    CALL_V("register_step NULL", trace_writer_register_step(NULL, "x", 1));
    CALL_I("enable_line_count_table NULL", trace_writer_enable_line_count_table(NULL));
    CALL_I("path_with_line_count NULL", trace_writer_register_path_with_line_count(NULL, "x", 1));
    CALL_U("path_version NULL", trace_writer_register_path_version(NULL, "x", 1));
    CALL_U("current_path_id NULL", trace_writer_current_path_id(NULL, "x"));
    CALL_U("register_path NULL", trace_writer_register_path(NULL, "x"));
    CALL_U("register_variable_name NULL", trace_writer_register_variable_name(NULL, "x"));
    CALL_I("declare_source_reload NULL", trace_writer_declare_source_reload(NULL));
    CALL_U("register_source_reload NULL", trace_writer_register_source_reload(NULL, NULL, 0, 0));
    CALL_U("source_reload_count NULL", trace_writer_source_reload_count(NULL));
    CALL_U("ensure_function_id NULL", trace_writer_ensure_function_id(NULL, "f", "x", 1));
    CALL_U("ensure_type_id NULL", trace_writer_ensure_type_id(NULL, 0, "x"));
    CALL_V("register_call NULL", trace_writer_register_call(NULL, 0));
    CALL_V("register_return NULL", trace_writer_register_return(NULL));
    CALL_V("register_return_int NULL", trace_writer_register_return_int(NULL, 1, 7, "int"));
    CALL_V("register_return_raw NULL", trace_writer_register_return_raw(NULL, "x", 7, "int"));
    CALL_V("register_variable_int NULL", trace_writer_register_variable_int(NULL, "x", 1, 7, "int"));
    CALL_V("register_variable_raw NULL", trace_writer_register_variable_raw(NULL, "x", "y", 7, "int"));
    CALL_V("return_int_by_type_id NULL", trace_writer_register_return_int_by_type_id(NULL, 1, 0));
    CALL_V("variable_int_by_type_id NULL", trace_writer_register_variable_int_by_type_id(NULL, "x", 1, 0));
    CALL_V("variable_raw_by_type_id NULL", trace_writer_register_variable_raw_by_type_id(NULL, "x", "y", 0));
    CALL_V("variable_cbor NULL", trace_writer_register_variable_cbor(NULL, "x", NULL, 0));
    CALL_I("assignment NULL", trace_writer_register_assignment(NULL, "x", 0, NULL, 0));
    CALL_I("drop_variables NULL", trace_writer_register_drop_variables(NULL, NULL, 0));
    CALL_I("drop_variable NULL", trace_writer_register_drop_variable(NULL, "x"));
    CALL_I("bind_variable NULL", trace_writer_bind_variable(NULL, "x", 1));
    CALL_I("cell_value NULL", trace_writer_register_cell_value(NULL, 1, NULL, 0));
    CALL_I("compound_value NULL", trace_writer_register_compound_value(NULL, 1, NULL, 0));
    CALL_I("assign_cell NULL", trace_writer_assign_cell(NULL, 1, NULL, 0));
    CALL_I("assign_compound_item NULL", trace_writer_assign_compound_item(NULL, 1, 0, 1));
    CALL_I("variable_cell NULL", trace_writer_register_variable_cell(NULL, "x", 1));
    CALL_V("return_cbor NULL", trace_writer_register_return_cbor(NULL, NULL, 0));
    CALL_V("special_event NULL", trace_writer_register_special_event(NULL, 0, "", ""));
    CALL_U("next_step_index NULL", trace_writer_next_step_index(NULL));
    CALL_I("source_view NULL", trace_writer_register_source_view(NULL, 0, 0, NULL, 0, NULL, 0, NULL, 0));
    CALL_V("thread_start NULL", trace_writer_register_thread_start(NULL, 1));
    CALL_V("thread_exit NULL", trace_writer_register_thread_exit(NULL, 1));
    CALL_V("thread_switch NULL", trace_writer_register_thread_switch(NULL, 1));
    CALL_V("raise NULL", trace_writer_register_raise(NULL, 0, NULL, 0));
    CALL_V("catch NULL", trace_writer_register_catch(NULL, 0));
    CALL_I("ct_write_meta_dat NULL", ct_write_meta_dat(NULL, NULL, 0));
    CALL_I("close NULL", trace_writer_close(NULL));
    CALL_V("free NULL", trace_writer_free(NULL));
    (void)u;
    return 0;
}

static int scenario_not_ready(void) {
    /* Every call made before a begin. */
    trace_writer_t w;
    CLR();
    w = trace_writer_new("/src/notready.py", FFI_TRACE_FORMAT_BINARY);
    CALL_V("start", trace_writer_start(w, "/src/a.py", 1));
    CALL_V("register_step", trace_writer_register_step(w, "/src/a.py", 1));
    CALL_U("ensure_function_id", trace_writer_ensure_function_id(w, "f", "/src/a.py", 1));
    CALL_U("ensure_type_id", trace_writer_ensure_type_id(w, FFI_TYPE_INT, "int"));
    CALL_V("variable_int", trace_writer_register_variable_int(w, "x", 1, FFI_TYPE_INT, "int"));
    CALL_V("special_event", trace_writer_register_special_event(w, 0, "", "x"));
    CALL_I("enable_line_count_table", trace_writer_enable_line_count_table(w));
    CALL_I("path_with_line_count", trace_writer_register_path_with_line_count(w, "/src/a.py", 1));
    CALL_U("path_version", trace_writer_register_path_version(w, "/src/a.py", 1));
    CALL_U("current_path_id", trace_writer_current_path_id(w, "/src/a.py"));
    CALL_U("register_path", trace_writer_register_path(w, "/src/a.py"));
    CALL_U("register_variable_name", trace_writer_register_variable_name(w, "x"));
    CALL_I("declare_source_reload", trace_writer_declare_source_reload(w));
    CALL_I("assignment", trace_writer_register_assignment(w, "x", 0, NULL, 0));
    CALL_I("drop_variable", trace_writer_register_drop_variable(w, "x"));
    CALL_I("bind_variable", trace_writer_bind_variable(w, "x", 1));
    CALL_I("set_mcr_fields", trace_writer_set_mcr_fields(w, 0, 0, 0, 0, 0, 0, "", "", "", "", "", "", NULL, 0));
    CALL_I("ct_write_meta_dat", ct_write_meta_dat(w, NULL, 0));
    CALL_U("next_step_index", trace_writer_next_step_index(w));
    CALL_I("source_view", trace_writer_register_source_view(w, 0, 0, NULL, 0, NULL, 0, NULL, 0));
    CALL_V("thread_start", trace_writer_register_thread_start(w, 1));
    CALL_V("raise", trace_writer_register_raise(w, 0, NULL, 0));
    CALL_I("finish_events", trace_writer_finish_events(w));
    CALL_I("close", trace_writer_close(w));
    CALL_V("free", trace_writer_free(w));
    return 0;
}

/* ------------------------------------------------------------------------ */

static int scenario_container(void) {
    const char* names[] = { "snap1.dat", "snap2.idx" };
    const uint8_t* contents[] = { (const uint8_t*)"hello", (const uint8_t*)"" };
    size_t lengths[] = { 5, 0 };
    const char* bad[] = { "UPPER.dat" };
    const char* dup[] = { "snap1.dat" };
    const char* nul[] = { NULL };
    size_t five[] = { 5 };
    const uint8_t* nocontent[] = { NULL };
    char path[4096];
    snprintf(path, sizeof path, "%s/c.ct", g_dir);
    CALL_I("create bad block size", ct_container_create(in_dir("bad.ct"), 13));
    CALL_I("create NULL", ct_container_create(NULL, 0));
    CALL_I("create", ct_container_create(path, 0));
    CALL_I("append", ct_container_append_files(path, names, contents, lengths, 2));
    CALL_I("append none", ct_container_append_files(path, NULL, NULL, NULL, 0));
    CALL_I("append bad name", ct_container_append_files(path, bad, contents, lengths, 1));
    CALL_I("append duplicate", ct_container_append_files(path, dup, contents, five, 1));
    CALL_I("append NULL name", ct_container_append_files(path, nul, contents, five, 1));
    CALL_I("append NULL content", ct_container_append_files(path, bad, nocontent, five, 1));
    CALL_I("append NULL arrays", ct_container_append_files(path, NULL, NULL, NULL, 1));
    CALL_I("append missing file", ct_container_append_files(in_dir("missing.ct"), names, contents, lengths, 1));
    CALL_I("append NULL path", ct_container_append_files(NULL, names, contents, lengths, 1));
    return 0;
}


/* ------------------------------------------------------------------------ */
/* Containers with one member malformed, built with the container calls so
 * that both readers see the same bytes: what each refuses, and where. */

typedef struct { uint8_t b[8192]; size_t n; } buf_t;
static void put(buf_t* x, const void* p, size_t n) { memcpy(x->b + x->n, p, n); x->n += n; }
static void put8(buf_t* x, uint8_t v) { put(x, &v, 1); }
static void put32(buf_t* x, uint32_t v) { put(x, &v, 4); }
static void put64(buf_t* x, uint64_t v) { put(x, &v, 8); }
static void putv(buf_t* x, uint64_t v) { do { uint8_t b = v & 0x7f; v >>= 7; if (v) b |= 0x80; put8(x, b); } while (v); }
static void putz(buf_t* x, int64_t v) { putv(x, v >= 0 ? ((uint64_t)v << 1) : ((((uint64_t)~v) << 1) | 1)); }
/* One uncompressed zstd frame (raw block) holding `content`. */
static void put_frame(buf_t* x, const buf_t* content) {
    uint32_t bh = (uint32_t)(content->n << 3) | 1; /* last block, raw */
    static const uint8_t magic[] = { 0x28, 0xb5, 0x2f, 0xfd };
    put(x, magic, 4);
    if (content->n < 256) { put8(x, 0x20); put8(x, (uint8_t)content->n); }
    else { put8(x, 0x60); uint16_t fcs = (uint16_t)(content->n - 256); put(x, &fcs, 2); }
    put(x, &bh, 3);
    put(x, content->b, content->n);
}

typedef struct { const char* name; buf_t data; } member_t;

static void craft(const char* file, member_t* m, size_t count) {
    const char* names[16];
    const uint8_t* contents[16];
    size_t lengths[16];
    char path[4096];
    for (size_t i = 0; i < count; i++) { names[i] = m[i].name; contents[i] = m[i].data.b; lengths[i] = m[i].data.n; }
    snprintf(path, sizeof path, "%s/%s", g_dir, file);
    CLR();
    if (ct_container_create(path, 0) != 0 || ct_container_append_files(path, names, contents, lengths, count) != 0)
        printf("craft %s failed err=%d\n", file, err_set());
    else
        printf("crafted %s (%zu members)\n", file, count);
}

/* A version 6 meta.dat declaring the five streams a writer creates, as
 * every writer's does (`internal-files.md` "Metadata (meta.dat)"). */
static void meta_member(member_t* m, uint32_t ext_flags) {
    m->name = "meta.dat";
    m->data.n = 0;
    put(&m->data, "CTMD", 4);
    put8(&m->data, 6); put8(&m->data, 0);
    put8(&m->data, 0x00); put8(&m->data, 0x1f);
    put32(&m->data, ext_flags);
    putv(&m->data, strlen(RID)); put(&m->data, RID, strlen(RID));
    putv(&m->data, 7); put(&m->data, "crafted", 7);
    putv(&m->data, 0);
    putv(&m->data, 2); put(&m->data, "/w", 2);
    putv(&m->data, 0);
}

/* A step stream: one chunk holding `content`. */
static void steps_members(member_t* dat, member_t* idx, const buf_t* content, int trailing) {
    dat->name = "steps.dat"; dat->data.n = 0;
    idx->name = "steps.idx"; idx->data.n = 0;
    put_frame(&dat->data, content);
    put32(&idx->data, 4096);
    put64(&idx->data, 0);
    if (trailing) put8(&idx->data, 0);
}

static void table_members(member_t* dat, member_t* off, const char* name, const buf_t* records, const uint64_t* ends, size_t n) {
    static char dn[16][16], on[16][16];
    static int slot = 0;
    slot = (slot + 1) % 16;
    snprintf(dn[slot], 16, "%s.dat", name);
    snprintf(on[slot], 16, "%s.off", name);
    dat->name = dn[slot]; dat->data = *records;
    off->name = on[slot]; off->data.n = 0;
    put64(&off->data, 0);
    for (size_t i = 0; i < n; i++) put64(&off->data, ends[i]);
}

static int scenario_craft(void) {
    member_t m[8];
    buf_t c;
    static const uint8_t bad_meta[] = { 'C', 'T', 'M', 'D', 99, 0, 1, 2, 3 };

    /* 1. A delta before the chunk's first absolute step, then a good chunk. */
    memset(m, 0, sizeof m);
    meta_member(&m[0], 0);
    c.n = 0; put8(&c, 0x01); putz(&c, 2); put8(&c, 0x00); putv(&c, 5); put8(&c, 0x01); putz(&c, -9);
    steps_members(&m[1], &m[2], &c, 0);
    craft("delta_first.ct", m, 3);

    /* 2. An unknown exec tag after two good records. */
    c.n = 0; put8(&c, 0x00); putv(&c, 3); put8(&c, 0x04); putv(&c, 7); put8(&c, 0x2a); putv(&c, 1);
    steps_members(&m[1], &m[2], &c, 0);
    craft("bad_tag_last.ct", m, 3);

    /* 3. An index with a trailing byte. */
    c.n = 0; put8(&c, 0x00); putv(&c, 3);
    steps_members(&m[1], &m[2], &c, 1);
    craft("idx_trailing.ct", m, 3);

    /* 4. A source-reload record in a trace that did not declare one. */
    c.n = 0; put8(&c, 0x00); putv(&c, 0); put8(&c, 0x08); putv(&c, 1); putv(&c, 1); putv(&c, 0); putv(&c, 1); putv(&c, 2); putv(&c, 0);
    steps_members(&m[1], &m[2], &c, 0);
    craft("undeclared_reload.ct", m, 3);

    /* 5. A chunk that is not a zstd frame. */
    m[1].data.n = 0; put(&m[1].data, "notzstd!", 8);
    craft("not_zstd.ct", m, 3);

    /* 6. Interning tables: an offset past the data, a truncated function
     * record, an empty type record, an offset file of odd size. */
    {
        buf_t rec; uint64_t ends[2];
        memset(m, 0, sizeof m);
        meta_member(&m[0], 0);
        rec.n = 0; put(&rec, "/a.py", 5); ends[0] = 5; ends[1] = 50;
        table_members(&m[1], &m[2], "paths", &rec, ends, 2);
        rec.n = 0; putv(&rec, 0); putv(&rec, 9); put(&rec, "f", 1); ends[0] = rec.n;
        table_members(&m[3], &m[4], "funcs", &rec, ends, 1);
        rec.n = 0; ends[0] = 0; put8(&rec, 7); putv(&rec, 3); put(&rec, "int", 3); ends[1] = rec.n;
        table_members(&m[5], &m[6], "types", &rec, ends, 2);
        m[7].name = "varnames.off"; m[7].data.n = 0; put32(&m[7].data, 0);
        craft("bad_tables.ct", m, 8);
    }

    /* 7. A source view record whose content is truncated. */
    {
        buf_t rec; uint64_t ends[1];
        memset(m, 0, sizeof m);
        meta_member(&m[0], 0);
        rec.n = 0; putv(&rec, 0); put8(&rec, 1); putv(&rec, 1); put(&rec, "n", 1); putv(&rec, 50); put(&rec, "abc", 3);
        ends[0] = rec.n;
        table_members(&m[1], &m[2], "srcviews", &rec, ends, 1);
        craft("bad_srcviews.ct", m, 3);
    }

    /* 7b. meta.dat present but not readable. */
    memset(m, 0, sizeof m);
    m[0].name = "meta.dat"; put(&m[0].data, bad_meta, sizeof bad_meta);
    craft("bad_meta.ct", m, 1);

    /* 8. Value, call and event streams whose one chunk is malformed. */
    {
        const char* streams[] = { "values", "calls", "events" };
        for (int k = 0; k < 3; k++) {
            char dn[32], in[32], file[64];
            memset(m, 0, sizeof m);
            meta_member(&m[0], 0);
            c.n = 0; put8(&c, 0x00); putv(&c, 0); put8(&c, 0x01); putz(&c, 1);
            steps_members(&m[1], &m[2], &c, 0);
            snprintf(dn, sizeof dn, "%s.dat", streams[k]);
            snprintf(in, sizeof in, "%s.idx", streams[k]);
            c.n = 0; putv(&c, 3); put8(&c, 0x7f); put8(&c, 0xff); put8(&c, 0xff);
            m[3].name = strdup(dn); put_frame(&m[3].data, &c);
            m[4].name = strdup(in); put32(&m[4].data, 64); put64(&m[4].data, 0);
            snprintf(file, sizeof file, "bad_%s.ct", streams[k]);
            craft(file, m, 5);
        }
    }
    return 0;
}

/* ------------------------------------------------------------------------ */

static void read_all(ct_reader_t r) {
    uint64_t steps, calls, events, paths, funcs, types, varnames;
    CALL_U("step_count", steps = ct_reader_step_count(r));
    CALL_U("call_count", calls = ct_reader_call_count(r));
    CALL_U("event_count", events = ct_reader_event_count(r));
    CALL_U("path_count", paths = ct_reader_path_count(r));
    CALL_U("function_count", funcs = ct_reader_function_count(r));
    CALL_U("type_count", types = ct_reader_type_count(r));
    CALL_U("varname_count", varnames = ct_reader_varname_count(r));
    TEXT("program", ct_reader_program(r, &_n));
    TEXT("workdir", ct_reader_workdir(r, &_n));
    CALL_I("has_column_aware_steps", ct_reader_has_column_aware_steps(r));
    CALL_I("column_aware_paths_suspected", ct_reader_column_aware_paths_suspected(r));
    CALL_I("supports_column_breakpoints", ct_reader_supports_column_breakpoints(r));
    CALL_I("supports_column_motions", ct_reader_supports_column_motions(r));
    for (uint64_t i = 0; i <= paths; i++) {
        char label[64];
        uint32_t v = 0;
        snprintf(label, sizeof label, "path %llu", (unsigned long long)i);
        TEXT(label, ct_reader_path(r, i, &_n));
        CALL_I("  path_table_kind", ct_reader_path_table_kind(r, i));
        CALL_U("  line_count_raw", ct_reader_line_count_raw(r, i));
        for (uint32_t l = 0; l < 6; l++) {
            int rc;
            CLR(); v = 0;
            rc = ct_reader_line_length(r, i, l, &v);
            printf("  line_length %u -> %d %u err=%d\n", l, rc, v, err_set());
            CLR(); v = 0;
            rc = ct_reader_line_length_raw(r, i, l, &v);
            printf("  line_length_raw %u -> %d %u err=%d\n", l, rc, v, err_set());
        }
    }
    for (uint64_t i = 0; i <= funcs; i++) {
        char label[64];
        snprintf(label, sizeof label, "function %llu", (unsigned long long)i);
        TEXT(label, ct_reader_function(r, i, &_n));
    }
    for (uint64_t i = 0; i <= types; i++) {
        char label[64];
        snprintf(label, sizeof label, "type %llu", (unsigned long long)i);
        TEXT(label, ct_reader_type_name(r, i, &_n));
    }
    for (uint64_t i = 0; i <= varnames; i++) {
        char label[64];
        snprintf(label, sizeof label, "varname %llu", (unsigned long long)i);
        TEXT(label, ct_reader_varname(r, i, &_n));
    }
    for (uint64_t i = 0; i <= steps; i++) {
        char label[64];
        uint64_t pid = 0, line = 0, nv;
        int rc;
        snprintf(label, sizeof label, "step %llu", (unsigned long long)i);
        TEXT(label, ct_reader_step(r, i, &_n));
        CLR();
        rc = ct_reader_step_location(r, i, &pid, &line);
        printf("  location -> %d %llu:%llu err=%d\n", rc, (unsigned long long)pid, (unsigned long long)line, err_set());
        TEXT("  values", ct_reader_values(r, i, &_n));
        CALL_U("  value_count", nv = ct_reader_step_value_count(r, i));
        for (uint64_t k = 0; k <= nv; k++) {
            uint64_t vid = 0, tid = 0;
            uint8_t* data = NULL;
            size_t dl = 0;
            CLR();
            rc = ct_reader_step_value(r, i, k, &vid, &tid, &data, &dl);
            printf("  value %llu -> %d varname=%llu type=%llu err=%d ", (unsigned long long)k, rc,
                   (unsigned long long)vid, (unsigned long long)tid, err_set());
            print_bytes("data", data, dl);
            if (data) ct_free_buffer(data);
        }
        TEXT("  call_for_step", ct_reader_call_for_step(r, i, &_n));
    }
    {
        uint64_t n = steps + 2;
        uint64_t* a = calloc(n, 8);
        uint64_t* b = calloc(n, 8);
        uint64_t* c = calloc(n, 8);
        uint64_t got;
        CALL_U("step_locations", got = ct_reader_step_locations(r, 0, n, a, b));
        for (uint64_t i = 0; i < got && got != UINT64_MAX; i++) printf("  %llu:%llu\n", (unsigned long long)a[i], (unsigned long long)b[i]);
        CALL_U("step_locations_with_columns", got = ct_reader_step_locations_with_columns(r, 0, n, a, b, c));
        for (uint64_t i = 0; i < got && got != UINT64_MAX; i++)
            printf("  %llu:%llu:%llu\n", (unsigned long long)a[i], (unsigned long long)b[i], (unsigned long long)c[i]);
        CALL_U("step_global_line_indices", got = ct_reader_step_global_line_indices(r, 0, n, a));
        for (uint64_t i = 0; i < got && got != UINT64_MAX; i++) printf("  %llu\n", (unsigned long long)a[i]);
        CALL_U("step_locations from 1, 1", got = ct_reader_step_locations(r, 1, 1, a, b));
        CALL_U("step_locations past end", ct_reader_step_locations(r, steps + 5, 2, a, b));
        CALL_U("step_locations count 0", ct_reader_step_locations(r, 0, 0, a, b));
        CALL_U("step_locations NULL out", ct_reader_step_locations(r, 0, 1, NULL, b));
        free(a); free(b); free(c);
    }
    for (uint64_t k = 0; k <= calls; k++) {
        char label[64];
        uint64_t fid = 0, entry = 0, exit_ = 0, nchildren = 0, nargs;
        int64_t parent = 0;
        uint32_t depth = 0;
        int rc;
        snprintf(label, sizeof label, "call %llu", (unsigned long long)k);
        TEXT(label, ct_reader_call(r, k, &_n));
        CLR();
        rc = ct_reader_call_fields(r, k, &fid, &parent, &entry, &exit_, &depth, &nchildren);
        printf("  fields -> %d f=%llu parent=%lld entry=%llu exit=%llu depth=%u children=%llu err=%d\n", rc,
               (unsigned long long)fid, (long long)parent, (unsigned long long)entry, (unsigned long long)exit_, depth,
               (unsigned long long)nchildren, err_set());
        for (uint64_t c = 0; c <= nchildren; c++) CALL_U("  child", ct_reader_call_child(r, k, c));
        CALL_U("  arg_count", nargs = ct_reader_call_arg_count(r, k));
        for (uint64_t a = 0; a <= nargs; a++) {
            uint64_t vid = 0;
            uint8_t* data = NULL;
            size_t dl = 0;
            CLR();
            rc = ct_reader_call_arg(r, k, a, &vid, &data, &dl);
            printf("  arg %llu -> %d varname=%llu err=%d ", (unsigned long long)a, rc, (unsigned long long)vid, err_set());
            print_bytes("data", data, dl);
            if (data) ct_free_buffer(data);
        }
    }
    for (uint64_t i = 0; i <= events; i++) {
        char label[64];
        uint8_t kind = 0xee;
        uint64_t step = 0;
        uint8_t* data = NULL;
        size_t dl = 0;
        int rc;
        snprintf(label, sizeof label, "event %llu", (unsigned long long)i);
        TEXT(label, ct_reader_event(r, i, &_n));
        CLR();
        rc = ct_reader_event_fields(r, i, &kind, &step, &data, &dl);
        printf("  fields -> %d kind=%u step=%llu err=%d ", rc, kind, (unsigned long long)step, err_set());
        print_bytes("data", data, dl);
        if (data) ct_free_buffer(data);
        data = NULL; dl = 0;
        CLR();
        rc = ct_reader_event_metadata(r, i, &data, &dl);
        printf("  metadata -> %d err=%d ", rc, err_set());
        print_bytes("data", data, dl);
        if (data) ct_free_buffer(data);
    }
}

static int read_file(const char* path) {
    ct_reader_t r;
    FILE* f;
    long size;
    uint8_t* bytes;
    printf("== %s\n", strrchr(path, '/') ? strrchr(path, '/') + 1 : path);
    CLR();
    r = ct_reader_open(path);
    printf("open -> %s err=%d\n", r ? "handle" : "NULL", err_set());
    if (r) {
        read_all(r);
        CALL_I("refresh", ct_reader_refresh(r, path));
        CALL_U("step_count after refresh", ct_reader_step_count(r));
        CALL_I("refresh NULL path", ct_reader_refresh(r, NULL));
        CALL_V("close", ct_reader_close(r));
    }
    CLR();
    r = ct_reader_open_assume_column_aware_paths(path);
    printf("open_assume_column_aware_paths -> %s err=%d\n", r ? "handle" : "NULL", err_set());
    if (r) {
        CALL_U("step_count", ct_reader_step_count(r));
        CALL_I("path_table_kind 0", ct_reader_path_table_kind(r, 0));
        CALL_V("close", ct_reader_close(r));
    }
    f = fopen(path, "rb");
    if (f == NULL) {
        printf("(no file)\n");
    } else {
        fseek(f, 0, SEEK_END);
        size = ftell(f);
        fseek(f, 0, SEEK_SET);
        bytes = malloc(size > 0 ? size : 1);
        if (fread(bytes, 1, size, f) != (size_t)size) size = 0;
        fclose(f);
        CLR();
        r = ct_reader_open_bytes(bytes, size);
        free(bytes);
        printf("open_bytes -> %s err=%d\n", r ? "handle" : "NULL", err_set());
        if (r) {
            read_all(r);
            CALL_V("close", ct_reader_close(r));
        }
    }
    return 0;
}

static int scenario_reader_nulls(void) {
    uint64_t u = 0;
    int64_t s = 0;
    uint32_t v = 0;
    uint8_t* d = NULL;
    size_t n = 0;
    uint8_t k = 0;
    CALL_I("open NULL", ct_reader_open(NULL) != NULL);
    CALL_I("open_assume NULL", ct_reader_open_assume_column_aware_paths(NULL) != NULL);
    CALL_I("open_bytes NULL len 3", ct_reader_open_bytes(NULL, 3) != NULL);
    CALL_I("open_bytes NULL len 0", ct_reader_open_bytes(NULL, 0) != NULL);
    CALL_I("refresh NULL", ct_reader_refresh(NULL, "x"));
    CALL_V("close NULL", ct_reader_close(NULL));
    CALL_U("step_count NULL", ct_reader_step_count(NULL));
    CALL_U("call_count NULL", ct_reader_call_count(NULL));
    CALL_U("event_count NULL", ct_reader_event_count(NULL));
    CALL_U("path_count NULL", ct_reader_path_count(NULL));
    CALL_U("function_count NULL", ct_reader_function_count(NULL));
    CALL_U("type_count NULL", ct_reader_type_count(NULL));
    CALL_U("varname_count NULL", ct_reader_varname_count(NULL));
    TEXT("path NULL", ct_reader_path(NULL, 0, &_n));
    TEXT("function NULL", ct_reader_function(NULL, 0, &_n));
    TEXT("type_name NULL", ct_reader_type_name(NULL, 0, &_n));
    TEXT("varname NULL", ct_reader_varname(NULL, 0, &_n));
    TEXT("program NULL", ct_reader_program(NULL, &_n));
    TEXT("workdir NULL", ct_reader_workdir(NULL, &_n));
    TEXT("step NULL", ct_reader_step(NULL, 0, &_n));
    TEXT("values NULL", ct_reader_values(NULL, 0, &_n));
    TEXT("call NULL", ct_reader_call(NULL, 0, &_n));
    TEXT("call_for_step NULL", ct_reader_call_for_step(NULL, 0, &_n));
    TEXT("event NULL", ct_reader_event(NULL, 0, &_n));
    CALL_I("step_location NULL", ct_reader_step_location(NULL, 0, &u, &u));
    CALL_U("step_locations NULL", ct_reader_step_locations(NULL, 0, 1, &u, &u));
    CALL_U("step_locations_with_columns NULL", ct_reader_step_locations_with_columns(NULL, 0, 1, &u, &u, &u));
    CALL_U("step_global_line_indices NULL", ct_reader_step_global_line_indices(NULL, 0, 1, &u));
    CALL_I("line_length NULL", ct_reader_line_length(NULL, 0, 0, &v));
    CALL_I("line_length_raw NULL", ct_reader_line_length_raw(NULL, 0, 0, &v));
    CALL_U("line_count_raw NULL", ct_reader_line_count_raw(NULL, 0));
    CALL_I("path_table_kind NULL", ct_reader_path_table_kind(NULL, 0));
    CALL_I("has_column_aware_steps NULL", ct_reader_has_column_aware_steps(NULL));
    CALL_I("column_aware_paths_suspected NULL", ct_reader_column_aware_paths_suspected(NULL));
    CALL_I("supports_column_breakpoints NULL", ct_reader_supports_column_breakpoints(NULL));
    CALL_I("supports_column_motions NULL", ct_reader_supports_column_motions(NULL));
    CALL_U("step_value_count NULL", ct_reader_step_value_count(NULL, 0));
    CALL_I("step_value NULL", ct_reader_step_value(NULL, 0, 0, &u, &u, &d, &n));
    CALL_I("call_fields NULL", ct_reader_call_fields(NULL, 0, &u, &s, &u, &u, &v, &u));
    CALL_U("call_child NULL", ct_reader_call_child(NULL, 0, 0));
    CALL_U("call_arg_count NULL", ct_reader_call_arg_count(NULL, 0));
    CALL_I("call_arg NULL", ct_reader_call_arg(NULL, 0, 0, &u, &d, &n));
    CALL_I("event_fields NULL", ct_reader_event_fields(NULL, 0, &k, &u, &d, &n));
    CALL_I("event_metadata NULL", ct_reader_event_metadata(NULL, 0, &d, &n));
    return 0;
}

int main(int argc, char** argv) {
    const char* s;
    if (argc < 3) {
        fprintf(stderr, "usage: parity_host <scenario> <dir> [file...]\n");
        return 2;
    }
    codetracer_trace_writer_init();
    s = argv[1];
    g_dir = argv[2];
    setvbuf(stdout, NULL, _IOFBF, 1 << 16);
    if (strcmp(s, "basic") == 0) return scenario_basic();
    if (strcmp(s, "threads") == 0) return scenario_threads();
    if (strcmp(s, "columns") == 0) return scenario_columns();
    if (strcmp(s, "late_columns") == 0) return scenario_late_columns();
    if (strcmp(s, "linecounts") == 0) return scenario_linecounts();
    if (strcmp(s, "undeclared_reload") == 0) return scenario_undeclared_reload();
    if (strcmp(s, "no_line_count_versions") == 0) return scenario_no_line_count_versions();
    if (strcmp(s, "values") == 0) return scenario_values();
    if (strcmp(s, "meta") == 0) return scenario_meta();
    if (strcmp(s, "in_memory") == 0) return scenario_in_memory();
    if (strcmp(s, "trailing") == 0) return scenario_trailing();
    if (strcmp(s, "free_closes") == 0) return scenario_free_closes();
    if (strcmp(s, "nulls") == 0) return scenario_nulls();
    if (strcmp(s, "not_ready") == 0) return scenario_not_ready();
    if (strcmp(s, "container") == 0) return scenario_container();
    if (strcmp(s, "reader_nulls") == 0) return scenario_reader_nulls();
    if (strcmp(s, "craft") == 0) return scenario_craft();
    if (strcmp(s, "read") == 0) {
        for (int i = 3; i < argc; i++) read_file(argv[i]);
        return 0;
    }
    fprintf(stderr, "unknown scenario %s\n", s);
    return 2;
}
