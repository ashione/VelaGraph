#ifndef VELAGRAPH_NATIVE_H
#define VELAGRAPH_NATIVE_H
#include <stdint.h>
#ifdef __cplusplus
extern "C" {
#endif
#define VG_ABI_VERSION 1u
#define VG_ERROR_BYTES 256u
/* ABI1 JSON schema and ownership: docs/NATIVE_FFI.md. */
typedef uint64_t vg_handle;
typedef struct vg_reply { uint64_t value; vg_handle buffer; char error[VG_ERROR_BYTES]; } vg_reply;
typedef struct vg_stats { uint64_t registered_handles, wire_reserved_bytes, plan_reserved_bytes, active_calls; } vg_stats;
enum vg_status { VG_OK=0, VG_INVALID_ARGUMENT=1, VG_INVALID_HANDLE=2, VG_UNSUPPORTED=3,
 VG_LIMIT=4, VG_CONFLICT=5, VG_CANCELLED=6, VG_DEADLINE=7, VG_IO=8, VG_INTERNAL=9,
 VG_OUTCOME_UNKNOWN=10, VG_BUFFER_TOO_SMALL=11, VG_BUSY=12, VG_CORRUPT=13 };
enum vg_operation { VG_OPEN=1, VG_APPLY=2, VG_QUERY_BATCH=3, VG_INFO=4,
 VG_CHECKPOINT=5, VG_PREPARE=6, VG_QUERY_PACKED=7 };
uint32_t vg_abi_version(void);
uint32_t vg_call(uint32_t abi, uint32_t operation, vg_handle store,
 const uint8_t *input, uint64_t input_len, vg_handle control,
 uint64_t timeout_ms, uint64_t max_output_bytes, vg_reply *out);
/* Release retires the handle. Admitted calls keep their own references. */
uint32_t vg_release(vg_handle handle);
uint32_t vg_control_new(vg_handle *out);
uint32_t vg_cancel(vg_handle control);
/* The independent lease keeps immutable bytes alive after buffer retirement.
 * Release view.lease only after all pointer accesses finish. */
typedef struct vg_buffer_view { vg_handle lease; const uint8_t *data; uint64_t len; } vg_buffer_view;
uint32_t vg_buffer_acquire(vg_handle buffer, vg_buffer_view *out);
uint32_t vg_buffer_size(vg_handle buffer, uint64_t *out_len);
uint32_t vg_buffer_copy(vg_handle buffer, uint8_t *out, uint64_t capacity);
/* Best-effort counters; exact leak checks require callers to be quiescent. */
uint32_t vg_runtime_stats(vg_stats *out);
#ifdef __cplusplus
}
#endif
#endif
