#include "../include-c/device.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

// Host implementation
void* host_malloc(size_t size) { return malloc(size); }
void* host_calloc(size_t count, size_t size) { return calloc(count, size); }
void host_free(void* ptr) { free(ptr); }

// Simulated CXL Memory
void* cxl_malloc(size_t size) { return malloc(size); }
void* cxl_calloc(size_t count, size_t size) { 
    return cxl_malloc(count * size);
}
void cxl_free(void* ptr) { free(ptr); }

void cxl_flush(void* ptr, size_t size) {
    // No-op
}

void cxl_memcpy_to_device(void* cxl_dest, const void* host_src, size_t size) {
    memcpy(cxl_dest, host_src, size);
    cxl_flush(cxl_dest, size);
}

void cxl_memcpy_to_host(void* host_dest, const void* cxl_src, size_t size) {
    cxl_flush(cxl_src, size);
    memcpy(host_dest, cxl_src, size);
}

void cxl_wait_done(uint32_t cid) {
    return;
}

void cxl_send_cmd(const command_entry_t* cmd) {
    // Simulated offload
    run_ndp_job(cmd);
}

void run_ndp_job(const command_entry_t* cmd) {
    printf("Command CID: %u, Opcode: %d, Num Vertices: %u\n", cmd->cid, cmd->opcode, cmd->num_vertices);
    switch (cmd->opcode) {
        case OPCODE_GEN_UPDATES:
            for (vid_t i = 0; i < cmd->num_vertices; ++i) {
                if (get_bit(cmd->frontier_ndp, i)) {
                    float contribution = cmd->vprops_mirror[i].pr;
                    for (vid_t nbr_idx = cmd->graph->row_ptr[i]; nbr_idx < cmd->graph->row_ptr[i + 1]; ++nbr_idx) {
                        vid_t nbr = cmd->graph->col_idx[nbr_idx];
                        cmd->vprops_mirror[nbr].delta += contribution;
                    }
                }
            }
            break;
        default:
            printf("Unknown opcode: %d\n", cmd->opcode);
    }

}

