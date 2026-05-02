#include "../include-c/device.h"
#include <stdio.h>

void run_ndp_job(const command_entry_t* cmd) {
    printf("Command CID: %u, Opcode: %d, Num Vertices: %u\n", cmd->cid, cmd->opcode, cmd->num_vertices);
    switch (cmd->opcode) {
        case OPCODE_GEN_UPDATES:
            for (vid_t i = 0; i < cmd->num_vertices; ++i) {
                if (!get_bit(cmd->frontier_ndp, i)) {
                    continue;
                }
                
                float contribution = cmd->vprops_mirror[i].pr;
                for (vid_t nbr_idx = cmd->graph->row_ptr[i]; nbr_idx < cmd->graph->row_ptr[i + 1]; ++nbr_idx) {
                    vid_t nbr = cmd->graph->col_idx[nbr_idx];
                    cmd->vprops_mirror[nbr].delta += contribution;
                }
            }
            break;
        default:
            printf("Unknown opcode: %d\n", cmd->opcode);
    }
}

