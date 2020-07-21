/*
 * Copyright (c) 2019 Mellanox Technologies, Inc.  All rights reserved.
 *
 * This software is available to you under a choice of one of two
 * licenses.  You may choose to be licensed under the terms of the GNU
 * General Public License (GPL) Version 2, available from the file
 * COPYING in the main directory of this source tree, or the
 * OpenIB.org BSD license below:
 *
 *     Redistribution and use in source and binary forms, with or
 *     without modification, are permitted provided that the following
 *     conditions are met:
 *
 *      - Redistributions of source code must retain the above
 *        copyright notice, this list of conditions and the following
 *        disclaimer.
 *
 *      - Redistributions in binary form must reproduce the above
 *        copyright notice, this list of conditions and the following
 *        disclaimer in the documentation and/or other materials
 *        provided with the distribution.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#ifndef _GPU_DIRECT_RDMA_ACCESS_H_
#define _GPU_DIRECT_RDMA_ACCESS_H_

#include <sys/uio.h> /* This file defines `struct iovec'  */

#include <infiniband/verbs.h>
#include <infiniband/mlx5dv.h>

#ifdef __cplusplus
extern "C" {
#endif

#define MAX_SEND_SGE    10

/*
 * rdma_device object holds the RDMA resources of the local RDMA device,
 * of a Targte or a Source
 */
struct rdma_device;

/*
 * rdma_buffer is used to represent the rdma parameters of a
 * applciation buffer on a specific device to be used in the RDMA operations
 */
struct rdma_buffer;

struct rdma_open_dev_attr {
    const char      *ib_devname;
    int             ib_port;
    int             gidx;
};

enum rdma_task_attr_flags {
        RDMA_TASK_ATTR_RDMA_READ = 1 << 0,
};

struct rdma_task_attr {
        char                    *remote_buf_desc_str;
        size_t                   remote_buf_desc_length;
        size_t                   remote_buf_offset;
        struct rdma_buffer      *local_buf_rdma;
        struct iovec            *local_buf_iovec;
        int                      local_buf_iovcnt;
        uint32_t                 flags; /* Use enum rdma_task_attr_flags */
        uint64_t                 wr_id;
};
/*
 * Open a RDMA device and allocated requiered resources.
 * find the capable RDMA device based on the 'addr' as an ip address
 * of the RDMA device selected to preform the RDMA operations.
 * Creates a PD, CQ, and QP as internal HW resources.
 *
 * Source rdma_device preforms the RDMA Read/Write operations to
 * the Target rdma_device.
 *
 * returns: a pointer to a rdma_device object or NULL on error
 */
struct rdma_device *rdma_open_device_client(struct sockaddr *addr);
struct rdma_device *rdma_open_device_server(struct sockaddr *addr);

/*
 * Reset device from failed state back to an operations state 
 */
int rdma_reset_device(struct rdma_device *device);

/*
 * Close and release all rdma_device resources
 */
void rdma_close_device(struct rdma_device *device);

/*
 * register and deregister an applciation buffer with the RDMA device
 */
struct rdma_buffer *rdma_buffer_reg(struct rdma_device *device, void *addr, size_t length);
void rdma_buffer_dereg(struct rdma_buffer *buffer);

/*
 * Get a rdma_buffer address description string representations
 *
 * The Client application should pass this description string to the
 * Server which will issue the RDMA Read/Write operation
 *
 * desc_str is input and output holding the rdma_buffer information
 * desc_length is input size in bytes of desc_str
 *
 * returns: an integer equal to the size of the char data copied into desc_str
 */
int rdma_buffer_get_desc_str(struct rdma_buffer *rdma_buff, char *desc_str, size_t desc_length);

/*
 * Issue a RDMA WRITE operation from a local buffer to a remote buffer, 
 * or a RDMA READ operation from remote buffer to a local buffer,
 * depending on the RDMA_TASK_ATTR_RDMA_READ flag.
 * Remote buffer is descibed by the remote_buffer_addr_str, starting at offset remote_buf_offset.
 * The local_iov gather list, of size local_iovcnt, hold the buffer addr & size
 * pairs, and should be in the range of the local_buffer, which holds relevant
 * the rdma info.
 * We don't pass struct rdma_device as parameter, because we can get it using
 * rdma_task_attr struct field local_buf_rdma
 *
 * On completion of the RDMA operation, the status and wr_id will be reported
 * from rdma_poll_completions()
 *
 * returns: 0 on success, or the value of errno on failure
 */
int rdma_submit_task(struct rdma_task_attr *attr);

enum rdma_completion_status {
	RDMA_STATUS_SUCCESS,
	RDMA_STATUS_ERR_LAST,
};

struct rdma_completion_event {
	uint64_t                    wr_id;
	enum rdma_completion_status status;
};

/*
 * Return rdma operations which have completed.
 * the event will hold the requets id (wr_id) and the status of the operation.
 *
 * returns: number of reported events in the event array (<= num_entries)
 */
int rdma_poll_completions(struct rdma_device *device,
		struct rdma_completion_event *event,
		uint32_t num_entries);

#ifdef __cplusplus
}
#endif

#endif /* _GPU_DIRECT_RDMA_ACCESS_H_ */
