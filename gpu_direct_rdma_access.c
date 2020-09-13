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

#if HAVE_CONFIG_H
#  include <config.h>
#endif /* HAVE_CONFIG_H */

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <netdb.h>
#include <malloc.h>
#include <getopt.h>
#include <arpa/inet.h>
#include <time.h>

#include <rdma/rdma_cma.h>
#include <infiniband/mlx5dv.h>

#include "khash.h"
#include "ibv_helper.h"
#include "gpu_direct_rdma_access.h"

int debug = 0;
int debug_fast_path = 0;

#define DEBUG_LOG if (debug) printf
#define DEBUG_LOG_FAST_PATH if (debug_fast_path) printf
#define FDEBUG_LOG if (debug) fprintf
#define FDEBUG_LOG_FAST_PATH if (debug_fast_path) fprintf

#define CQ_DEPTH        640
#define SEND_Q_DEPTH    640 
#define DC_KEY          0xffeeddcc  /*this is defined for both sides: client and server*/
#define COMP_ARRAY_SIZE 16
#define TC_PRIO         3

#define WR_ID_FLUSH_MARKER UINT64_MAX  

#define mmin(a, b)      a < b ? a : b

KHASH_TYPE(kh_ib_ah, struct ibv_ah_attr, struct ibv_ah*);

enum wr_id_flags {
	WR_ID_FLAGS_ACTIVE = 1 << 0
};

struct wr_id_reported {
    uint64_t 	wr_id;
    uint16_t	num_wrs;
    uint16_t	flags; /* enum wr_id_flags */
};

#ifdef PRINT_LATENCY
struct wr_latency {
    uint64_t    wr_start_ts;
    uint64_t    wr_complete_ts;
    uint64_t    completion_ts;
    uint64_t    read_comp_ts;
};
#endif /*PRINT_LATENCY*/

struct rdma_device {

    struct rdma_event_channel *cm_channel;
    struct rdma_cm_id *cm_id;

    struct ibv_context *context;
    struct ibv_pd      *pd;
#ifdef PRINT_LATENCY
    struct ibv_cq_ex   *cq;
#else
    struct ibv_cq      *cq;
#endif
    struct ibv_srq     *srq; /* for DCT (client) only, for DCI (server) this is NULL */
    struct ibv_qp      *qp;
    struct ibv_qp_ex       *qpex;  /* DCI (server) only */
    struct mlx5dv_qp_ex    *mqpex; /* DCI (server) only */
    
    /* Address handler (port info) relateed fields */
    int                 ib_port;
    int                 is_global;
    int                 gidx;
    union ibv_gid       gid;
    uint16_t            lid;
    enum ibv_mtu        mtu;

    struct wr_id_reported       app_wr_id[SEND_Q_DEPTH];
    int                 app_wr_id_idx;
    int                 qp_available_wr;
    int                 rdma_buff_cnt;

    /* AH hash */
    khash_t(kh_ib_ah)   ah_hash;
#ifdef PRINT_LATENCY
    uint64_t            hca_core_clock_kHz;
    struct wr_latency   latency[SEND_Q_DEPTH];
    uint64_t    measure_index;
    uint64_t    wr_complete_latency_sum; /*from wr_start_ts*/
    uint64_t    completion_latency_sum; /*from wr_start_ts*/
    uint64_t    read_comp_latency_sum; /*from completion_ts*/
    uint64_t    min_wr_complete_latency;
    uint64_t    min_completion_latency;
    uint64_t    min_read_comp_latency;
    uint64_t    max_wr_complete_latency;
    uint64_t    max_completion_latency;
    uint64_t    max_read_comp_latency;
#endif /*PRINT_LATENCY*/
};

struct rdma_buffer {
    /* Buffer Related fields */
    void               *buf_addr;   //uint64_t  addr;
    size_t              buf_size;   //uint32_t  size;
    /* MR Related fields */
    struct ibv_mr      *mr;
    uint32_t            rkey;
    /* Linked rdma_device */
    struct rdma_device *rdma_dev;
};

struct rdma_exec_params {
	struct rdma_device 	*device;
	uint64_t 		 wr_id;
	unsigned long		 rem_buf_rkey;
	unsigned long long 	 rem_buf_addr;
	uint32_t 		 rem_buf_size;
	struct ibv_ah 		*ah;
	unsigned long 		 rem_dctn; /*QP number from DCT (client)*/
	uint32_t 		 local_buf_mr_lkey;
	void 			*local_buf_addr;
	struct iovec            *local_buf_iovec;
	int                      local_buf_iovcnt;
	uint32_t 		 flags; /*enum rdma_task_attr_flags*/
};

static inline
int is_server(struct rdma_device *device)
{
	return device->srq == NULL;
}

/* use both gid + lid data for key generarion (lid - ib based, gid - RoCE) */
static inline
khint32_t kh_ib_ah_hash_func(struct ibv_ah_attr attr)
{
    return kh_int64_hash_func(attr.grh.dgid.global.subnet_prefix ^
                              attr.grh.dgid.global.interface_id  ^
                              attr.dlid);
}

static inline
int kh_ib_ah_hash_equal(struct ibv_ah_attr a, struct ibv_ah_attr b)
{
    return !memcmp(&a, &b, sizeof(a));
}

KHASH_IMPL(kh_ib_ah, struct ibv_ah_attr, struct ibv_ah*, 1,
           kh_ib_ah_hash_func, kh_ib_ah_hash_equal)


//============================================================================================
static struct ibv_context *open_ib_device_by_name(const char *ib_dev_name)
{
    struct ibv_device **dev_list;
    struct ibv_device  *ib_dev;
    struct ibv_context *context = NULL;

    /****************************************************************************************************
     * In the next block we are checking if given IB device name matches one of devices in the list.
     * The result of this block is ig_dev - initialized pointer to the relevant struct ibv_device
     ****************************************************************************************************/
    dev_list = ibv_get_device_list(NULL);
    if (!dev_list) {
        perror("Failed to get IB devices list");
        return NULL;
    }

    DEBUG_LOG ("Given device name \"%s\"\n", ib_dev_name);
    int i;
    for (i = 0; dev_list[i]; ++i) {
        char *dev_name_from_list = (char*)ibv_get_device_name(dev_list[i]);
        DEBUG_LOG ("Device %d name \"%s\"\n", i, dev_name_from_list);
        if (!strcmp(dev_name_from_list, ib_dev_name)) /*if found*/
            break;
    }
    ib_dev = dev_list[i];
    if (!ib_dev) {
        fprintf(stderr, "IB device %s not found\n", ib_dev_name);
        goto clean_device_list;
    }
    /****************************************************************************************************/

    DEBUG_LOG ("ibv_open_device(ib_dev = %p)\n", ib_dev);
    context = ibv_open_device(ib_dev);
    if (!context) {
        fprintf(stderr, "Couldn't get context for %s\n", ib_dev_name);
        goto clean_device_list;
    }
    DEBUG_LOG("created ib context %p\n", context);
    /* We are now done with device list, we can free it */
    
clean_device_list:
    ibv_free_device_list(dev_list); /*dev_list is not NULL*/

    return context;
}

//============================================================================================
static struct ibv_context *open_ib_device_by_addr(struct rdma_device *rdma_dev, struct sockaddr *addr)
{
    int ret;
    uint16_t sin_port;
    char str[INET_ADDRSTRLEN];

        rdma_dev->cm_channel = rdma_create_event_channel();
        if (!rdma_dev->cm_channel) {
                DEBUG_LOG("rdma_create_event_channel() failure");
        return NULL;
        }

        ret = rdma_create_id(rdma_dev->cm_channel, &rdma_dev->cm_id, rdma_dev, RDMA_PS_UDP);
        if (ret) {
                DEBUG_LOG("rdma_create_id() failure");
                goto out1;
        }

    ret = rdma_bind_addr(rdma_dev->cm_id, addr);
    if (ret) {
        DEBUG_LOG("rdma_bind_addr() failure");
                goto out2;
    }

        if (addr->sa_family == AF_INET) {
        sin_port = ((struct sockaddr_in *)addr)->sin_port;
                inet_ntop(AF_INET, &(((struct sockaddr_in *)addr)->sin_addr), str, INET_ADDRSTRLEN);
        }
        else {
        sin_port = ((struct sockaddr_in6 *)addr)->sin6_port;
                inet_ntop(AF_INET6, &(((struct sockaddr_in6 *)addr)->sin6_addr), str, INET_ADDRSTRLEN);
    }

    if (rdma_dev->cm_id->verbs == NULL) {
        DEBUG_LOG("Failed to bind to an RDMA device, exiting... <%s, %d>\n", str, ntohs(sin_port));
        goto out2;
    }

    rdma_dev->ib_port = rdma_dev->cm_id->port_num;
    rdma_dev->gidx = -1;

    DEBUG_LOG("bound to RDMA device name:%s, port:%d, based on '%s'\n",
              rdma_dev->cm_id->verbs->device->name, rdma_dev->cm_id->port_num, str); 

    return rdma_dev->cm_id->verbs;

out2:
    rdma_destroy_id(rdma_dev->cm_id);
out1:
    rdma_destroy_event_channel(rdma_dev->cm_channel);
    return NULL;

}

static void close_ib_device(struct rdma_device *rdma_dev)
{
    int ret;

    if (rdma_dev->cm_channel) {

        /* if we are using RDMA_CM then we just referance the cma's ibv_context */
    rdma_dev->context = NULL;

        if (rdma_dev->cm_id) {
            DEBUG_LOG("rdma_destroy_id(%p)\n", rdma_dev->cm_id);
            ret = rdma_destroy_id(rdma_dev->cm_id);
            if (ret) {
                fprintf(stderr, "failure in rdma_destroy_id(), error %d\n", ret);
            }
        }

        DEBUG_LOG("rdma_destroy_event_channel(%p)\n", rdma_dev->cm_id);
        rdma_destroy_event_channel(rdma_dev->cm_channel);
    }

    if (rdma_dev->context) {
        DEBUG_LOG("ibv_close_device(%p)\n", rdma_dev->context);
        ret = ibv_close_device(rdma_dev->context);
        if (ret) {
            fprintf(stderr, "failure in ibv_close_device(), error %d\n", ret);
        }
    }
}

/***********************************************************************************
 * Fill portinfo structure, get lid and gid from portinfo
 * Return value: 0 - success, 1 - error
 ****************************************************************************************/
static int rdma_set_lid_gid_from_port_info(struct rdma_device *rdma_dev)
{
    struct ibv_port_attr    portinfo;
    int    ret_val;

    ret_val = ibv_query_port(rdma_dev->context, rdma_dev->ib_port, &portinfo);
    if (ret_val) {
        fprintf(stderr, "Couldn't get port info\n");
        return 1;
    }

    rdma_dev->mtu = portinfo.active_mtu;
    rdma_dev->lid = portinfo.lid;
    if ((portinfo.link_layer != IBV_LINK_LAYER_ETHERNET) && (!portinfo.lid)) {
        fprintf(stderr, "Couldn't get local LID\n");
        return 1;
    }

    if (rdma_dev->cm_id && portinfo.link_layer == IBV_LINK_LAYER_ETHERNET) {
        rdma_dev->gidx = ibv_find_sgid_type(rdma_dev->context, rdma_dev->ib_port, 
                IBV_GID_TYPE_ROCE_V2, rdma_dev->cm_id->route.addr.src_addr.sa_family);
    }
    
    if (rdma_dev->gidx < 0) {
        if (portinfo.link_layer == IBV_LINK_LAYER_ETHERNET) {
            fprintf(stderr, "Wrong GID index (%d) for ETHERNET port\n", rdma_dev->gidx);
            return 1;
        } else {
            memset(&(rdma_dev->gid), 0, sizeof rdma_dev->gid);
        }
    } else /* rdma_dev->gidx >= 0*/ {
        ret_val = ibv_query_gid(rdma_dev->context, rdma_dev->ib_port, rdma_dev->gidx, &(rdma_dev->gid));
        if (ret_val) {
            fprintf(stderr, "can't read GID of index %d, error code %d\n", rdma_dev->gidx, ret_val);
            return 1;
        }
        DEBUG_LOG ("my gid idx: %d, value:%02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x\n", rdma_dev->gidx,
                   rdma_dev->gid.raw[0], rdma_dev->gid.raw[1], rdma_dev->gid.raw[2], rdma_dev->gid.raw[3],
                   rdma_dev->gid.raw[4], rdma_dev->gid.raw[5], rdma_dev->gid.raw[6], rdma_dev->gid.raw[7], 
                   rdma_dev->gid.raw[8], rdma_dev->gid.raw[9], rdma_dev->gid.raw[10], rdma_dev->gid.raw[11],
                   rdma_dev->gid.raw[12], rdma_dev->gid.raw[13], rdma_dev->gid.raw[14], rdma_dev->gid.raw[15] );
    }
    rdma_dev->is_global = (rdma_dev->gid.global.interface_id != 0);

    DEBUG_LOG ("link_layer:%s, lid:%d, is_global:%d, MTU:%d Bytes\n",
        (portinfo.link_layer == IBV_LINK_LAYER_ETHERNET ? "ETH" : "IB"),
        rdma_dev->lid, rdma_dev->is_global, (256<<(rdma_dev->mtu - 1)));

    return 0;
}

/****************************************************************************************
 * Modify target QP state to RTR (on the client side)
 * Return value: 0 - success, 1 - error
 ****************************************************************************************/
static int modify_target_qp_to_rtr(struct rdma_device *rdma_dev)
{
    struct ibv_qp_attr      qp_attr;
    enum ibv_qp_attr_mask   attr_mask;

    memset(&qp_attr, 0, sizeof qp_attr);
    qp_attr.qp_state       = IBV_QPS_RTR;
    qp_attr.path_mtu       = rdma_dev->mtu;
    qp_attr.min_rnr_timer  = 16;
    qp_attr.ah_attr.port_num    = rdma_dev->ib_port;

    if (rdma_dev->gid.global.interface_id) {
        qp_attr.ah_attr.is_global = 1;
        qp_attr.ah_attr.grh.hop_limit  = 1;
        qp_attr.ah_attr.grh.sgid_index = rdma_dev->gidx;
        qp_attr.ah_attr.grh.traffic_class = TC_PRIO << 5; // <<3 for dscp2prio, <<2 for ECN bits
    }
    attr_mask = IBV_QP_STATE          |
                IBV_QP_AV             |
                IBV_QP_PATH_MTU       |
                IBV_QP_MIN_RNR_TIMER; // for DCT

    DEBUG_LOG("ibv_modify_qp(qp = %p, qp_attr.qp_state = %d, attr_mask = 0x%x)\n",
               rdma_dev->qp, qp_attr.qp_state, attr_mask);
    if (ibv_modify_qp(rdma_dev->qp, &qp_attr, attr_mask)) {
        fprintf(stderr, "Failed to modify QP to RTR\n");
        return 1;
    }
    DEBUG_LOG ("ibv_modify_qp to state %d completed: qp_num = 0x%x\n", qp_attr.qp_state, rdma_dev->qp->qp_num);

    return 0;
}

/****************************************************************************************
 * Modify source QP state to RTR and then to RTS (on the server side)
 * Return value: 0 - success, 1 - error
 ****************************************************************************************/
static int modify_source_qp_to_rtr_and_rts(struct rdma_device *rdma_dev)
{
    struct ibv_qp_attr      qp_attr;
    enum ibv_qp_attr_mask   attr_mask;

    memset(&qp_attr, 0, sizeof qp_attr);
    
    /* - - - - - - -  Modify QP to RTR  - - - - - - - */
    qp_attr.qp_state = IBV_QPS_RTR;
    qp_attr.path_mtu = rdma_dev->mtu;
    qp_attr.ah_attr.port_num = rdma_dev->ib_port;

    if (rdma_dev->gid.global.interface_id) {
        qp_attr.ah_attr.is_global = 1;
        qp_attr.ah_attr.grh.hop_limit  = 1;
        qp_attr.ah_attr.grh.sgid_index = rdma_dev->gidx;
        qp_attr.ah_attr.grh.traffic_class = TC_PRIO << 5; // <<3 for dscp2prio, <<2 for ECN bits
    }
    attr_mask = IBV_QP_STATE    |
                IBV_QP_AV       |
                IBV_QP_PATH_MTU ;

    DEBUG_LOG("ibv_modify_qp(qp = %p, qp_attr.qp_state = %d, attr_mask = 0x%x)\n",
               rdma_dev->qp, qp_attr.qp_state, attr_mask);
    if (ibv_modify_qp(rdma_dev->qp, &qp_attr, attr_mask)) {
        fprintf(stderr, "Failed to modify QP to RTR\n");
        return 1;
    }
    DEBUG_LOG ("ibv_modify_qp to state %d completed: qp_num = 0x%lx\n", qp_attr.qp_state, rdma_dev->qp->qp_num);

    /* - - - - - - -  Modify QP to RTS  - - - - - - - */
    qp_attr.qp_state       = IBV_QPS_RTS;
    qp_attr.timeout        = 16;
    qp_attr.retry_cnt      = 7;
    qp_attr.rnr_retry      = 7;
    //qp_attr.sq_psn         = 0;
    qp_attr.max_rd_atomic  = 1;
    attr_mask = IBV_QP_STATE            |
                IBV_QP_TIMEOUT          |
                IBV_QP_RETRY_CNT        |
                IBV_QP_RNR_RETRY        |
                IBV_QP_SQ_PSN           |
                IBV_QP_MAX_QP_RD_ATOMIC ;
    DEBUG_LOG("ibv_modify_qp(qp = %p, qp_attr.qp_state = %d, attr_mask = 0x%x)\n",
               rdma_dev->qp, qp_attr.qp_state, attr_mask);
    if (ibv_modify_qp(rdma_dev->qp, &qp_attr, attr_mask)) {
        fprintf(stderr, "Failed to modify QP to RTS\n");
        return 1;
    }
    DEBUG_LOG ("ibv_modify_qp to state %d completed: qp_num = 0x%lx\n", qp_attr.qp_state, rdma_dev->qp->qp_num);
    
    return 0;
}

static int destroy_qp(struct ibv_qp *qp) 
{
	int ret;
	if (qp) {
		DEBUG_LOG("ibv_destroy_qp(%p)\n", qp);
		ret = ibv_destroy_qp(qp);
		if (ret) {
			fprintf(stderr, "Couldn't destroy QP: error %d\n", ret);  
		}
	}
	return ret;
}

static int modify_source_qp_rst2rts(struct rdma_device *rdma_dev) 
{
    int ret_val;
    /* - - - - - - - - - -  Modify QP to INIT  - - - - - - - - - - - - - */
    struct ibv_qp_attr qp_attr = {
        .qp_state        = IBV_QPS_INIT,
        .pkey_index      = 0,
        .port_num        = rdma_dev->ib_port,
        .qp_access_flags = IBV_ACCESS_LOCAL_WRITE
    };
    enum ibv_qp_attr_mask attr_mask = IBV_QP_STATE      |
                                      IBV_QP_PKEY_INDEX |
                                      IBV_QP_PORT       |
                                      0 /*IBV_QP_ACCESS_FLAGS*/; /*we must zero this bit for DCI QP*/
    DEBUG_LOG("ibv_modify_qp(qp = %p, qp_attr.qp_state = %d, attr_mask = 0x%x)\n",
               rdma_dev->qp, qp_attr.qp_state, attr_mask);
    ret_val = ibv_modify_qp(rdma_dev->qp, &qp_attr, attr_mask);
    if (ret_val) {
        fprintf(stderr, "Failed to modify QP to INIT, error %d\n", ret_val);
        return 1;
    }
    DEBUG_LOG("ibv_modify_qp to state %d completed: qp_num = 0x%lx\n", qp_attr.qp_state, rdma_dev->qp->qp_num);
    
    /* - - - - - - - - - - - - -  Modify QP to RTS  - - - - - - - - - - - - */
    ret_val = modify_source_qp_to_rtr_and_rts(rdma_dev);
    if (ret_val) {
        return 1;
    }

    rdma_dev->qpex->wr_flags = IBV_SEND_SIGNALED;

    return 0;
}

//============================================================================================
struct rdma_device *rdma_open_device_client(struct sockaddr *addr)
{
    struct rdma_device *rdma_dev;
    int                 ret_val;

    rdma_dev = calloc(1, sizeof *rdma_dev);
    if (!rdma_dev) {
        fprintf(stderr, "rdma_device memory allocation failed\n");
        return NULL;
    }

    /****************************************************************************************************
     * In the next function we let rdma_cm find a IB device that matches the IP address of a the local netdev,
     * if yes, we return a pointer to that ib context
     * The result of this function is ib_dev - initialized pointer to the relevant struct ibv_device
     ****************************************************************************************************/
    rdma_dev->context = open_ib_device_by_addr(rdma_dev, addr);
    if (!rdma_dev->context){
        goto clean_rdma_dev;
    }

    ret_val = rdma_set_lid_gid_from_port_info(rdma_dev);
    if (ret_val) {
        goto clean_device;
    }

    /****************************************************************************************************/
    
    DEBUG_LOG ("ibv_alloc_pd(ibv_context = %p)\n", rdma_dev->context);
    rdma_dev->pd = ibv_alloc_pd(rdma_dev->context);
    if (!rdma_dev->pd) {
        fprintf(stderr, "Couldn't allocate PD\n");
        goto clean_device;
    }
    DEBUG_LOG("created pd %p\n", rdma_dev->pd);

    /* **********************************  Create CQ  ********************************** */
#ifdef PRINT_LATENCY
	struct ibv_cq_init_attr_ex cq_attr_ex;
	
    memset(&cq_attr_ex, 0, sizeof(cq_attr_ex));
	cq_attr_ex.cqe = CQ_DEPTH;
	cq_attr_ex.cq_context = rdma_dev;
	cq_attr_ex.channel = NULL;
	cq_attr_ex.comp_vector = 0;
	cq_attr_ex.wc_flags = IBV_WC_EX_WITH_COMPLETION_TIMESTAMP;

    DEBUG_LOG ("ibv_create_cq_ex(rdma_dev->context = %p, &cq_attr_ex)\n", rdma_dev->context);
	rdma_dev->cq = ibv_create_cq_ex(rdma_dev->context, &cq_attr_ex);
#else /*PRINT_LATENCY*/
    DEBUG_LOG ("ibv_create_cq(%p, %d, NULL, NULL, 0)\n", rdma_dev->context, CQ_DEPTH);
    rdma_dev->cq = ibv_create_cq(rdma_dev->context, CQ_DEPTH, NULL, NULL /*comp. events channel*/, 0);
#endif /*PRINT_LATENCY*/
    if (!rdma_dev->cq) {
        fprintf(stderr, "Couldn't create CQ\n");
        goto clean_pd;
    }
    DEBUG_LOG("created cq %p\n", rdma_dev->cq);

    /* **********************************  Create SRQ  ********************************** */
    struct ibv_srq_init_attr srq_attr;
    memset(&srq_attr, 0, sizeof(srq_attr));
    srq_attr.attr.max_wr = 2;
    srq_attr.attr.max_sge = 1;
    DEBUG_LOG ("ibv_create_srq(%p, %d, NULL, NULL, 0)\n", rdma_dev->context, CQ_DEPTH);
    rdma_dev->srq = ibv_create_srq(rdma_dev->pd, &srq_attr);
    if (!rdma_dev->srq) {
        fprintf(stderr, "ibv_create_srq failed\n");
        goto clean_cq;
    }
    DEBUG_LOG("created srq %p\n", rdma_dev->srq);

    /* **********************************  Create QP  ********************************** */
    struct ibv_qp_init_attr_ex attr_ex;
    struct mlx5dv_qp_init_attr attr_dv;

    memset(&attr_ex, 0, sizeof(attr_ex));
    memset(&attr_dv, 0, sizeof(attr_dv));

    attr_ex.qp_type = IBV_QPT_DRIVER;
#ifdef PRINT_LATENCY
    attr_ex.send_cq = ibv_cq_ex_to_cq(rdma_dev->cq);
    attr_ex.recv_cq = ibv_cq_ex_to_cq(rdma_dev->cq);
#else /*PRINT_LATENCY*/
    attr_ex.send_cq = rdma_dev->cq;
    attr_ex.recv_cq = rdma_dev->cq;
#endif /*PRINT_LATENCY*/

    attr_ex.comp_mask |= IBV_QP_INIT_ATTR_PD;
    attr_ex.pd = rdma_dev->pd;
    attr_ex.srq = rdma_dev->srq; /* Should use SRQ for client only (DCT) */

    /* create DCT */
    attr_dv.comp_mask |= MLX5DV_QP_INIT_ATTR_MASK_DC;
    attr_dv.dc_init_attr.dc_type = MLX5DV_DCTYPE_DCT;
    attr_dv.dc_init_attr.dct_access_key = DC_KEY;

    DEBUG_LOG ("mlx5dv_create_qp(%p)\n", rdma_dev->context);
    rdma_dev->qp = mlx5dv_create_qp(rdma_dev->context, &attr_ex, &attr_dv);

    if (!rdma_dev->qp)  {
        fprintf(stderr, "Couldn't create QP\n");
        goto clean_srq;
    }
    DEBUG_LOG ("mlx5dv_create_qp %p completed: qp_num = 0x%lx\n", rdma_dev->qp, rdma_dev->qp->qp_num);

    /* - - - - - - -  Modify QP to INIT  - - - - - - - */
    struct ibv_qp_attr qp_attr = {
        .qp_state        = IBV_QPS_INIT,
        .pkey_index      = 0,
        .port_num        = rdma_dev->ib_port,
        .qp_access_flags = IBV_ACCESS_REMOTE_READ | IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE
    };
    enum ibv_qp_attr_mask attr_mask = IBV_QP_STATE      |
                                      IBV_QP_PKEY_INDEX |
                                      IBV_QP_PORT       |
                                      IBV_QP_ACCESS_FLAGS;
    DEBUG_LOG ("ibv_modify_qp(qp = %p, qp_attr.qp_state = %d, attr_mask = 0x%x)\n",
               rdma_dev->qp, qp_attr.qp_state, attr_mask);
    ret_val = ibv_modify_qp(rdma_dev->qp, &qp_attr, attr_mask);
    if (ret_val) {
        fprintf(stderr, "Failed to modify QP to INIT, error %d\n", ret_val);
        goto clean_qp;
    }
    DEBUG_LOG ("ibv_modify_qp to state %d completed: qp_num = 0x%lx\n", qp_attr.qp_state, rdma_dev->qp->qp_num);

    ret_val = modify_target_qp_to_rtr(rdma_dev);
    if (ret_val) {
        goto clean_qp;
    }
    
    DEBUG_LOG("init AH cache\n");
    kh_init_inplace(kh_ib_ah, &rdma_dev->ah_hash);

#ifdef PRINT_LATENCY
    struct ibv_device_attr_ex           device_attr_ex = {};
    //struct ibv_query_device_ex_input    query_device_ex_input = {
    //    .comp_masc = ...
    //}
    
    ret_val = ibv_query_device_ex(rdma_dev->context, /*struct ibv_query_device_ex_input*/NULL, &device_attr_ex);
    if (ret_val) {
        fprintf(stderr, "ibv_query_device_ex failed\n");
        goto clean_qp;
    }
    if (!device_attr_ex.hca_core_clock) {
        fprintf(stderr, "hca_core_clock = 0\n");
        goto clean_qp;
    }

    rdma_dev->hca_core_clock_kHz = device_attr_ex.hca_core_clock;
    DEBUG_LOG("hca_core_clock = %d kHz\n", rdma_dev->hca_core_clock_kHz);
#endif /*PRINT_LATENCY*/
    
    return rdma_dev;

clean_qp:
    destroy_qp(rdma_dev->qp);

clean_srq:
    if (rdma_dev->srq) {
        ibv_destroy_srq(rdma_dev->srq);
    }

clean_cq:
    if (rdma_dev->cq) {
#ifdef PRINT_LATENCY
        ibv_destroy_cq(ibv_cq_ex_to_cq(rdma_dev->cq));
#else /*PRINT_LATENCY*/
        ibv_destroy_cq(rdma_dev->cq);
#endif /*PRINT_LATENCY*/
    }

clean_pd:
    if (rdma_dev->pd) {
        ibv_dealloc_pd(rdma_dev->pd);
    }

clean_device:
    close_ib_device(rdma_dev);
    
clean_rdma_dev:
    free(rdma_dev);

    return NULL;
}

//============================================================================================
struct rdma_device *rdma_open_device_server(struct sockaddr *addr)
{
    struct rdma_device *rdma_dev;
    int                 ret_val;

    rdma_dev = calloc(1, sizeof *rdma_dev);
    if (!rdma_dev) {
        fprintf(stderr, "rdma_device memory allocation failed\n");
        return NULL;
    }

    /****************************************************************************************************
     * In the next function we let rdma_cm find a IB device that matches the IP address of a the local netdev,
     * if yes, we return a pointer to that ib context
     * The result of this function is ib_dev - initialized pointer to the relevant struct ibv_device
     ****************************************************************************************************/
    rdma_dev->context = open_ib_device_by_addr(rdma_dev, addr);
    if (!rdma_dev->context){
        goto clean_rdma_dev;
    }
    
    ret_val = rdma_set_lid_gid_from_port_info(rdma_dev);
    if (ret_val) {
        goto clean_device;
    }

    /****************************************************************************************************/

    DEBUG_LOG ("ibv_alloc_pd(ibv_context = %p)\n", rdma_dev->context);
    rdma_dev->pd = ibv_alloc_pd(rdma_dev->context);
    if (!rdma_dev->pd) {
        fprintf(stderr, "Couldn't allocate PD\n");
        goto clean_device;
    }
    DEBUG_LOG("created pd %p\n", rdma_dev->pd);

    /* We don't create completion events channel (ibv_create_comp_channel), we prefer working in polling mode */
    
    /* **********************************  Create CQ  ********************************** */
#ifdef PRINT_LATENCY
	struct ibv_cq_init_attr_ex cq_attr_ex;
	
    memset(&cq_attr_ex, 0, sizeof(cq_attr_ex));
	cq_attr_ex.cqe = CQ_DEPTH;
	cq_attr_ex.cq_context = rdma_dev;
	cq_attr_ex.channel = NULL;
	cq_attr_ex.comp_vector = 0;
	cq_attr_ex.wc_flags = IBV_WC_EX_WITH_COMPLETION_TIMESTAMP;

    DEBUG_LOG ("ibv_create_cq_ex(rdma_dev->context = %p, &cq_attr_ex)\n", rdma_dev->context);
	rdma_dev->cq = ibv_create_cq_ex(rdma_dev->context, &cq_attr_ex);
#else /*PRINT_LATENCY*/
    DEBUG_LOG ("ibv_create_cq(%p, %d, NULL, NULL, 0)\n", rdma_dev->context, CQ_DEPTH);
    rdma_dev->cq = ibv_create_cq(rdma_dev->context, CQ_DEPTH, NULL, NULL /*comp. events channel*/, 0);
#endif /*PRINT_LATENCY*/
    if (!rdma_dev->cq) {
        fprintf(stderr, "Couldn't create CQ\n");
        goto clean_pd;
    }
    DEBUG_LOG("created cq %p\n", rdma_dev->cq);

    /* We don't create SRQ for DCI (server) side */

    /* **********************************  Create QP  ********************************** */
    struct ibv_qp_init_attr_ex attr_ex;
    struct mlx5dv_qp_init_attr attr_dv;

    memset(&attr_ex, 0, sizeof(attr_ex));
    memset(&attr_dv, 0, sizeof(attr_dv));

    attr_ex.qp_type = IBV_QPT_DRIVER;
#ifdef PRINT_LATENCY
    attr_ex.send_cq = ibv_cq_ex_to_cq(rdma_dev->cq);
    attr_ex.recv_cq = ibv_cq_ex_to_cq(rdma_dev->cq);
#else /*PRINT_LATENCY*/
    attr_ex.send_cq = rdma_dev->cq;
    attr_ex.recv_cq = rdma_dev->cq;
#endif /*PRINT_LATENCY*/

    attr_ex.comp_mask |= IBV_QP_INIT_ATTR_PD;
    attr_ex.pd = rdma_dev->pd;

    /* create DCI */
    attr_dv.comp_mask |= MLX5DV_QP_INIT_ATTR_MASK_DC;
    attr_dv.dc_init_attr.dc_type = MLX5DV_DCTYPE_DCI;
    
    attr_ex.cap.max_send_wr  = SEND_Q_DEPTH;
    attr_ex.cap.max_send_sge = MAX_SEND_SGE;
    rdma_dev->qp_available_wr = SEND_Q_DEPTH;

    attr_ex.comp_mask |= IBV_QP_INIT_ATTR_SEND_OPS_FLAGS;
    attr_ex.send_ops_flags = IBV_QP_EX_WITH_RDMA_WRITE | IBV_QP_EX_WITH_RDMA_READ;

    attr_dv.comp_mask |= MLX5DV_QP_INIT_ATTR_MASK_QP_CREATE_FLAGS;
    attr_dv.create_flags |= MLX5DV_QP_CREATE_DISABLE_SCATTER_TO_CQE; /*driver doesnt support scatter2cqe data-path on DCI yet*/
    
    DEBUG_LOG ("mlx5dv_create_qp(%p)\n", rdma_dev->context);
    rdma_dev->qp = mlx5dv_create_qp(rdma_dev->context, &attr_ex, &attr_dv);
    DEBUG_LOG ("mlx5dv_create_qp %p completed: qp_num = 0x%lx\n", rdma_dev->qp, rdma_dev->qp->qp_num);

    if (!rdma_dev->qp)  {
        fprintf(stderr, "Couldn't create QP\n");
        goto clean_cq;
    }
    rdma_dev->qpex = ibv_qp_to_qp_ex(rdma_dev->qp);
    if (!rdma_dev->qpex)  {
        fprintf(stderr, "Couldn't create QPEX\n");
        goto clean_qp;
    }
    rdma_dev->mqpex = mlx5dv_qp_ex_from_ibv_qp_ex(rdma_dev->qpex);
    if (!rdma_dev->mqpex)  {
        fprintf(stderr, "Couldn't create MQPEX\n");
        goto clean_qp;
    }
    ret_val = modify_source_qp_rst2rts(rdma_dev);
    if (ret_val) {
        goto clean_qp;
    }

    DEBUG_LOG("init AH cache\n");
    kh_init_inplace(kh_ib_ah, &rdma_dev->ah_hash);
    
#ifdef PRINT_LATENCY
    struct ibv_device_attr_ex           device_attr_ex = {};
    //struct ibv_query_device_ex_input    query_device_ex_input = {
    //    .comp_masc = ...
    //}
    
    ret_val = ibv_query_device_ex(rdma_dev->context, /*struct ibv_query_device_ex_input*/NULL, &device_attr_ex);
    if (ret_val) {
        fprintf(stderr, "ibv_query_device_ex failed\n");
        goto clean_qp;
    }
    if (!device_attr_ex.hca_core_clock) {
        fprintf(stderr, "hca_core_clock = 0\n");
        goto clean_qp;
    }

    rdma_dev->hca_core_clock_kHz = device_attr_ex.hca_core_clock;
    DEBUG_LOG("hca_core_clock = %d kHz\n", rdma_dev->hca_core_clock_kHz);

    rdma_dev->min_wr_complete_latency = 0x8FFFFFFFFFFFFFFF;
    rdma_dev->min_completion_latency = 0x8FFFFFFFFFFFFFFF;
    rdma_dev->min_read_comp_latency = 0x8FFFFFFFFFFFFFFF;
#endif /*PRINT_LATENCY*/
    
    return rdma_dev;

clean_qp:
    destroy_qp(rdma_dev->qp);

clean_cq:
    if (rdma_dev->cq) {
#ifdef PRINT_LATENCY
        ibv_destroy_cq(ibv_cq_ex_to_cq(rdma_dev->cq));
#else /*PRINT_LATENCY*/
        ibv_destroy_cq(rdma_dev->cq);
#endif /*PRINT_LATENCY*/
    }

clean_pd:
    if (rdma_dev->pd) {
        ibv_dealloc_pd(rdma_dev->pd);
    }

clean_device:
    close_ib_device(rdma_dev);

clean_rdma_dev:
    free(rdma_dev);
    
    return NULL;
}

//===========================================================================================
static
int rdma_exec_task(struct rdma_exec_params *exec_params) 
{
	int ret_val;
	int required_wr = (exec_params->local_buf_iovcnt) ? (exec_params->local_buf_iovcnt + MAX_SEND_SGE - 1) / MAX_SEND_SGE : 1;
	if (required_wr > exec_params->device->qp_available_wr) {
		fprintf(stderr, "Required WR number %d is greater than available in QP WRs %d\n", 
				required_wr, exec_params->device->qp_available_wr);
		return 1;
	}
	void (*ibv_wr_rdma_rw_post)(struct ibv_qp_ex *qp, uint32_t rkey, uint64_t remote_addr) = (exec_params->flags & RDMA_TASK_ATTR_RDMA_READ) 
		? ibv_wr_rdma_read // client wants to send data to the server
		: ibv_wr_rdma_write; // client wants to receive data from the server

	/* RDMA Read/Write for DCI connect, this will create cqe->ts_start */
	DEBUG_LOG_FAST_PATH("RDMA Read/Write: ibv_wr_start: qpex = %p\n", exec_params->device->qpex);
	ibv_wr_start(exec_params->device->qpex);
#ifdef PRINT_LATENCY
	struct ibv_values_ex ts_values = {
		.comp_mask = IBV_VALUES_MASK_RAW_CLOCK,
		.raw_clock = {} /*struct timespec*/
	};

	ret_val = ibv_query_rt_values_ex(exec_params->device->context, &ts_values);
	if (ret_val) {
		fprintf(stderr, "ibv_query_rt_values_ex failed after ibv_wr_start call\n");
		return 1;
	}
#endif /*PRINT_LATENCY*/

	// The following code should be atomic operation
	int wr_id_idx = exec_params->device->app_wr_id_idx++;
	if (exec_params->device->app_wr_id_idx >= SEND_Q_DEPTH) {
		exec_params->device->app_wr_id_idx = 0;
	}
	// end of atomic operation

#ifdef PRINT_LATENCY
	exec_params->device->latency[wr_id_idx].wr_start_ts = ts_values.raw_clock.tv_nsec; /*the value in hca clocks*/
#endif /*PRINT_LATENCY*/

	// update internal wr_id DB
	exec_params->device->qp_available_wr -= required_wr;
	exec_params->device->app_wr_id[wr_id_idx].num_wrs = required_wr;
	exec_params->device->app_wr_id[wr_id_idx].wr_id = exec_params->wr_id;
	exec_params->device->app_wr_id[wr_id_idx].flags = WR_ID_FLAGS_ACTIVE;

	exec_params->device->qpex->wr_id = (uint64_t)wr_id_idx;

	if (exec_params->local_buf_iovcnt) {
		int i, start_i = 0;
		struct ibv_sge sg_list[MAX_SEND_SGE];
	       	uint64_t curr_rem_addr = (uint64_t)exec_params->rem_buf_addr;
		int num_sges_to_send = exec_params->local_buf_iovcnt;

		while (num_sges_to_send > 0) {
			int curr_iovcnt = mmin(MAX_SEND_SGE, num_sges_to_send);
			exec_params->device->qpex->wr_flags = num_sges_to_send > MAX_SEND_SGE ? 0 : IBV_SEND_SIGNALED;

			DEBUG_LOG_FAST_PATH("RDMA Read/Write: ibv_wr_rdma_%s: wr_id=0x%llx, qpex=%p, rkey=0x%lx, remote_buf=0x%llx\n",
					exec_params->flags & RDMA_TASK_ATTR_RDMA_READ ? "read" : "write",
					(long long unsigned int)exec_params->wr_id, exec_params->device->qpex, exec_params->rem_buf_rkey, (long long unsigned int)curr_rem_addr);
			ibv_wr_rdma_rw_post(exec_params->device->qpex, exec_params->rem_buf_rkey, curr_rem_addr);
		
			for (i = 0; i < curr_iovcnt; i++) {
				sg_list[i].addr   = (uint64_t)exec_params->local_buf_iovec[start_i + i].iov_base;
				sg_list[i].length = (uint32_t)exec_params->local_buf_iovec[start_i + i].iov_len;
				sg_list[i].lkey   = exec_params->local_buf_mr_lkey;
				curr_rem_addr += sg_list[i].length;
			}
		
			DEBUG_LOG_FAST_PATH("RDMA Read/Write: ibv_wr_set_sge_list(qpex=%p, num_sge=%lu, sg_list=%p), start_i=%d, num_sges_to_send=%d, sg[0].length=%u\n",
				exec_params->device->qpex, (size_t)curr_iovcnt, (void*)sg_list, start_i, num_sges_to_send, sg_list[0].length);
			ibv_wr_set_sge_list(exec_params->device->qpex, (size_t)curr_iovcnt, sg_list);
			num_sges_to_send -= curr_iovcnt;
			start_i += curr_iovcnt;


			DEBUG_LOG_FAST_PATH("RDMA Read/Write: mlx5dv_wr_set_dc_addr: mqpex=%p, ah=%p, rem_dctn=0x%06lx\n",
				exec_params->device->mqpex, exec_params->ah, exec_params->rem_dctn);
			mlx5dv_wr_set_dc_addr(exec_params->device->mqpex, exec_params->ah, exec_params->rem_dctn, DC_KEY);
		}
	} else {
		exec_params->device->qpex->wr_flags = IBV_SEND_SIGNALED;

		DEBUG_LOG_FAST_PATH("RDMA Read/Write: ibv_wr_rdma_%s: wr_id=0x%llx, qpex=%p, rkey=0x%lx, remote_buf=0x%llx\n",
				exec_params->flags & RDMA_TASK_ATTR_RDMA_READ ? "read" : "write",
				(long long unsigned int)exec_params->wr_id, exec_params->device->qpex, exec_params->rem_buf_rkey, (unsigned long long)exec_params->rem_buf_addr);

		ibv_wr_rdma_rw_post(exec_params->device->qpex, exec_params->rem_buf_rkey, exec_params->rem_buf_addr);
		
		DEBUG_LOG_FAST_PATH("RDMA Read/Write: ibv_wr_set_sge: qpex=%p, lkey=0x%x, local_buf=0x%llx, size=%u\n",
				exec_params->device->qpex, exec_params->local_buf_mr_lkey,
				(unsigned long long)exec_params->local_buf_addr, exec_params->rem_buf_size);
		ibv_wr_set_sge(exec_params->device->qpex, exec_params->local_buf_mr_lkey, (uintptr_t)exec_params->local_buf_addr, exec_params->rem_buf_size);

		DEBUG_LOG_FAST_PATH("RDMA Read/Write: mlx5dv_wr_set_dc_addr: mqpex=%p, ah=%p, rem_dctn=0x%06lx\n",
				exec_params->device->mqpex, exec_params->ah, exec_params->rem_dctn);
		mlx5dv_wr_set_dc_addr(exec_params->device->mqpex, exec_params->ah, exec_params->rem_dctn, DC_KEY);
	}

	/* ring DB */
	DEBUG_LOG_FAST_PATH("ibv_wr_complete: qpex=%p, required_wr=%d\n", exec_params->device->qpex, required_wr);
	ret_val = ibv_wr_complete(exec_params->device->qpex);
	if (ret_val) {
		DEBUG_LOG_FAST_PATH("FAILURE: ibv_wr_complete (error=%d\n", ret_val);
		return ret_val;
	}
 #ifdef PRINT_LATENCY
	ret_val = ibv_query_rt_values_ex(exec_params->device->context, &ts_values);
	if (ret_val) {
		fprintf(stderr, "ibv_query_rt_values_ex failed after ibv_wr_start call\n");
		return 1;
	}
	exec_params->device->latency[wr_id_idx].wr_complete_ts = ts_values.raw_clock.tv_nsec; /*the value in hca clocks*/
#endif /*PRINT_LATENCY*/
	return ret_val;
}
//===========================================================================================

int rdma_reset_device(struct rdma_device *device)
{
	if (!is_server(device)) {
		fprintf(stderr, "Method \"rdma_reset_device()\" could be executed only by server side!\n");
		return EOPNOTSUPP;
	}
	struct ibv_qp_attr      qp_attr;
	enum ibv_qp_attr_mask   attr_mask;
	memset(&qp_attr, 0, sizeof qp_attr);
	
	/* - - - - - - - Modify QP to ERR - - - - - - - */
	qp_attr.qp_state = IBV_QPS_ERR;
	attr_mask = IBV_QP_STATE;
	DEBUG_LOG("ibv_modify_qp(qp = %p, qp_attr.qp_state = %d, attr_mask = 0x%x)\n",
                      device->qp, qp_attr.qp_state, attr_mask);
	if (ibv_modify_qp(device->qp, &qp_attr, attr_mask)) {
		fprintf(stderr, "Failed to modify QP to ERR\n");
		return 1;
	}
	
	/* - - - - - - - FLUSH WORK COMPLETIONS - - - - - - - */
	struct rdma_exec_params exec_params;
	memset(&exec_params, 0, sizeof exec_params);
	khiter_t ah_itr = 0;
	for (ah_itr = kh_begin(&device->ah_hash); ah_itr != kh_end(&device->ah_hash); ++ah_itr) {
		if (kh_exist(&device->ah_hash, ah_itr) && kh_value(&device->ah_hash, ah_itr) != NULL) {
			exec_params.ah = kh_value(&device->ah_hash, ah_itr);
		}
	}
	if (exec_params.ah) {
		exec_params.wr_id = WR_ID_FLUSH_MARKER;
		exec_params.device = device;

		DEBUG_LOG_FAST_PATH("Posting FLUSH MARKER on queue\n");
		rdma_exec_task(&exec_params);

		DEBUG_LOG_FAST_PATH("Flushing Work Completions\n");
		struct rdma_completion_event rdma_comp_ev[COMP_ARRAY_SIZE];
		int flushed = 0;
		do {
			int i, reported_ev = 0;
			reported_ev = rdma_poll_completions(device, &rdma_comp_ev[reported_ev], COMP_ARRAY_SIZE);
			for (i = 0; !flushed && i < reported_ev; i++) {
				flushed = rdma_comp_ev[i].wr_id == WR_ID_FLUSH_MARKER;
			}
		} while (!flushed);
		DEBUG_LOG_FAST_PATH("Finished Work Completions flushing\n");
	}

	/* - - - - - - - RESET RDMA_DEVICE MEMBERS - - - - - - - */
	memset(device->app_wr_id, 0, sizeof(device->app_wr_id));
	device->app_wr_id_idx = 0;
	device->qp_available_wr = SEND_Q_DEPTH;
	/* - - - - - - - Modify QP to RESET - - - - - - - */
	qp_attr.qp_state = IBV_QPS_RESET;
	attr_mask = IBV_QP_STATE;
	DEBUG_LOG("ibv_modify_qp(qp = %p, qp_attr.qp_state = %d, attr_mask = 0x%x)\n",
                    device->qp, qp_attr.qp_state, attr_mask);
	if (ibv_modify_qp(device->qp, &qp_attr, attr_mask)) {
		fprintf(stderr, "Failed to modify QP to RESET\n");
		return 1;
	}
	DEBUG_LOG ("ibv_modify_qp to state %d completed.\n", qp_attr.qp_state, device->qp->qp_num);

	/* - - - - - - - Modify QP to RTS (RESET->INIT->RTR->RTS) - - - - - - - */
	return modify_source_qp_rst2rts(device);
}

//============================================================================================
void rdma_close_device(struct rdma_device *rdma_dev)
{
    int ret_val;
    struct ibv_ah *ah;

    if (rdma_dev->rdma_buff_cnt > 0) {
        fprintf(stderr, "The number of attached RDMA buffers is not zero (%d). Can't close device.\n",
                rdma_dev->rdma_buff_cnt);
        return;
    }
#ifdef PRINT_LATENCY
    if (rdma_dev->measure_index) {
        DEBUG_LOG("PRINT_LATENCY: %6lu wr-s, wr_sent latency: min %8lu, max %8lu, avg %8lu (nSec)\n",
                  rdma_dev->measure_index,
                  rdma_dev->min_wr_complete_latency * 1000000 / rdma_dev->hca_core_clock_kHz,
                  rdma_dev->max_wr_complete_latency * 1000000 / rdma_dev->hca_core_clock_kHz,
                  rdma_dev->wr_complete_latency_sum / rdma_dev->measure_index * 1000000 / rdma_dev->hca_core_clock_kHz);

        DEBUG_LOG("PRINT_LATENCY:   completion latency        : min %8lu, max %8lu, avg %8lu (nSec)\n",
                  rdma_dev->min_completion_latency * 1000000 / rdma_dev->hca_core_clock_kHz,
                  rdma_dev->max_completion_latency * 1000000 / rdma_dev->hca_core_clock_kHz,
                  rdma_dev->completion_latency_sum / rdma_dev->measure_index * 1000000 / rdma_dev->hca_core_clock_kHz);

        DEBUG_LOG("PRINT_LATENCY:   read_comp latency         : min %8lu, max %8lu, avg %8lu (nSec)\n",
                  rdma_dev->min_read_comp_latency * 1000000 / rdma_dev->hca_core_clock_kHz,
                  rdma_dev->max_read_comp_latency * 1000000 / rdma_dev->hca_core_clock_kHz,
                  rdma_dev->read_comp_latency_sum / rdma_dev->measure_index * 1000000 / rdma_dev->hca_core_clock_kHz);

        fflush(stdout);
    }
#endif /*PRINT_LATENCY*/
    ret_val = destroy_qp(rdma_dev->qp);
    if (ret_val) {
        return;
    }

    if (rdma_dev->srq) {
        DEBUG_LOG("ibv_destroy_srq(%p)\n", rdma_dev->srq);
        ret_val = ibv_destroy_srq(rdma_dev->srq);
        if (ret_val) {
            fprintf(stderr, "Couldn't destroy SRQ\n");
            return;
        }
    }
    
    DEBUG_LOG("ibv_destroy_cq(%p)\n", rdma_dev->cq);
#ifdef PRINT_LATENCY
    ibv_destroy_cq(ibv_cq_ex_to_cq(rdma_dev->cq));
#else /*PRINT_LATENCY*/
    ibv_destroy_cq(rdma_dev->cq);
#endif /*PRINT_LATENCY*/
    if (ret_val) {
        fprintf(stderr, "Couldn't destroy CQ, error %d\n", ret_val);
        return;
    }

    DEBUG_LOG("destroy ibv_ah's\n");
    kh_foreach_value(&rdma_dev->ah_hash, ah, ibv_destroy_ah(ah));

    DEBUG_LOG("ibv_dealloc_pd(%p)\n", rdma_dev->pd);
    ret_val = ibv_dealloc_pd(rdma_dev->pd);
    if (ret_val) {
        fprintf(stderr, "Couldn't deallocate PD, error %d\n", ret_val);
        return;
    }

    DEBUG_LOG("destroy AH cache\n");
    kh_destroy_inplace(kh_ib_ah, &rdma_dev->ah_hash);

    close_ib_device(rdma_dev);

    free(rdma_dev);

    return;
}

//============================================================================================
struct rdma_buffer *rdma_buffer_reg(struct rdma_device *rdma_dev, void *addr, size_t length)
{
    struct rdma_buffer *rdma_buff;
    int    ret_val;

    rdma_buff = calloc(1, sizeof *rdma_buff);
    if (!rdma_buff) {
        fprintf(stderr, "rdma_buff memory allocation failed\n");
        return NULL;
    }

    enum ibv_access_flags access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
    /*In the case of local buffer we can use IBV_ACCESS_LOCAL_WRITE only flag*/
    DEBUG_LOG("ibv_reg_mr(pd %p, buf %p, size = %lu, access_flags = 0x%08x\n",
               rdma_dev->pd, addr, length, access_flags);
    rdma_buff->mr = ibv_reg_mr(rdma_dev->pd, addr, length, access_flags);
    if (!rdma_buff->mr) {
        fprintf(stderr, "Couldn't register GPU MR\n");
        goto clean_rdma_buff;
    }
    DEBUG_LOG("ibv_reg_mr completed: buf %p, size = %lu, rkey = 0x%08x\n",
               addr, length, rdma_buff->mr->rkey);

    rdma_buff->buf_addr = addr;
    rdma_buff->buf_size = length;
    rdma_buff->rkey     = rdma_buff->mr->rkey; /*not used for local buffer case*/
    rdma_buff->rdma_dev = rdma_dev;
    rdma_dev->rdma_buff_cnt++;

    return rdma_buff;

clean_rdma_buff:
    /* We don't decrement device rdma_buff_cnt because we still did not increment it,
    we just free the allocated for rdma_buff memory. */
    free(rdma_buff);
    
    return NULL;
}

//============================================================================================
void rdma_buffer_dereg(struct rdma_buffer *rdma_buff)
{
    int ret_val;

    DEBUG_LOG("ibv_dereg_mr(%p)\n", rdma_buff->mr);
    if (rdma_buff->mr) {
        ret_val = ibv_dereg_mr(rdma_buff->mr);
        if (ret_val) {
            fprintf(stderr, "Couldn't deregister MR, error %d\n", ret_val);
            return;
        }
    }
    rdma_buff->rdma_dev->rdma_buff_cnt--;
    DEBUG_LOG("The buffer detached from rdma_device (%p). Number of attached to device buffers is %d.\n",
              rdma_buff->rdma_dev, rdma_buff->rdma_dev->rdma_buff_cnt);

    free(rdma_buff);
}

//============================================================================================
static void wire_gid_to_gid(const char *wgid, union ibv_gid *gid)
{
    char tmp[9];
    uint32_t v32;
    uint32_t *raw = (uint32_t *)gid->raw;
    int i;

    for (tmp[8] = 0, i = 0; i < 4; ++i) {
        memcpy(tmp, wgid + i * 8, 8);
        sscanf(tmp, "%x", &v32);
        raw[i] = ntohl(v32);
    }
}

static void gid_to_wire_gid(const union ibv_gid *gid, char wgid[])
{
    int i;
    uint32_t *raw = (uint32_t *)gid->raw;

    for (i = 0; i < 4; ++i)
        sprintf(&wgid[i * 8], "%08x", htonl(raw[i]));
}

//===============================================================================================
/*                                       addr             size     rkey     lid  dctn   g gid   */
#define BUFF_DESC_STRING_LENGTH (sizeof "0102030405060708:01020304:01020304:0102:010203:1:0102030405060708090a0b0c0d0e0f10")

int rdma_buffer_get_desc_str(struct rdma_buffer *rdma_buff, char *desc_str, size_t desc_length)
{
    if (desc_length < BUFF_DESC_STRING_LENGTH) {
        fprintf(stderr, "desc string size (%lu) is less than required (%lu) for sending rdma_buffer attributes\n",
                desc_length, BUFF_DESC_STRING_LENGTH);
        return 0;
    }
    /*       addr             size     rkey     lid  dctn   g 
            "0102030405060708:01020304:01020304:0102:010203:1:" */
    sprintf(desc_str, "%016llx:%08lx:%08x:%04x:%06x:%d:",
            (unsigned long long)rdma_buff->buf_addr,
            (unsigned long)rdma_buff->buf_size,
            rdma_buff->rkey,
            rdma_buff->rdma_dev->lid,
            rdma_buff->rdma_dev->qp->qp_num /* dctn */,
            rdma_buff->rdma_dev->is_global & 0x1);
    
    gid_to_wire_gid(&rdma_buff->rdma_dev->gid, desc_str + sizeof "0102030405060708:01020304:01020304:0102:010203:1");
    
    return strlen(desc_str) + 1; /*including the terminating null character*/
}

static int rdma_create_ah_cached(struct rdma_device *rdma_dev,
                 struct ibv_ah_attr *ah_attr,
                 struct ibv_ah **p_ah)
{
    int ret = -1;
    khiter_t iter;

    /* looking for existing AH with same attributes */
    iter = kh_get(kh_ib_ah, &rdma_dev->ah_hash, *ah_attr);
    if (iter == kh_end(&rdma_dev->ah_hash)) {

        /* new AH */
        DEBUG_LOG("ibv_create_ah(dlid=%d port=%d is_global=%d, tc=%d)\n",
            ah_attr->dlid, ah_attr->port_num, ah_attr->is_global, (ah_attr->grh.traffic_class >> 5));
        *p_ah = ibv_create_ah(rdma_dev->pd, ah_attr);

        if (*p_ah == NULL) {
            perror("ibv_create_ah");
            goto out;
        }

        /* store AH in hash */
        iter = kh_put(kh_ib_ah, &rdma_dev->ah_hash, *ah_attr, &ret);

        /* failed to store - rollback */
        if (iter == kh_end(&rdma_dev->ah_hash)) {
            perror("rdma_create_ah_cached failed storing");
            ibv_destroy_ah(*p_ah);
            goto out;
        }

        kh_value(&rdma_dev->ah_hash, iter) = *p_ah;
        ret = 0;
    } else {
        /* found existing AH */
        *p_ah = kh_value(&rdma_dev->ah_hash, iter);
        ret = 0;
    }

out:
    return ret;
}

//============================================================================================
static int buff_size_validation(struct rdma_task_attr *attr, unsigned long rem_buf_size)
{
    size_t  total_len = 0;
    int     i;

    for (i = 0; i < attr->local_buf_iovcnt; i++) {
        if ((attr->local_buf_iovec[i].iov_base < attr->local_buf_rdma->buf_addr) ||
            (attr->local_buf_iovec[i].iov_base + attr->local_buf_iovec[i].iov_len >
             attr->local_buf_rdma->buf_addr + attr->local_buf_rdma->buf_size)) {

            fprintf(stderr, "sge buffer %d (%p, %p) exceeds the local buffer bounary (%p, %p)\n", i,
                    attr->local_buf_iovec[i].iov_base, attr->local_buf_iovec[i].iov_base + attr->local_buf_iovec[i].iov_len,
                    attr->local_buf_rdma->buf_addr, attr->local_buf_rdma->buf_addr + attr->local_buf_rdma->buf_size);
            return 1;
        }
        total_len += attr->local_buf_iovec[i].iov_len;
        if (total_len > rem_buf_size) {
            fprintf(stderr, "The sum of sge buffers lengths (%lu) exceeded the remote buffer size %lu on iteration %d\n",
                    total_len, rem_buf_size, i);
            return 1;
        }
    }
    if ((attr->local_buf_iovcnt) && (total_len != rem_buf_size)) {
        fprintf(stderr, "WARN: The sum of sge buffers lengths (%lu) differs from the remote buffer size %lu\n",
                total_len, rem_buf_size);
    }
    if ((!attr->local_buf_iovcnt) && (rem_buf_size > attr->local_buf_rdma->buf_size)) {
        fprintf(stderr, "WARN: When not using sge list, the requested buffer size %lu is greater than allocated local size %lu\n",
                rem_buf_size, attr->local_buf_rdma->buf_size);
    }
    return 0;
}

//============================================================================================
int rdma_submit_task(struct rdma_task_attr *attr)
{
	struct rdma_exec_params exec_params = {};
	uint16_t                rem_lid = 0;
	int                     is_global = 0;
    	union ibv_gid           rem_gid;
    	int                     ret_val;

	exec_params.wr_id = attr->wr_id;
	exec_params.device = attr->local_buf_rdma->rdma_dev;
	exec_params.flags = attr->flags;
	exec_params.local_buf_mr_lkey = (uint32_t)attr->local_buf_rdma->mr->lkey;
	exec_params.local_buf_addr = attr->local_buf_rdma->buf_addr;
	exec_params.local_buf_iovec = attr->local_buf_iovec;
	exec_params.local_buf_iovcnt = attr->local_buf_iovcnt;
	/*
	 * Parse desc string, extracting remote buffer address, size, rkey, lid, dctn, and if global is true, also gid
	 */
	DEBUG_LOG_FAST_PATH("Starting to parse desc string: \"%s\"\n", attr->remote_buf_desc_str);
	/*   addr             size     rkey     lid  dctn   g gid                                              
	 *  "0102030405060708:01020304:01020304:0102:010203:1:0102030405060708090a0b0c0d0e0f10"*/
	sscanf(attr->remote_buf_desc_str, "%llx:%lx:%lx:%hx:%lx:%d",
			&exec_params.rem_buf_addr, &exec_params.rem_buf_size,
		       	&exec_params.rem_buf_rkey, &rem_lid,
			&exec_params.rem_dctn, &is_global);
	memset(&rem_gid, 0, sizeof(rem_gid));
	if (is_global) {
		wire_gid_to_gid(attr->remote_buf_desc_str + sizeof "0102030405060708:01020304:01020304:0102:010203:1", &rem_gid);
	}
	DEBUG_LOG_FAST_PATH("rem_buf_addr=0x%llx, rem_buf_size=%u, rem_buf_offset=%u, rem_buf_rkey=0x%lx, rem_lid=0x%hx, rem_dctn=0x%lx, is_global=%d\n",
			exec_params.rem_buf_addr, exec_params.rem_buf_size, attr->remote_buf_offset, exec_params.rem_buf_rkey, rem_lid, exec_params.rem_dctn, is_global);
       	DEBUG_LOG_FAST_PATH("Rem GID: %02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x\n",
                        rem_gid.raw[0],  rem_gid.raw[1],  rem_gid.raw[2],  rem_gid.raw[3],
                        rem_gid.raw[4],  rem_gid.raw[5],  rem_gid.raw[6],  rem_gid.raw[7], 
                        rem_gid.raw[8],  rem_gid.raw[9],  rem_gid.raw[10], rem_gid.raw[11],
                        rem_gid.raw[12], rem_gid.raw[13], rem_gid.raw[14], rem_gid.raw[15] );
	DEBUG_LOG_FAST_PATH("rdma_task_attr_flags=%08x\n", exec_params.flags);

	/* upadte the remote buffer addr and size acording to the requested start offset */
	exec_params.rem_buf_addr += attr->remote_buf_offset;
	exec_params.rem_buf_size -= attr->remote_buf_offset;

	/*
	 * Pass attr->local_buf_iovec - local_buf_iovcnt elements and check that
	 * the sum of local_buf_iovec[i].iov_len doesn't exceed rem_buf_size
	 */
	if (debug_fast_path) {
		/* We do these validation code in debug mode only, because if something
		 * is wrong in the fast path, the HW will give completion error */
		ret_val = buff_size_validation(attr, exec_params.rem_buf_size);
		if (ret_val) {
			return ret_val;
		}
	}
    
    /* Check if address handler corresponding to the given key is present in the hash table,
       if yes - return it and if it is not, create ah and add it to the hash table */
    struct ibv_ah_attr  ah_attr;

    memset(&ah_attr, 0, sizeof ah_attr);
    ah_attr.is_global   = is_global;
    ah_attr.dlid        = rem_lid;
    ah_attr.port_num    = exec_params.device->ib_port;
    
    if (ah_attr.is_global) {
        ah_attr.grh.hop_limit = 1;
        ah_attr.grh.dgid = rem_gid;
        ah_attr.grh.sgid_index = exec_params.device->gidx;
        ah_attr.grh.traffic_class = TC_PRIO << 5; // <<3 for dscp2prio, <<2 for ECN bits
    }

    if (rdma_create_ah_cached(exec_params.device, &ah_attr, &exec_params.ah)) {
        return 1;
    }
    ret_val = rdma_exec_task(&exec_params);

    return ret_val;
}

//============================================================================================
int rdma_poll_completions(struct rdma_device            *rdma_dev,
                          struct rdma_completion_event  *event,
                          uint32_t                      num_entries)
{
    int    reported_entries = 0;

    if (num_entries > COMP_ARRAY_SIZE) {
        num_entries = COMP_ARRAY_SIZE; /* We don't returne more than 16 entries,
                        If user needs more, he can call rdma_poll_completions again */
    }

    /* Polling completion queue */
    //DEBUG_LOG_FAST_PATH("Polling completion queue: ibv_poll_cq\n");
#ifdef PRINT_LATENCY
    struct ibv_poll_cq_attr cq_attr = {};
    uint64_t comp_ts;
    int      ret_val;

    ret_val = ibv_start_poll(rdma_dev->cq, &cq_attr);
    if ((ret_val) && (ret_val != ENOENT)) {
        perror("ibv_start_poll");
        return reported_entries; /*0*/
    }
    
    while (ret_val != ENOENT) {
        uint64_t cq_wr_id = rdma_dev->cq->wr_id;
        DEBUG_LOG_FAST_PATH("virtual wr_id %llu, original wr_id 0x%llx, num_wrs=%d\n",
                            (long long unsigned int)cq_wr_id,
                            (long long unsigned int)rdma_dev->app_wr_id[cq_wr_id].wr_id,
                            rdma_dev->app_wr_id[cq_wr_id].num_wrs);
        if (rdma_dev->app_wr_id[cq_wr_id].flags & WR_ID_FLAGS_ACTIVE) {
		rdma_dev->app_wr_id[wc[i].wr_id].flags = 0;
		rdma_dev->qp_available_wr += rdma_dev->app_wr_id[cq_wr_id].num_wrs;
        	event[reported_entries].wr_id  = rdma_dev->app_wr_id[cq_wr_id].wr_id;
        	event[reported_entries].status = rdma_dev->cq->status;
        	reported_entries++;
	}
        
        rdma_dev->latency[cq_wr_id].completion_ts = ibv_wc_read_completion_ts(rdma_dev->cq);
        
        struct ibv_values_ex ts_values = {
            .comp_mask = IBV_VALUES_MASK_RAW_CLOCK,
            .raw_clock = {} /*struct timespec*/
        };

        ret_val = ibv_query_rt_values_ex(rdma_dev->context, &ts_values);
        if (ret_val) {
            fprintf(stderr, "ibv_query_rt_values_ex failed after ibv_wr_start call\n");
            ts_values.raw_clock.tv_nsec = 0;
        }
        rdma_dev->latency[cq_wr_id].read_comp_ts = ts_values.raw_clock.tv_nsec;

        uint64_t    wr_complete_latency = rdma_dev->latency[cq_wr_id].wr_complete_ts - rdma_dev->latency[cq_wr_id].wr_start_ts;
        uint64_t    completion_latency  = rdma_dev->latency[cq_wr_id].completion_ts  - rdma_dev->latency[cq_wr_id].wr_start_ts;
        uint64_t    read_comp_latency   = rdma_dev->latency[cq_wr_id].read_comp_ts   - rdma_dev->latency[cq_wr_id].completion_ts;
        
        rdma_dev->measure_index++;
        rdma_dev->wr_complete_latency_sum += wr_complete_latency;
        rdma_dev->completion_latency_sum  += completion_latency;
        rdma_dev->read_comp_latency_sum   += read_comp_latency;

        rdma_dev->min_wr_complete_latency = (wr_complete_latency < rdma_dev->min_wr_complete_latency)?
                                            wr_complete_latency: rdma_dev->min_wr_complete_latency;
        rdma_dev->min_completion_latency  = (completion_latency < rdma_dev->min_completion_latency)?
                                            completion_latency: rdma_dev->min_completion_latency;
        rdma_dev->min_read_comp_latency   = (read_comp_latency < rdma_dev->min_read_comp_latency)?
                                            read_comp_latency: rdma_dev->min_read_comp_latency;
        
        rdma_dev->max_wr_complete_latency = (wr_complete_latency > rdma_dev->max_wr_complete_latency)?
                                            wr_complete_latency: rdma_dev->max_wr_complete_latency;
        rdma_dev->max_completion_latency  = (completion_latency > rdma_dev->max_completion_latency)?
                                            completion_latency: rdma_dev->max_completion_latency;
        rdma_dev->max_read_comp_latency   = (read_comp_latency > rdma_dev->max_read_comp_latency)?
                                            read_comp_latency: rdma_dev->max_read_comp_latency;
        
        DEBUG_LOG_FAST_PATH("PRINT_LATENCY: wr_id = %6lu, wr_sent latency: current %8lu, min %8lu, max %8lu, avg %8lu (nSec)\n",
			cq_wr_id,
			wr_complete_latency * 1000000 / rdma_dev->hca_core_clock_kHz,
			rdma_dev->min_wr_complete_latency * 1000000 / rdma_dev->hca_core_clock_kHz,
			rdma_dev->max_wr_complete_latency * 1000000 / rdma_dev->hca_core_clock_kHz,
			rdma_dev->wr_complete_latency_sum / rdma_dev->measure_index * 1000000 / rdma_dev->hca_core_clock_kHz);

        DEBUG_LOG_FAST_PATH("PRINT_LATENCY:   completion latency           : current %8lu, min %8lu, max %8lu, avg %8lu (nSec)\n",
			completion_latency * 1000000 / rdma_dev->hca_core_clock_kHz,
			rdma_dev->min_completion_latency * 1000000 / rdma_dev->hca_core_clock_kHz,
			rdma_dev->max_completion_latency * 1000000 / rdma_dev->hca_core_clock_kHz,
			rdma_dev->completion_latency_sum / rdma_dev->measure_index * 1000000 / rdma_dev->hca_core_clock_kHz);
        
        DEBUG_LOG_FAST_PATH("PRINT_LATENCY:   read_comp latency            : current %8lu, min %8lu, max %8lu, avg %8lu (nSec)\n",
			read_comp_latency * 1000000 / rdma_dev->hca_core_clock_kHz,
			rdma_dev->min_read_comp_latency * 1000000 / rdma_dev->hca_core_clock_kHz,
			rdma_dev->max_read_comp_latency * 1000000 / rdma_dev->hca_core_clock_kHz,
			rdma_dev->read_comp_latency_sum / rdma_dev->measure_index * 1000000 / rdma_dev->hca_core_clock_kHz);

        ret_val = ibv_next_poll(rdma_dev->cq);
        if ((ret_val) && (ret_val != ENOENT)) {
            perror("ibv_start_poll");
            return reported_entries;
        }
    }
    ibv_end_poll(rdma_dev->cq);
#else /*PRINT_LATENCY*/
    struct ibv_wc wc[COMP_ARRAY_SIZE];
    int    i, wcn;
    
    wcn = ibv_poll_cq(rdma_dev->cq, num_entries, wc);
    if (wcn < 0) {
        fprintf(stderr, "poll CQ failed %d\n", wcn);
        return 0;
    }
    
    for (i = 0; i < wcn; ++i) {
        DEBUG_LOG_FAST_PATH("cqe idx %d: virtual wr_id %llu, original wr_id 0x%llx, num_wrs=%d\n",
                            i, (long long unsigned int)wc[i].wr_id,
                            (long long unsigned int)rdma_dev->app_wr_id[wc[i].wr_id].wr_id,
                            rdma_dev->app_wr_id[wc[i].wr_id].num_wrs);
        if (rdma_dev->app_wr_id[wc[i].wr_id].flags & WR_ID_FLAGS_ACTIVE) {
		rdma_dev->app_wr_id[wc[i].wr_id].flags = 0;
		rdma_dev->qp_available_wr += rdma_dev->app_wr_id[wc[i].wr_id].num_wrs;
		event[reported_entries].wr_id  = rdma_dev->app_wr_id[wc[i].wr_id].wr_id;
  		event[reported_entries].status = wc[i].status;
		reported_entries++;
	}
    }
#endif /*PRINT_LATENCY*/
    return reported_entries;
}
