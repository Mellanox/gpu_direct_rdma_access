/*
 * Copyright (c) 2005 Topspin Communications.  All rights reserved.
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

#ifdef HAVE_CUDA
#include "/usr/local/cuda/include/cuda.h"
#include "/usr/local/cuda/include/cuda_runtime_api.h"
#endif //HAVE_CUDA

#include "write_to_gpu.h"
#include "rdma_write_to_gpu.h"

static int debug = 1;
static int debug_fast_path = 1;
#define DEBUG_LOG if (debug) printf
#define DEBUG_LOG_FAST_PATH if (debug_fast_path) printf
#define FDEBUG_LOG if (debug) fprintf
#define FDEBUG_LOG_FAST_PATH if (debug_fast_path) fprintf

#define CQ_DEPTH    8
#define DC_KEY 0xffeeddcc

/* RDMA control buffer */
struct rdma_device {

    struct ibv_context *context;
    struct ibv_pd      *pd;
    struct ibv_cq      *cq;
    struct ibv_srq     *srq; /* for DCT only, for DCI this is NULL */
    struct ibv_qp      *qp;
    int                 ib_port;
    struct ibv_port_attr    portinfo;
    
    /* QP related fields */
    uint64_t            dc_key;
    uint32_t            dctn; /*QP num*/

    /* Address handler (port info) relateed fields */
    int                 gidx;
    union ibv_gid       gid;
    uint16_t            lid;
};

struct rdma_buffer {
    /* Buffer Related fields */
    void           *buf_addr;
    size_t          buf_size;
    int             use_cuda;

    /* MR Related fields */
    struct ibv_mr  *gpu_mr;
    uint32_t        rkey;
    
    //uint64_t        addr;
    //uint32_t        size;
};

//============================================================================================
//       CUDA START === CUDA START === CUDA START
//============================================================================================
#ifdef HAVE_CUDA
#define ASSERT(x)   \
    do {            \
        if (!(x)) { \
            fprintf(stdout, "Assertion \"%s\" failed at %s:%d\n", #x, __FILE__, __LINE__);\
        }           \
    } while (0)

#define CUCHECK(stmt)                   \
    do {                                \
        CUresult result = (stmt);       \
        ASSERT(CUDA_SUCCESS == result); \
    } while (0)

/*----------------------------------------------------------------------------*/

static CUdevice cuDevice;
static CUcontext cuContext;

static int init_gpu(struct rdma_device *cb, size_t _size)
{
    const size_t gpu_page_size = 64*1024;
    size_t size = (_size + gpu_page_size - 1) & ~(gpu_page_size - 1);
    printf("initializing CUDA\n");
    CUresult error = cuInit(0);
    if (error != CUDA_SUCCESS) {
        printf("cuInit(0) returned %d\n", error);
        exit(1);
    }

    int deviceCount = 0;
    error = cuDeviceGetCount(&deviceCount);
    if (error != CUDA_SUCCESS) {
        printf("cuDeviceGetCount() returned %d\n", error);
        exit(1);
    }
    /* This function call returns 0 if there are no CUDA capable devices. */
    if (deviceCount == 0) {
        printf("There are no available device(s) that support CUDA\n");
        return 1;
    } else if (deviceCount == 1)
        printf("There is 1 device supporting CUDA\n");
    else
        printf("There are %d devices supporting CUDA, picking first...\n", deviceCount);

    int devID = 0;

    /* pick up device with zero ordinal (default, or devID) */
    CUCHECK(cuDeviceGet(&cuDevice, devID));

    char name[128];
    CUCHECK(cuDeviceGetName(name, sizeof(name), devID));
    printf("[pid = %d, dev = %d] device name = [%s]\n", getpid(), cuDevice, name);
    printf("creating CUDA Contnext\n");

    /* Create context */
    error = cuCtxCreate(&cuContext, CU_CTX_MAP_HOST, cuDevice);
    if (error != CUDA_SUCCESS) {
        printf("cuCtxCreate() error=%d\n", error);
        return 1;
    }

    printf("making it the current CUDA Context\n");
    error = cuCtxSetCurrent(cuContext);
    if (error != CUDA_SUCCESS) {
        printf("cuCtxSetCurrent() error=%d\n", error);
        return 1;
    }

    printf("cuMemAlloc() of a %zd bytes GPU buffer\n", size);
    CUdeviceptr d_A;
    error = cuMemAlloc(&d_A, size);
    if (error != CUDA_SUCCESS) {
        printf("cuMemAlloc error=%d\n", error);
        return 1;
    }
    printf("allocated GPU buffer address at %016llx pointer=%p\n", d_A,
           (void *) d_A);
    cb->gpu_buf = (void*)d_A;

    return 0;
}

static int free_gpu(struct rdma_device *cb)
{
    CUdeviceptr d_A = (CUdeviceptr) cb->gpu_buf;

    printf("deallocating RX GPU buffer\n");
    cuMemFree(d_A);
    d_A = 0;

    printf("destroying current CUDA Context\n");
    CUCHECK(cuCtxDestroy(cuContext));

    return 0;
}
#endif //HAVE_CUDA
//============================================================================================
struct rdma_device *rdma_open_device_target(const char *ib_dev_name, int ib_port) /* client */
{
    struct rdma_device     *rdma_dev;
    struct ibv_device     **dev_list;
    struct ibv_device      *ib_dev;
    int     ret_val;
    int     i;

    rdma_dev = calloc(1, sizeof *rdma_dev);
    if (!rdma_dev) {
        fprintf(stderr, "rdma_device memory allocation failed\n");
        return NULL;
    }

    /****************************************************************************************************
     * In the next block we are checking if given IB device name matches one of devices in the list.
     * The result of this block is ig_dev - initialized pointer to the relevant struct ibv_device
     ****************************************************************************************************/
    dev_list = ibv_get_device_list(NULL);
    if (!dev_list) {
        perror("Failed to get IB devices list");
        goto cleea_device_list;
    }

    DEBUG_LOG ("Given device name \"%s\"\n", ib_dev_name);
    for (i = 0; dev_list[i]; ++i) {
        char *dev_name_from_list = (char*)ibv_get_device_name(dev_list[i]);
        DEBUG_LOG ("Device %d name \"%s\"\n", i, dev_name_from_list);
        if (!strcmp(dev_name_from_list, ib_dev_name)) /*if found*/
            break;
    }
    ib_dev = dev_list[i];
    if (!ib_dev) {
        fprintf(stderr, "IB device %s not found\n", ib_dev_name);
        goto cleea_device_list;
    }
    /****************************************************************************************************/

    DEBUG_LOG ("ibv_open_device(ib_dev = %p)\n", ib_dev);
    rdma_dev->context = ibv_open_device(ib_dev);
    if (!rdma_dev->context) {
        fprintf(stderr, "Couldn't get context for %s\n", ib_dev_name);
        goto cleea_device_list;
    }
    DEBUG_LOG("created ib context %p\n", rdma_dev->context);
    /* We are now done with device list, free it */
    ibv_free_device_list(dev_list);
    dev_list = NULL;
    ib_dev = NULL;    
    
    DEBUG_LOG ("ibv_alloc_pd(ibv_context = %p)\n", rdma_dev->context);
    rdma_dev->pd = ibv_alloc_pd(rdma_dev->context);
    if (!rdma_dev->pd) {
        fprintf(stderr, "Couldn't allocate PD\n");
        goto clean_device;
    }
    DEBUG_LOG("created pd %p\n", rdma_dev->pd);

    DEBUG_LOG ("ibv_create_cq(%p, %d, NULL, NULL, 0)\n", rdma_dev->context, CQ_DEPTH);
    rdma_dev->cq = ibv_create_cq(rdma_dev->context, CQ_DEPTH, NULL, NULL, 0);
    if (!rdma_dev->cq) {
        fprintf(stderr, "Couldn't create CQ\n");
        goto clean_pd;
    }
    DEBUG_LOG("created cq %p\n", rdma_dev->cq);

    /* - - - - - - -  Create SRQ  - - - - - - - */
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

    /* - - - - - - -  Create QP  - - - - - - - */
    struct ibv_qp_init_attr_ex attr_ex;
    struct mlx5dv_qp_init_attr attr_dv;

    memset(&attr_ex, 0, sizeof(attr_ex));
    memset(&attr_dv, 0, sizeof(attr_dv));

    attr_ex.qp_type = IBV_QPT_DRIVER;
    attr_ex.send_cq = rdma_dev->cq;
    attr_ex.recv_cq = rdma_dev->cq;

    attr_ex.comp_mask |= IBV_QP_INIT_ATTR_PD;
    attr_ex.pd = rdma_dev->pd;
    attr_ex.srq = rdma_dev->srq; /* Should use SRQ for client only (DCT) */

    /* create DCT */
    attr_dv.comp_mask |= MLX5DV_QP_INIT_ATTR_MASK_DC;
    attr_dv.dc_init_attr.dc_type = MLX5DV_DCTYPE_DCT;
    attr_dv.dc_init_attr.dct_access_key = DC_KEY;

    DEBUG_LOG ("mlx5dv_create_qp(%p,%p,%p)\n", rdma_dev->context, &attr_ex, &attr_dv);
    rdma_dev->qp = mlx5dv_create_qp(rdma_dev->context, &attr_ex, &attr_dv);

    if (!rdma_dev->qp)  {
        fprintf(stderr, "Couldn't create QP\n");
        goto clean_srq;
    }

    /* - - - - - - -  Modify QP to INIT  - - - - - - - */
    struct ibv_qp_attr attr = {
        .qp_state        = IBV_QPS_INIT,
        .pkey_index      = 0,
        .port_num        = ib_port,
        .qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE
    };
    enum ibv_qp_attr_mask attr_mask = IBV_QP_STATE      |
                                      IBV_QP_PKEY_INDEX |
                                      IBV_QP_PORT       |
                                      IBV_QP_ACCESS_FLAGS;
    ret_val = ibv_modify_qp(rdma_dev->qp, &attr, attr_mask);
    if (ret_val) {
        fprintf(stderr, "Failed to modify QP to INIT, error %d\n", ret_val);
        goto clean_qp;
    }

    rdma_dev->ib_port = ib_port;
    rdma_dev->dc_key  = DC_KEY;
    rdma_dev->dctn    = rdma_dev->qp->qp_num;
    rdma_dev->gidx    = -1; /*default value (no GID)*/

    return rdma_dev;

clean_qp:
    if (rdma_dev->qp) {
        ibv_destroy_qp(rdma_dev->qp);
    }

clean_srq:
    if (rdma_dev->srq) {
        ibv_destroy_srq(rdma_dev->srq);
    }

clean_cq:
    if (rdma_dev->cq) {
        ibv_destroy_cq(rdma_dev->cq);
    }

clean_pd:
    if (rdma_dev->pd) {
        ibv_dealloc_pd(rdma_dev->pd);
    }

clean_device:
    if (rdma_dev->context) {
        ibv_close_device(rdma_dev->context);
    }

clean_device_list:
    if (dev_list) {
        ibv_free_device_list(dev_list);
    }

    return NULL;
}

/****************************************************************************************
 * Fill portinfo tructure, get lid and gid from portinfo
 * Return value: 0 - success, 1 - error
 ****************************************************************************************/
int rdma_set_lid_gid_from_port_info(struct rdma_device *rdma_dev, int gidx)
{
    int ret_val;

	ret_val = ibv_query_port(rdma_dev->context, rdma_dev->ib_port, &(rdma_dev->portinfo));
    if (ret_val) {
        fprintf(stderr, "Couldn't get port info\n");
        return 1;
    }

    rdma_dev->lid = rdma_dev->portinfo.lid;
    if ((rdma_dev->portinfo.link_layer != IBV_LINK_LAYER_ETHERNET) && (!my_dest.lid)) {
        fprintf(stderr, "Couldn't get local LID\n");
        return 1;
    }
    
    if (gidx < 0) {
        if (rdma_dev->portinfo.link_layer == IBV_LINK_LAYER_ETHERNET) {
            fprintf(stderr, "Wrong GID index (%d) for ETHERNET port\n", gidx);
            return 1;
        } else {
            memset(&(rdma_dev->gid), 0, sizeof rdma_dev->gid);
        }
    } else /* gidx >= 0*/ {
        ret_val = ibv_query_gid(cb->context, rdma_dev->ib_port, gidx, &(rdma_dev->gid))
        if (ret_val) {
            fprintf(stderr, "can't read GID of index %d, error code %d\n", gidx, ret_val);
            return 1;
        }
        DEBUG_LOG ("My GID: %02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x\n",
                   rdma_dev->gid.raw[0], rdma_dev->gid.raw[1], rdma_dev->gid.raw[2], rdma_dev->gid.raw[3],
                   rdma_dev->gid.raw[4], rdma_dev->gid.raw[5], rdma_dev->gid.raw[6], rdma_dev->gid.raw[7], 
                   rdma_dev->gid.raw[8], rdma_dev->gid.raw[9], rdma_dev->gid.raw[10], rdma_dev->gid.raw[11],
                   rdma_dev->gid.raw[12], rdma_dev->gid.raw[13], rdma_dev->gid.raw[14], rdma_dev->gid.raw[15] );
    }
    rdma_dev->gidx = gidx;
    return 0;
}

/****************************************************************************************
 * Modify target QP state to RTR (on the client side)
 * Return value: 0 - success, 1 - error
 ****************************************************************************************/
int modify_target_qp_to_rtr(struct rdma_device *rdma_dev, enum ibv_mtu mtu)
{
    struct ibv_qp_attr      qp_attr;
    enum ibv_qp_attr_mask   attr_mask;

    memset(&qp_attr, 0, sizeof qp_attr);
    qp_attr.qp_state       = IBV_QPS_RTR,
    qp_attr.path_mtu       = mtu,
    qp_attr.min_rnr_timer  = 16,
    qp_attr.ah_attr.port_num    = rdma_dev->ib_port;

    if (rdma_dev->gid.global.interface_id) {
        qp_attr.ah_attr.is_global = 1;
        qp_attr.ah_attr.grh.hop_limit  = 1;
        qp_attr.ah_attr.grh.sgid_index = rdma_dev->gidx;
    }
    attr_mask = IBV_QP_STATE          |
                IBV_QP_AV             |
                IBV_QP_PATH_MTU       |
                IBV_QP_MIN_RNR_TIMER; // for DCT

    if (ibv_modify_qp(cb->qp, &attr, attr_mask)) {
        fprintf(stderr, "Failed to modify QP to RTR\n");
        return 1;
    }

    return 0;
}

//============================================================================================
struct rdma_buffer *rdma_buffer_reg(struct rdma_device device, void *addr, size_t length)
{
    struct rdma_buffer *rdma_buff;

    //TODO

    return rdma_buff;
}

//============================================================================================
void rdma_buffer_dereg(struct rdma_buffer *buffer)
{
    //TODO
}

#if 0
static struct pingpong_dest *pp_client_exch_dest(struct rdma_device *cb,
                                                 const char *servername,
                                                 int port,
                                                 const struct pingpong_dest *my_dest)
{
    struct addrinfo *res, *t;
    struct addrinfo hints = {
        .ai_family   = AF_UNSPEC,
        .ai_socktype = SOCK_STREAM
    };
    char *service;
    char msg[sizeof "0000:000000:000000:00000000000000000000000000000000"];
    int n;
    struct pingpong_dest *rem_dest = NULL;
    char gid[33];

    if (asprintf(&service, "%d", port) < 0)
        return NULL;

    n = getaddrinfo(servername, service, &hints, &res);

    if (n < 0) {
        fprintf(stderr, "%s for %s:%d\n", gai_strerror(n), servername, port);
        free(service);
        return NULL;
    }

    for (t = res; t; t = t->ai_next) {
        cb->sockfd = socket(t->ai_family, t->ai_socktype, t->ai_protocol);
        if (cb->sockfd >= 0) {
            if (!connect(cb->sockfd, t->ai_addr, t->ai_addrlen))
                break;
            close(cb->sockfd);
            cb->sockfd = -1;
        }
    }

    freeaddrinfo(res);
    free(service);

    if (cb->sockfd < 0) {
        fprintf(stderr, "Couldn't connect to %s:%d\n", servername, port);
        return NULL;
    }

    gid_to_wire_gid(&my_dest->gid, gid);
    sprintf(msg, "%04x:%06x:%06x:%s", my_dest->lid, my_dest->qpn, my_dest->psn, gid);
    DEBUG_LOG ("exch_dest:  before send  \"%s\"\n", msg);
    if (write(cb->sockfd, msg, sizeof msg) != sizeof msg) {
        fprintf(stderr, "Couldn't send local address\n");
        return NULL;
    }

    if (recv(cb->sockfd, msg, sizeof(msg), MSG_WAITALL) != sizeof(msg)) {
        perror("client read");
        fprintf(stderr, "Couldn't read remote address\n");
        return NULL;
    }
    DEBUG_LOG ("exch_dest: after receive \"%s\"\n", msg);

//    if (write(cb->sockfd, "done", sizeof("done")) != sizeof("done")) {
//        fprintf(stderr, "Couldn't send \"done\" msg\n");
//        return NULL;
//    }

    rem_dest = malloc(sizeof *rem_dest);
    if (!rem_dest) {
        fprintf(stderr, "rem_dest memory allocation failed\n");
        return NULL;
    }

    sscanf(msg, "%x:%x:%x:%s", &rem_dest->lid, &rem_dest->qpn,
                        &rem_dest->psn, gid);
    wire_gid_to_gid(gid, &rem_dest->gid);
    DEBUG_LOG ("Rem GID: %02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x\n",
           rem_dest->gid.raw[0], rem_dest->gid.raw[1], rem_dest->gid.raw[2], rem_dest->gid.raw[3],
           rem_dest->gid.raw[4], rem_dest->gid.raw[5], rem_dest->gid.raw[6], rem_dest->gid.raw[7], 
           rem_dest->gid.raw[8], rem_dest->gid.raw[9], rem_dest->gid.raw[10], rem_dest->gid.raw[11],
           rem_dest->gid.raw[12], rem_dest->gid.raw[13], rem_dest->gid.raw[14], rem_dest->gid.raw[15] );

    return rem_dest;
}

static struct rdma_device *pp_init_ctx(struct ibv_device *ib_dev,
                                   struct user_params *usr_par)
{
    struct rdma_device *cb;
    int             ret;

    cb = calloc(1, sizeof *cb);
    if (!cb)
        return NULL;

    cb->gpu_buf_size = usr_par->size;
    cb->sockfd       = -1;

#ifdef HAVE_CUDA
    cb->use_cuda = usr_par->use_cuda;
    if (cb->use_cuda) {
        if (init_gpu(cb, cb->gpu_buf_size)) {
            fprintf(stderr, "Couldn't allocate work buf.\n");
            return NULL;
        }
    } else
#endif //HAVE_CUDA
    {
        // Mem allocation on CPU
        int page_size = sysconf(_SC_PAGESIZE);
        cb->gpu_buf = memalign(page_size, usr_par->size);
        if (!cb->gpu_buf) {
            fprintf(stderr, "Couldn't allocate buffer for rdma_write from server.\n");
            goto clean_ctx;
        }
    }
    DEBUG_LOG ("ibv_open_device(%p)\n", ib_dev);
    cb->context = ibv_open_device(ib_dev);
    if (!cb->context) {
        fprintf(stderr, "Couldn't get context for %s\n",
            ibv_get_device_name(ib_dev));
        goto clean_buffer;
    }


    DEBUG_LOG ("ibv_alloc_pd(%p)\n", cb->context);
    cb->pd = ibv_alloc_pd(cb->context);
    if (!cb->pd) {
        fprintf(stderr, "Couldn't allocate PD\n");
        goto clean_device;
    }

    cb->gpu_mr = ibv_reg_mr(cb->pd, cb->gpu_buf, usr_par->size,
                             IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE );
    DEBUG_LOG("ibv_reg_mr completed: buf %p, size = %lu, rkey = 0x%08x\n",
               cb->gpu_buf, usr_par->size, cb->gpu_mr->rkey);

    if (!cb->gpu_mr) {
        fprintf(stderr, "Couldn't register GPU MR\n");
        goto clean_pd;
    }
    
#ifdef HAVE_CUDA
    if (!cb->use_cuda) {
        /* FIXME memset(cb->buf, 0, size); */
        memset(cb->gpu_buf, 0x7b, usr_par->size);
    }
#else
    /* FIXME memset(cb->buf, 0, size); */
    memset(cb->gpu_buf, 0x7b, usr_par->size);
#endif //HAVE_CUDA

    DEBUG_LOG ("ibv_create_cq(%p, %d, NULL, NULL, 0)\n", cb->context, CQ_DEPTH);
    cb->cq = ibv_create_cq(cb->context, CQ_DEPTH, NULL, NULL, 0);
    if (!cb->cq) {
        fprintf(stderr, "Couldn't create CQ\n");
        goto clean_mr;
    }

    /* - - - - - - -  Create SRQ  - - - - - - - */
    struct ibv_srq_init_attr srq_attr;
    memset(&srq_attr, 0, sizeof(srq_attr));
    srq_attr.attr.max_wr = 2;
    srq_attr.attr.max_sge = 1;
    cb->srq = ibv_create_srq(cb->pd, &srq_attr);
    if (!cb->srq) {
        fprintf(stderr, "ibv_create_srq failed\n");
        goto clean_cq;
    }
    DEBUG_LOG("created srq %p\n", cb->srq);

    {
        struct ibv_qp_init_attr_ex attr_ex;
        struct mlx5dv_qp_init_attr attr_dv;

        memset(&attr_ex, 0, sizeof(attr_ex));
        memset(&attr_dv, 0, sizeof(attr_dv));

        attr_ex.qp_type = IBV_QPT_DRIVER;
        attr_ex.send_cq = cb->cq;
        attr_ex.recv_cq = cb->cq;

        attr_ex.comp_mask |= IBV_QP_INIT_ATTR_PD;
        attr_ex.pd = cb->pd;
        attr_ex.srq = cb->srq; /* Should use SRQ for client only (DCT) */
        
        /* create DCT */
        attr_dv.comp_mask |= MLX5DV_QP_INIT_ATTR_MASK_DC;
        attr_dv.dc_init_attr.dc_type = MLX5DV_DCTYPE_DCT;
        attr_dv.dc_init_attr.dct_access_key = DC_KEY;

        DEBUG_LOG ("mlx5dv_create_qp(%p,%p,%p)\n", cb->context, &attr_ex, &attr_dv);
        cb->qp = mlx5dv_create_qp(cb->context, &attr_ex, &attr_dv);

        if (!cb->qp)  {
            fprintf(stderr, "Couldn't create QP\n");
            goto clean_srq;
        }
    }

    {
        struct ibv_qp_attr attr = {
            .qp_state        = IBV_QPS_INIT,
            .pkey_index      = 0,
            .port_num        = usr_par->ib_port,
            .qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE
        };

        if (ibv_modify_qp(cb->qp, &attr,
                  IBV_QP_STATE              |
                  IBV_QP_PKEY_INDEX         |
                  IBV_QP_PORT               |
                  IBV_QP_ACCESS_FLAGS)) {
            fprintf(stderr, "Failed to modify QP to INIT\n");
            goto clean_qp;
        }
    }


    return cb;


clean_qp:
    ibv_destroy_qp(cb->qp);

clean_cq:
    ibv_destroy_cq(cb->cq);

clean_srq:
    ibv_destroy_srq(cb->srq);

clean_mr:
    ibv_dereg_mr(cb->gpu_mr);

clean_pd:
    ibv_dealloc_pd(cb->pd);

clean_device:
    ibv_close_device(cb->context);

clean_buffer:
#ifdef HAVE_CUDA
    if (cb->use_cuda) {
        free_gpu(cb);
    } else
#endif //HAVE_CUDA
    {
        free(cb->gpu_buf);
    }

clean_ctx:
    free(cb);

    return NULL;
}

int pp_close_ctx(struct rdma_device *cb)
{
    int rc;
    DEBUG_LOG("ibv_destroy_qp(%p)\n", cb->qp);
    rc = ibv_destroy_qp(cb->qp);
    if (rc) {
        fprintf(stderr, "Couldn't destroy QP: error %d\n", rc);
        return 1;
    }

    DEBUG_LOG("ibv_destroy_cq(%p)\n", cb->cq);
    rc = ibv_destroy_cq(cb->cq);
    if (rc) {
        fprintf(stderr, "Couldn't destroy CQ, error %d\n", rc);
        return 1;
    }

    if (cb->srq) {
        DEBUG_LOG("ibv_destroy_srq(%p)\n", cb->srq);
        rc = ibv_destroy_srq(cb->srq);
        if (rc) {
            fprintf(stderr, "Couldn't destroy SRQ\n");
            return 1;
        }
    }
    
    DEBUG_LOG("ibv_dereg_mr(%p)\n", cb->gpu_mr);
    rc = ibv_dereg_mr(cb->gpu_mr);
    if (rc) {
        fprintf(stderr, "Couldn't deregister MR, error %d\n", rc);
        return 1;
    }
    
    DEBUG_LOG("ibv_dealloc_pd(%p)\n", cb->pd);
    rc = ibv_dealloc_pd(cb->pd);
    if (rc) {
        fprintf(stderr, "Couldn't deallocate PD, error %d\n", rc);
        return 1;
    }

    DEBUG_LOG("ibv_close_device(%p)\n", cb->context);
    rc = ibv_close_device(cb->context);
    if (rc) {
        fprintf(stderr, "Couldn't release context, error %d\n", rc);
        return 1;
    }

    if (cb->sockfd != -1) {
        DEBUG_LOG("close socket(%p)\n", cb->sockfd);
        close(cb->sockfd);
    }
    
#ifdef HAVE_CUDA
    if (cb->use_cuda) {
        free_gpu(cb);
    } else
#endif //HAVE_CUDA
    {
        DEBUG_LOG("free memory buffer(%p)\n", cb->gpu_buf);
        free(cb->gpu_buf);
    }

    free(cb);

    return 0;
}

#endif
