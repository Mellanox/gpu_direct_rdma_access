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

#define CQ_DEPTH        8
#define SEND_Q_DEPTH    64
#define MAX_SEND_SGE    16
#define DC_KEY          0xffeeddcc

/* RDMA control buffer */
struct rdma_device {

    struct ibv_context *context;
    struct ibv_pd      *pd;
    struct ibv_cq      *cq;
    struct ibv_srq     *srq; /* for DCT (client) only, for DCI (server) this is NULL */
    struct ibv_qp      *qp;
    struct ibv_qp_ex       *qpex;  /* DCI (server) only */
    struct mlx5dv_qp_ex    *mqpex; /* DCI (server) only */
    int                 ib_port;
    int                 rdma_buff_cnt;
    
    /* QP related fields for DCT (client) side, DCI (server) side receives these from socket */
    uint64_t            dc_key;
    uint32_t            dctn; /*QP num*/

    /* Address handler (port info) relateed fields */
    int                 is_global;
    int                 gidx;
    union ibv_gid       gid;
    uint16_t            lid;
};

struct rdma_buffer {
    /* Buffer Related fields */
    void           *buf_addr;   //uint64_t  addr;
    size_t          buf_size;   //uint32_t  size;
//    int             use_cuda;
    rdma_device    *rdma_dev;

    /* MR Related fields */
    struct ibv_mr  *mr;
    uint32_t        rkey;
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

static void *init_gpu(size_t gpu_buf_size)
{
    const size_t    gpu_page_size = 64*1024;
    size_t          aligned_size;
    CUresult        error;

    aligned_size = (gpu_buf_size + gpu_page_size - 1) & ~(gpu_page_size - 1);
    printf("initializing CUDA\n");
    error = cuInit(0);
    if (error != CUDA_SUCCESS) {
        fprintf(stderr, "cuInit(0) returned %d\n", error);
        return NULL;
    }

    int deviceCount = 0;
    error = cuDeviceGetCount(&deviceCount);
    if (error != CUDA_SUCCESS) {
        fprintf(stderr, "cuDeviceGetCount() returned %d\n", error);
        return NULL;
    }

    /* This function call returns NULL if there are no CUDA capable devices. */
    if (deviceCount == 0) {
        fprintf(stderr, "There are no available device(s) that support CUDA\n");
        return NULL;
    } else if (deviceCount == 1) {
        DEBUG_LOG("There is 1 device supporting CUDA\n");
    } else {
        DEBUG_LOG("There are %d devices supporting CUDA, picking first...\n", deviceCount);
    }

    int devID = 0;

    /* pick up device with zero ordinal (default, or devID) */
    CUCHECK(cuDeviceGet(&cuDevice, devID));

    char name[128];
    CUCHECK(cuDeviceGetName(name, sizeof(name), devID));
    DEBUG_LOG("[pid = %d, dev = %d] device name = [%s]\n", getpid(), cuDevice, name);
    DEBUG_LOG("creating CUDA Contnext\n");

    /* Create context */
    error = cuCtxCreate(&cuContext, CU_CTX_MAP_HOST, cuDevice);
    if (error != CUDA_SUCCESS) {
        fprintf(stderr, "cuCtxCreate() error=%d\n", error);
        return NULL;
    }

    DEBUG_LOG("making it the current CUDA Context\n");
    error = cuCtxSetCurrent(cuContext);
    if (error != CUDA_SUCCESS) {
        fprintf(stderr, "cuCtxSetCurrent() error=%d\n", error);
        return NULL;
    }

    DEBUG_LOG("cuMemAlloc() of a %zd bytes GPU buffer\n", aligned_size);
    CUdeviceptr d_A;
    error = cuMemAlloc(&d_A, aligned_size);
    if (error != CUDA_SUCCESS) {
        fprintf(stderr, "cuMemAlloc error=%d\n", error);
        return NULL;
    }
    DEBUG_LOG("allocated GPU buffer address at %016llx pointer=%p\n", d_A, (void*)d_A);

    return ((void*)d_A);
}

static int free_gpu(void *gpu_buff)
{
    CUdeviceptr d_A = (CUdeviceptr) gpu_buff;

    printf("deallocating RX GPU buffer\n");
    cuMemFree(d_A);
    d_A = 0;

    DEBUG_LOG("destroying current CUDA Context\n");
    CUCHECK(cuCtxDestroy(cuContext));

    return 0;
}
#endif //HAVE_CUDA
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
struct rdma_device *rdma_open_device_target(const char *ib_dev_name, int ib_port) /* client */
{
    struct rdma_device *rdma_dev;
    int                 ret_val;

    rdma_dev = calloc(1, sizeof *rdma_dev);
    if (!rdma_dev) {
        fprintf(stderr, "rdma_device memory allocation failed\n");
        return NULL;
    }

    /****************************************************************************************************
     * In the next function we are checking if given IB device name matches one of devices in the list,
     * if yes, we open device by the given name and return pointer to the ib context
     * The result of this function is ig_dev - initialized pointer to the relevant struct ibv_device
     ****************************************************************************************************/
    rdma_dev->context = open_ib_device_by_name(ib_dev_name);
    if (!rdma_dev->context){
        return NULL;
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
    DEBUG_LOG ("ibv_create_cq(%p, %d, NULL, NULL, 0)\n", rdma_dev->context, CQ_DEPTH);
    rdma_dev->cq = ibv_create_cq(rdma_dev->context, CQ_DEPTH, NULL, NULL, 0);
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

    return NULL;
}

//============================================================================================
struct rdma_device *rdma_open_device_source(const char *ib_dev_name, int ib_port) /* server */
{
    struct rdma_device *rdma_dev;
    int                 ret_val;

    rdma_dev = calloc(1, sizeof *rdma_dev);
    if (!rdma_dev) {
        fprintf(stderr, "rdma_device memory allocation failed\n");
        return NULL;
    }

    /****************************************************************************************************
     * In the next function we are checking if given IB device name matches one of devices in the list,
     * if yes, we open device by the given name and return pointer to the ib context
     * The result of this function is ig_dev - initialized pointer to the relevant struct ibv_device
     ****************************************************************************************************/
    rdma_dev->context = open_ib_device_by_name(ib_dev_name);
    if (!rdma_dev->context){
        return NULL;
    }
    
    DEBUG_LOG ("ibv_alloc_pd(ibv_context = %p)\n", rdma_dev->context);
    rdma_dev->pd = ibv_alloc_pd(rdma_dev->context);
    if (!rdma_dev->pd) {
        fprintf(stderr, "Couldn't allocate PD\n");
        goto clean_device;
    }
    DEBUG_LOG("created pd %p\n", rdma_dev->pd);

    /* We don't create completion events channel (ibv_create_comp_channel), we prefer working in polling mode */
    
    /* **********************************  Create CQ  ********************************** */
    DEBUG_LOG ("ibv_create_cq(%p, %d, NULL, NULL, 0)\n", rdma_dev->context, CQ_DEPTH);
    rdma_dev->cq = ibv_create_cq(rdma_dev->context, CQ_DEPTH, NULL, NULL /*comp. events channel*/, 0);
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
    attr_ex.send_cq = rdma_dev->cq;
    attr_ex.recv_cq = rdma_dev->cq;

    attr_ex.comp_mask |= IBV_QP_INIT_ATTR_PD;
    attr_ex.pd = rdma_dev->pd;

    /* create DCI */
    attr_dv.comp_mask |= MLX5DV_QP_INIT_ATTR_MASK_DC;
    attr_dv.dc_init_attr.dc_type = MLX5DV_DCTYPE_DCI;
    
    attr_ex.cap.max_send_wr  = SEND_Q_DEPTH;
    attr_ex.cap.max_send_sge = MAX_SEND_SGE; // in old example 1???

    attr_ex.comp_mask |= IBV_QP_INIT_ATTR_SEND_OPS_FLAGS;
    attr_ex.send_ops_flags = IBV_QP_EX_WITH_RDMA_WRITE/* | IBV_QP_EX_WITH_RDMA_READ*/;

    attr_dv.comp_mask |= MLX5DV_QP_INIT_ATTR_MASK_QP_CREATE_FLAGS;
    attr_dv.create_flags |= MLX5DV_QP_CREATE_DISABLE_SCATTER_TO_CQE; /*driver doesnt support scatter2cqe data-path on DCI yet*/
    
    DEBUG_LOG ("mlx5dv_create_qp(%p,%p,%p)\n", rdma_dev->context, &attr_ex, &attr_dv);
    rdma_dev->qp = mlx5dv_create_qp(rdma_dev->context, &attr_ex, &attr_dv);

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

    /* - - - - - - -  Modify QP to INIT  - - - - - - - */
    struct ibv_qp_attr attr = {
        .qp_state        = IBV_QPS_INIT,
        .pkey_index      = 0,
        .port_num        = ib_port,
        .qp_access_flags = IBV_ACCESS_LOCAL_WRITE
    };
    enum ibv_qp_attr_mask attr_mask = IBV_QP_STATE      |
                                      IBV_QP_PKEY_INDEX |
                                      IBV_QP_PORT       |
                                      0 /*IBV_QP_ACCESS_FLAGS*/; /*we must zero this bit for DCI QP*/
    ret_val = ibv_modify_qp(rdma_dev->qp, &attr, attr_mask);
    if (ret_val) {
        fprintf(stderr, "Failed to modify QP to INIT, error %d\n", ret_val);
        goto clean_qp;
    }

    rdma_dev->ib_port = ib_port;
    rdma_dev->gidx    = -1; /*default value (no GID)*/

    return rdma_dev;

clean_qp:
    if (rdma_dev->qp) {
        ibv_destroy_qp(rdma_dev->qp);
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

    return NULL;
}

//============================================================================================
void rdma_close_device(struct rdma_device *rdma_dev)
{
    int ret_val;

    if (rdma_dev->rdma_buff_cnt > 0) {
        fprintf(stderr, "The number of attached RDMA buffers is not zero (%d). Can't close device.\n", rdma_buff_cnt);
        return;
    }
    DEBUG_LOG("ibv_destroy_qp(%p)\n", rdma_dev->qp);
    ret_val = ibv_destroy_qp(rdma_dev->qp);
    if (ret_val) {
        fprintf(stderr, "Couldn't destroy QP: error %d\n", ret_val);
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
    ret_val = ibv_destroy_cq(rdma_dev->cq);
    if (ret_val) {
        fprintf(stderr, "Couldn't destroy CQ, error %d\n", ret_val);
        return;
    }

    DEBUG_LOG("ibv_dealloc_pd(%p)\n", rdma_dev->pd);
    ret_val = ibv_dealloc_pd(rdma_dev->pd);
    if (ret_val) {
        fprintf(stderr, "Couldn't deallocate PD, error %d\n", ret_val);
        return;
    }

    DEBUG_LOG("ibv_close_device(%p)\n", rdma_dev->context);
    ret_val = ibv_close_device(rdma_dev->context);
    if (ret_val) {
        fprintf(stderr, "Couldn't release context, error %d\n", ret_val);
        return;
    }

    free(rdma_dev);

    return;
}

/****************************************************************************************
 * Fill portinfo structure, get lid and gid from portinfo
 * Return value: 0 - success, 1 - error
 ****************************************************************************************/
int rdma_set_lid_gid_from_port_info(struct rdma_device *rdma_dev, int gidx)
{
    struct ibv_port_attr    portinfo;
    int    ret_val;

    ret_val = ibv_query_port(rdma_dev->context, rdma_dev->ib_port, &portinfo);
    if (ret_val) {
        fprintf(stderr, "Couldn't get port info\n");
        return 1;
    }

    rdma_dev->lid = portinfo.lid;
    if ((portinfo.link_layer != IBV_LINK_LAYER_ETHERNET) && (!portinfo.lid)) {
        fprintf(stderr, "Couldn't get local LID\n");
        return 1;
    }
    
    if (gidx < 0) {
        if (portinfo.link_layer == IBV_LINK_LAYER_ETHERNET) {
            fprintf(stderr, "Wrong GID index (%d) for ETHERNET port\n", gidx);
            return 1;
        } else {
            memset(&(rdma_dev->gid), 0, sizeof rdma_dev->gid);
        }
    } else /* gidx >= 0*/ {
        ret_val = ibv_query_gid(rdma_dev->context, rdma_dev->ib_port, gidx, &(rdma_dev->gid))
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
    rdma_dev->is_global = (rdma_dev->gid.global.interface_id != 0);
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
    qp_attr.qp_state       = IBV_QPS_RTR;
    qp_attr.path_mtu       = mtu;
    qp_attr.min_rnr_timer  = 16;
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

    if (ibv_modify_qp(rdma_dev->qp, &attr, attr_mask)) {
        fprintf(stderr, "Failed to modify QP to RTR\n");
        return 1;
    }

    return 0;
}

/****************************************************************************************
 * Modify source QP state to RTR and then to RTS (on the server side)
 * Return value: 0 - success, 1 - error
 ****************************************************************************************/
int modify_source_qp_to_rtr_and_rts(struct rdma_device *rdma_dev, enum ibv_mtu mtu)
{
    struct ibv_qp_attr      qp_attr;
    enum ibv_qp_attr_mask   attr_mask;

    memset(&qp_attr, 0, sizeof qp_attr);
    
    /* - - - - - - -  Modify QP to RTR  - - - - - - - */
    qp_attr.qp_state = IBV_QPS_RTR;
    qp_attr.path_mtu = mtu;
    qp_attr.ah_attr.port_num = rdma_dev->ib_port;

    if (rdma_dev->gid.global.interface_id) {
        qp_attr.ah_attr.is_global = 1;
        qp_attr.ah_attr.grh.hop_limit  = 1;
        qp_attr.ah_attr.grh.sgid_index = rdma_dev->gidx;
    }
    enum ibv_qp_attr_mask attr_mask;

    attr_mask = IBV_QP_STATE              |
                IBV_QP_AV                 |
                IBV_QP_PATH_MTU           ;

    if (ibv_modify_qp(rdma_dev->qp, &attr, attr_mask)) {
        fprintf(stderr, "Failed to modify QP to RTR\n");
        return 1;
    }

    /* - - - - - - -  Modify QP to RTS  - - - - - - - */
    qp_attr.qp_state       = IBV_QPS_RTS;
    qp_attr.timeout        = 16;
    qp_attr.retry_cnt      = 7;
    qp_attr.rnr_retry      = 7;
    //qp_attr.sq_psn         = 0;
    qp_attr.max_rd_atomic  = 1;
    qp_attr_mask = IBV_QP_STATE            |
                   IBV_QP_TIMEOUT          |
                   IBV_QP_RETRY_CNT        |
                   IBV_QP_RNR_RETRY        |
                   IBV_QP_SQ_PSN           |
                   IBV_QP_MAX_QP_RD_ATOMIC ;
    if (ibv_modify_qp(rdma_dev->qp, &attr, attr_mask)) {
        fprintf(stderr, "Failed to modify QP to RTS\n");
        return 1;
    }
    
    return 0;
}

/****************************************************************************************
 * Memory allocation on CPU or GPU according to HAVE_CUDA pre-compile option and use_cuda flag
 * Return value: Allocated buffer pointer (if success), NULL (if error)
 ****************************************************************************************/
#ifdef HAVE_CUDA
void *work_buffer_alloc(size_t length, int use_cuda)
#else
void *work_buffer_alloc(size_t length)
#endif //HAVE_CUDA
{
    void    *buff;

#ifdef HAVE_CUDA
    if (use_cuda) {
        /* Mem allocation on GPU */
        buff = init_gpu(length);
        if (!buff) {
            fprintf(stderr, "Couldn't allocate work buffer on GPU.\n");
            return NULL;
        }
    } else
#endif //HAVE_CUDA
    {
        /* Mem allocation on CPU */
        int page_size = sysconf(_SC_PAGESIZE);
        buff = memalign(page_size, length);
        if (!buff) {
            fprintf(stderr, "Couldn't allocate work buffer on CPU.\n");
            return NULL;
        }
    }
    return buff;
}

/****************************************************************************************
 * CPU or GPU memory free, according to HAVE_CUDA pre-compile option and use_cuda flag
 ****************************************************************************************/
void work_buffer_free(void *buff, int use_cuda)
{
#ifdef HAVE_CUDA
    if (use_cuda) {
        free_gpu(buff);
    } else
#endif //HAVE_CUDA
    {
        DEBUG_LOG("free memory buffer(%p)\n", buff);
        free(buff);
    }
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

    enum ibv_access_flags   access_flags =  IBV_ACCESS_LOCAL_WRITE |
                                            IBV_ACCESS_REMOTE_WRITE;
    rdma_buff->mr = ibv_reg_mr(rdma_dev->pd, addr, length, access_flags);
    if (!rdma_buff->mr) {
        fprintf(stderr, "Couldn't register GPU MR\n");
        goto clean_rdma_buff;
    }
    DEBUG_LOG("ibv_reg_mr completed: buf %p, size = %lu, rkey = 0x%08x\n",
               addr, length, rdma_buff->mr->rkey);

    rdma_buff->buf_addr = addr;
    rdma_buff->buf_size = length;
    rdma_buff->rkey     = rdma_buff->mr->rkey;
    rdma_buff->rdma_dev = rdma_dev;
    rdma_dev->rdma_buff_cnt++;

    return rdma_buff;

clean_rdma_buff:
    if (rdma_dev->qp) {
        ibv_destroy_qp(rdma_dev->qp);
    }

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
            return 1;
        }
    }
    rdma_buff->rdma_dev->rdma_buff_cnt--;
    DEBUG_LOG("The buffer detached from rdma_device (%p). Number of attached to device buffers is %d.\n",
              rdma_buff->rdma_dev, rdma_buff->rdma_dev->rdma_buff_cnt);

    free(rdma_buff);
}

//============================================================================================
void wire_gid_to_gid(const char *wgid, union ibv_gid *gid)
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

void gid_to_wire_gid(const union ibv_gid *gid, char wgid[])
{
    int i;
    uint32_t *raw = (uint32_t *)gid->raw;

    for (i = 0; i < 4; ++i)
        sprintf(&wgid[i * 8], "%08x",
            htonl(raw[i]));
}

//============================================================================================
int rdma_buffer_get_desc_str(struct rdma_buffer *rdma_buff, char *desc_str, size_t desc_length)
{
    /*                        addr             size     rkey*/
    if (desc_length < sizeof "0102030405060708:01020304:01020304") {
        printf(stderr, "desc string size (%u) is less than required (%u) for sending rdma_buffer attributes\n",
               descr_length, sizeof "0102030405060708:01020304:01020304");
        return 1;
    }

    sprintf(desc_str, "%016llx:%08lx:%08x",
            (unsigned long long)rdma_buff->buf_addr,
            (unsigned long)rdma_buff->buf_size,
            rdma_buff->rkey);
    return 0;
}

//============================================================================================
int rdma_device_get_desc_str(struct rdma_device *rdma_dev, char *desc_str, size_t desc_length)
{
    /*                        lid  dctn   dc_key           g gid*/
    if (desc_length < sizeof "0102:010203:0102030405060708:1:0102030405060708090a0b0c0d0e0f10") {
        printf(stderr, "desc string size (%u) is less than required (%u) for sending rdma_device attributes\n",
               descr_length, sizeof "0102:010203:0102030405060708:1:0102030405060708090a0b0c0d0e0f10");
        return 1;
    }
    sprintf(desc_str, "%04x:%06x:%016llx:%d:",
            rdma_dev->lid,
            rdma_dev->dctn,
            (unsigned long long)rdma_dev->dc_key,
            rdma_dev->is_global & 0x1);

    gid_to_wire_gid(&rdma_dev->gid, desc_str + sizeof "0102:010203:0102030405060708:1")

    return 0;
}

//============================================================================================
int rdma_write_to_peer(struct rdma_device *rdma_dev, struct rdma_write_attr *attr)
{
    unsigned long long  rem_buf_addr = 0;
    unsigned long       rem_buf_size = 0;
    unsigned long       rem_buf_rkey = 0;
    uint16_t            rem_lid = 0;
    unsigned long       rem_dctn = 0; // QP number from DCT (client)
    int                 is_global = 0;
    union ibv_gid       rem_gid;
    uint64_t            rem_dc_key = 0;

    /* 1st small RDMA Write for DCI connect, this will create cqe->ts_start */
    DEBUG_LOG_FAST_PATH("1st small RDMA Write: ibv_wr_start: qpex = %p\n", rdma_dev->qpex);
    ibv_wr_start(rdma_dev->qpex);
    rdma_dev->qpex->wr_id = attr->wr_id | (1<<63);
    rdma_dev->qpex->wr_flags = IBV_SEND_SIGNALED;

    /*   addr             size     rkey
        "0102030405060708:01020304:01020304"*/
    sscanf(attr->remote_buf_desc_str, "%llx:%lx:%lx", &rem_buf_addr, &rem_buf_size, &rem_buf_rkey);

    DEBUG_LOG_FAST_PATH("1st small RDMA Write: ibv_wr_rdma_write: qpex = %p, rkey = 0x%x, remote buf 0x%llx\n",
                        attr->qpex, rem_buf_rkey, (unsigned long long)rem_buf_addr);
    ibv_wr_rdma_write(rdma_dev->qpex, rem_buf_rkey, rem_buf_addr);

    /*   lid  dctn   dc_key           g gid
        "0102:010203:0102030405060708:1:0102030405060708090a0b0c0d0e0f10"*/
    sscanf(attr->remote_dev_desc_str, "%x:%x:%llx:%d", &rem_lid, &rem_dctn, &rem_dc_key, &is_global);
    memset(&rem_gid, 0, sizeof rem_gid);
    if (is_global) {
        wire_gid_to_gid(attr->remote_dev_desc_str + sizeof "0102:010203:0102030405060708:1", &rem_gid);
    }
    DEBUG_LOG_FAST_PATH ("Rem GID: %02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x\n",
                         rem_gid.raw[0],  rem_gid.raw[1],  rem_gid.raw[2],  rem_gid.raw[3],
                         rem_gid.raw[4],  rem_gid.raw[5],  rem_gid.raw[6],  rem_gid.raw[7], 
                         rem_gid.raw[8],  rem_gid.raw[9],  rem_gid.raw[10], rem_gid.raw[11],
                         rem_gid.raw[12], rem_gid.raw[13], rem_gid.raw[14], rem_gid.raw[15] );

    /* Check if address handler (ah) is present in the hash, if not, create ah */
    // TODO...
    struct ibv_ah_attr  ah_attr;
    struct ibv_ah       *ah;
    
    memset(&ah_attr, 0, sizeof ah_attr);
    ah_attr.is_global   = is_global;
    ah_attr.dlid        = rem_lid;
    ah_attr.port_num    = rdma_dev->ib_port;

    if (ah_attr.is_global) {
        ah_attr.grh.hop_limit = 1;
        ah_attr.grh.dgid = rem_gid;
        ah_attr.grh.sgid_index = rdma_dev->gidx;
    }
    ah = ibv_create_ah(rdma_dev->pd, &ah_attr);
    if (!ah) {
        perror("ibv_create_ah");
        return 1;
    }
    
    DEBUG_LOG_FAST_PATH("1st small RDMA Write: mlx5dv_wr_set_dc_addr: mqpex = %p, ah = %p, rem_dctn = 0x%06x, rem_dc_key = %llx\n",
                        rdma_dev->mqpex, ah, rem_dctn, rem_dc_key);
    mlx5dv_wr_set_dc_addr(rdma_dev->mqpex, ah, rem_dctn, rem_dc_key);
    
    DEBUG_LOG_FAST_PATH("1st small RDMA Write: ibv_wr_set_sge: qpex = %p, lkey 0x%x, local buf 0x%llx, size = %u\n",
                        rdma_dev->qpex, attr->local_buf_rdma->mr->lkey, (unsigned long long)attr->local_buf_rdma->buf_addr, 1);
    ibv_wr_set_sge(rdma_dev->qpex, attr->local_buf_rdma->mr->lkey, (uintptr_t)attr->local_buf_rdma->buf_addr, 1);

    /* 2nd SIZE x RDMA Write, this will create cqe->ts_end */
    rdma_dev->qpex->wr_id = attr->wr_id;
    DEBUG_LOG_FAST_PATH("2nd SIZE x RDMA Write: ibv_wr_rdma_write: qpex = %p, rkey = 0x%x, remote buf 0x%llx\n",
                        rdma_dev->qpex, rem_buf_rkey, (unsigned long long)rem_buf_addr);
    ibv_wr_rdma_write(rdma_dev->qpex, rem_buf_rkey, rem_buf_addr);
    mlx5dv_wr_set_dc_addr(rdma_dev->mqpex, ah, rem_dctn, rem_dc_key);
    DEBUG_LOG_FAST_PATH("2nd SIZE x RDMA Write: ibv_wr_set_sge: qpex = %p, lkey 0x%x, local buf 0x%llx, size = %u\n",
                        rdma_dev->qpex, attr->local_buf_rdma->mr->lkey, (unsigned long long)attr->local_buf_rdma->buf_addr, attr->local_buf_rdma->buf_size);
    ibv_wr_set_sge(rdma_dev->qpex, attr->local_buf_rdma->mr->lkey, (uintptr_t)attr->local_buf_rdma->buf_addr, (uint32_t)attr->local_buf_rdma->buf_size);

    /* ring DB */
    DEBUG_LOG_FAST_PATH("ibv_wr_complete: qpex = %p\n", rdma_dev->qpex);
    return ibv_wr_complete(rdma_dev->qpex);
    // TODO: question: where should we use the following fields of struct rdma_write_attr:
    //    struct iovec       *local_buf_iovec;
    //    int                 local_buf_iovcnt; ???
}

//============================================================================================
int rdma_poll_completions(struct rdma_device            *rdma_dev,
                          struct rdma_completion_event  *event,
                          uint32_t                      num_entries)
{
    struct ibv_wc wc[100];
    int    reported_entries, i;

    // I don't want to malloc/free whole the run
    if (num_entries > 100){
        fprintf(stderr, "given num_entries %d for rdma_poll_completions exceed the max number %d\n",
                num_entries, 100);
        return 0;
    }

    // Polling completion queue
    DEBUG_LOG_FAST_PATH("Polling completion queue\n");
    do {
        DEBUG_LOG_FAST_PATH("Before ibv_poll_cq\n");
        reported_entries = ibv_poll_cq(rdma_dev->cq, 2, wc);
        if (reported_entries < 0) {
            fprintf(stderr, "poll CQ failed %d\n", reported_entries);
            return 0;
        }
    } while (reported_entries < 1);

    for (i = 0; i < reported_entries; ++i) {
        event[i].wr_id  = wc[i].wr_id;
        event[i].status = wc[i].status; // or (wc[i].status == IBV_WC_SUCCESS)? RDMA_STATUS_SUCCESS: RDMA_STATUS_ERR_LAST
    }
}

