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

static int debug = 1;
static int debug_fast_path = 1;
#define DEBUG_LOG if (debug) printf
#define DEBUG_LOG_FAST_PATH if (debug_fast_path) printf
#define FDEBUG_LOG if (debug) fprintf
#define FDEBUG_LOG_FAST_PATH if (debug_fast_path) fprintf

#define DC_KEY 0xffeeddcc

/* RDMA control buffer */
struct rdma_cb {

    struct ibv_context     *context;
    struct ibv_pd          *pd;
    struct ibv_mr          *gpu_mr;
    struct ibv_cq          *cq;
    struct ibv_srq         *srq; /* for DCT only */
    struct ibv_qp          *qp;
    void                   *gpu_buf;
    unsigned long           gpu_buf_size;
    int                     rx_depth;
    struct ibv_port_attr    portinfo;
    int                     sockfd;

    int                     use_cuda;

};

struct user_params {

    int                  port;
    int                  ib_port;
    unsigned long        size;
    char                *ib_devname;
    enum ibv_mtu         mtu;
    int                  rx_depth;
    int                  iters;
    int                  gidx;
    int                  use_cuda;
    char                *servername;
};

struct pingpong_dest {
    int             lid;
    int             qpn;
    int             psn;
    union ibv_gid   gid;
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

static int pp_init_gpu(struct rdma_cb *cb, size_t _size)
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

static int pp_free_gpu(struct rdma_cb *cb)
{
    int ret = 0;
    CUdeviceptr d_A = (CUdeviceptr) cb->gpu_buf;

    printf("deallocating RX GPU buffer\n");
    cuMemFree(d_A);
    d_A = 0;

    printf("destroying current CUDA Context\n");
    CUCHECK(cuCtxDestroy(cuContext));

    return ret;
}
#endif //HAVE_CUDA
//============================================================================================
static int pp_connect_ctx(struct rdma_cb *cb, struct user_params *usr_par,
                          int my_psn, struct pingpong_dest *dest)
{
    struct ibv_qp_attr attr = {
        .qp_state           = IBV_QPS_RTR,
        .path_mtu           = usr_par->mtu,
        .dest_qp_num        = dest->qpn, //for RC only
        .rq_psn             = dest->psn, //for RC only
        .max_dest_rd_atomic = 1,
        .min_rnr_timer      = 12, // or 16 ?
        .ah_attr            = {
            .is_global      = 0,
            .dlid           = dest->lid, // for RC only
            .sl             = 0,
            .src_path_bits  = 0,
            .port_num       = usr_par->ib_port
        }
    };
    enum ibv_qp_attr_mask attr_mask;

    if (dest->gid.global.interface_id) {
        attr.ah_attr.is_global = 1;
        attr.ah_attr.grh.hop_limit = 1;
        attr.ah_attr.grh.dgid = dest->gid; // for RC only
        attr.ah_attr.grh.sgid_index = usr_par->gidx;
    }
    attr_mask = IBV_QP_STATE              |
                IBV_QP_AV                 |
                IBV_QP_PATH_MTU           |
                IBV_QP_DEST_QPN           | //RC
                IBV_QP_RQ_PSN             | //RC
                IBV_QP_MAX_DEST_RD_ATOMIC | // ?
                IBV_QP_MIN_RNR_TIMER;

    if (ibv_modify_qp(cb->qp, &attr, attr_mask)) {
        fprintf(stderr, "Failed to modify QP to RTR\n");
        return 1;
    }

    // The next we don't need for DC (probably, for RC too)
//    attr.qp_state       = IBV_QPS_RTS;
//    attr.timeout        = 14;
//    attr.retry_cnt      = 7;
//    attr.rnr_retry      = 7;
//    attr.sq_psn     = my_psn;
//    attr.max_rd_atomic  = 1;
//    attr_mask = IBV_QP_STATE              |
//                IBV_QP_TIMEOUT            |
//                IBV_QP_RETRY_CNT          |
//                IBV_QP_RNR_RETRY          |
//                IBV_QP_SQ_PSN             |
//                IBV_QP_MAX_QP_RD_ATOMIC);
//    if (ibv_modify_qp(cb->qp, &attr, attr_mask)) {
//        fprintf(stderr, "Failed to modify QP to RTS\n");
//        return 1;
//    }

    return 0;
}

static struct pingpong_dest *pp_client_exch_dest(struct rdma_cb *cb,
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

static struct rdma_cb *pp_init_ctx(struct ibv_device *ib_dev,
                                   struct user_params *usr_par)
{
    struct rdma_cb *cb;
    int             ret;

    cb = calloc(1, sizeof *cb);
    if (!cb)
        return NULL;

    cb->gpu_buf_size = usr_par->size;
    cb->rx_depth     = usr_par->rx_depth;
    cb->sockfd       = -1;

#ifdef HAVE_CUDA
    cb->use_cuda = usr_par->use_cuda;
    if (cb->use_cuda) {
        if (pp_init_gpu(cb, cb->gpu_buf_size)) {
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

    DEBUG_LOG ("ibv_create_cq(%p, %d, NULL, NULL, 0)\n", cb->context, cb->rx_depth + 1);
    cb->cq = ibv_create_cq(cb->context, cb->rx_depth + 1, NULL, NULL, 0);
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
//        struct ibv_qp_init_attr attr = {
//            .send_cq = cb->cq,
//            .recv_cq = cb->cq,
//            .cap     = {
//                .max_send_wr  = 1,
//                .max_recv_wr  = cb->rx_depth,
//                .max_send_sge = 1,
//                .max_recv_sge = 1
//            },
//            .qp_type = IBV_QPT_RC,
//        };
        struct ibv_qp_init_attr_ex attr_ex;
        struct mlx5dv_qp_init_attr attr_dv;

        memset(&attr_ex, 0, sizeof(attr_ex));
        memset(&attr_dv, 0, sizeof(attr_dv));

        attr_ex.qp_type = IBV_QPT_RC; //IBV_QPT_DRIVER;
        attr_ex.send_cq = cb->cq;
        attr_ex.recv_cq = cb->cq;

        // For RC start
        attr_ex.cap.max_send_wr  = 1;
        attr_ex.cap.max_recv_wr  = 0; /* SRQ is used */ //cb->rx_depth;
        attr_ex.cap.max_send_sge = 1;
        attr_ex.cap.max_recv_sge = 1;
        // For RC end //////

        attr_ex.comp_mask |= IBV_QP_INIT_ATTR_PD;
        attr_ex.pd = cb->pd;
        attr_ex.srq = cb->srq; /* Should use SRQ for client only (DCT) */
        
//        /* create DCT */
//        attr_dv.comp_mask |= MLX5DV_QP_INIT_ATTR_MASK_DC;
//        attr_dv.dc_init_attr.dc_type = MLX5DV_DCTYPE_DCT;
//        attr_dv.dc_init_attr.dct_access_key = DC_KEY;

        //DEBUG_LOG ("ibv_create_qp(%p,%p)\n", cb->pd, &attr);
        //cb->qp = ibv_create_qp(cb->pd, &attr); //TO-DC-1
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
        pp_free_gpu(cb);
    } else
#endif //HAVE_CUDA
    {
        free(cb->gpu_buf);
    }

clean_ctx:
    free(cb);

    return NULL;
}

int pp_close_ctx(struct rdma_cb *cb)
{
    if (ibv_destroy_qp(cb->qp)) {
        fprintf(stderr, "Couldn't destroy QP\n");
        return 1;
    }

    if (ibv_destroy_cq(cb->cq)) {
        fprintf(stderr, "Couldn't destroy CQ\n");
        return 1;
    }

	if (cb->srq) {
        if (ibv_destroy_srq(cb->srq)) {
            fprintf(stderr, "Couldn't destroy CQ\n");
            return 1;
        }
    }
    
    if (ibv_dereg_mr(cb->gpu_mr)) {
        fprintf(stderr, "Couldn't deregister GPU MR\n");
        return 1;
    }
    
    if (ibv_dealloc_pd(cb->pd)) {
        fprintf(stderr, "Couldn't deallocate PD\n");
        return 1;
    }

    if (ibv_close_device(cb->context)) {
        fprintf(stderr, "Couldn't release context\n");
        return 1;
    }

    if (cb->sockfd != -1)
        close(cb->sockfd);
    
#ifdef HAVE_CUDA
    if (cb->use_cuda) {
        pp_free_gpu(cb);
    } else
#endif //HAVE_CUDA
    {
        free(cb->gpu_buf);
    }

    free(cb);

    return 0;
}

static int pp_post_recv(struct rdma_cb *cb, int n)//
{                                                 //
    struct ibv_sge list = {                       //
        .addr   = (uintptr_t) cb->gpu_buf,        //
        .length = cb->gpu_buf_size,               //
        .lkey   = cb->gpu_mr->lkey                //
    };                                            //
    struct ibv_recv_wr wr = {                     //
        .wr_id      = 0 /*RECV_WRID*/,            //
        .sg_list    = &list,                      //
        .num_sge    = 1,                          //
        .next       = NULL                        //
    };                                            //
    struct ibv_recv_wr *bad_wr;                   //
    int i;                                        //
                                                  //
    for (i = 0; i < n; ++i)                       //
        if (ibv_post_recv(cb->qp, &wr, &bad_wr))  //
            break;                                //
                                                  //
    return i;                                     //
}                                                 //

static void usage(const char *argv0)
{
    printf("Usage:\n");
    printf("  %s <host>     connect to server at <host>\n", argv0);
    printf("\n");
    printf("Options:\n");
    printf("  -p, --port=<port>         listen on/connect to port <port> (default 18515)\n");
    printf("  -d, --ib-dev=<dev>        use IB device <dev> (default first device found)\n");
    printf("  -i, --ib-port=<port>      use port <port> of IB device (default 1)\n");
    printf("  -s, --size=<size>         size of message to exchange (default 4096)\n");
    printf("  -m, --mtu=<size>          path MTU (default 1024)\n");
    printf("  -r, --rx-depth=<dep>      number of receives to post at a time (default 500)\n");
    printf("  -n, --iters=<iters>       number of exchanges (default 1000)\n");
    printf("  -g, --gid-idx=<gid index> local port gid index\n");
    printf("  -u, --use-cuda            use CUDA pacage (work with GPU memoty)\n");
}

static int parse_command_line(int argc, char *argv[], struct user_params *usr_par)
{
    memset(usr_par, 0, sizeof *usr_par);
    /*Set defaults*/
    usr_par->port       = 18515;
    usr_par->ib_port    = 1;
    usr_par->size       = 4096;
    usr_par->mtu        = IBV_MTU_1024;
    usr_par->rx_depth   = 500;
    usr_par->iters      = 1000;
    usr_par->gidx       = -1;

    while (1) {
        int c;

        static struct option long_options[] = {
            { .name = "port",          .has_arg = 1, .val = 'p' },
            { .name = "ib-dev",        .has_arg = 1, .val = 'd' },
            { .name = "ib-port",       .has_arg = 1, .val = 'i' },
            { .name = "size",          .has_arg = 1, .val = 's' },
            { .name = "mtu",           .has_arg = 1, .val = 'm' },
            { .name = "rx-depth",      .has_arg = 1, .val = 'r' },
            { .name = "iters",         .has_arg = 1, .val = 'n' },
            { .name = "gid-idx",       .has_arg = 1, .val = 'g' },
            { .name = "use-cuda",      .has_arg = 0, .val = 'u' },
            { 0 }
        };

        c = getopt_long(argc, argv, "p:d:i:s:m:r:n:g:u",
                        long_options, NULL);
        if (c == -1)
            break;

        switch (c) {
        
        case 'p':
            usr_par->port = strtol(optarg, NULL, 0);
            if (usr_par->port < 0 || usr_par->port > 65535) {
                usage(argv[0]);
                return 1;
            }
            break;

        case 'd':
            //usr_par->ib_devname = strdupa(optarg);
            usr_par->ib_devname = calloc(1, strlen(optarg)+1);
            if (!usr_par->ib_devname){
                perror("ib_devname mem alloc failure");
                return 1;
            }
            strcpy(usr_par->ib_devname, optarg);
            break;

        case 'i':
            usr_par->ib_port = strtol(optarg, NULL, 0);
            if (usr_par->ib_port < 0) {
                usage(argv[0]);
                return 1;
            }
            break;

        case 's':
            usr_par->size = strtol(optarg, NULL, 0);
            break;

        case 'm':
            usr_par->mtu = pp_mtu_to_enum(strtol(optarg, NULL, 0));
            if (usr_par->mtu < 0) {
                usage(argv[0]);
                return 1;
            }
            break;

        case 'r':
            usr_par->rx_depth = strtol(optarg, NULL, 0);
            break;

        case 'n':
            usr_par->iters = strtol(optarg, NULL, 0);
            break;

        case 'g':
            usr_par->gidx = strtol(optarg, NULL, 0);
            break;

        case 'u':
            usr_par->use_cuda = 1;
            break;
        
        default:
            usage(argv[0]);
            return 1;
        }
    }

    if (optind == argc - 1) {
        //usr_par->servername = strdupa(argv[optind]);
        usr_par->servername = calloc(1, strlen(argv[optind])+1);
        if (!usr_par->servername){
            perror("servername mem alloc failure");
            return 1;
        }
        strcpy(usr_par->servername, argv[optind]);
    }
    else if (optind < argc) {
        usage(argv[0]);
        return 1;
    }

    return 0;
}

int main(int argc, char *argv[])
{
    struct ibv_device     **dev_list;
    struct ibv_device      *ib_dev;
    struct rdma_cb         *cb;
    struct pingpong_dest    my_dest;
    struct pingpong_dest   *rem_dest;
    struct timeval          start, end;
    int                     routs;
    int                     cnt;
    char                    gid[INET6_ADDRSTRLEN];
    struct user_params      usr_par;
    int                     ret;

    srand48(getpid() * time(NULL));

    ret = parse_command_line(argc, argv, &usr_par);
    if (ret) {
        return ret;
    }
    
    /****************************************************************************************************
     * In the next block we are checking if given IB device name matches one of devices in the list.
     * If the name is not given, we take the first available IB device, else the matching to the name one.
     * The result of this block is ig_dev - initialized pointer to the relevant struct ibv_device
     */
    dev_list = ibv_get_device_list(NULL);
    if (!dev_list) {
        perror("Failed to get IB devices list");
        return 1;
    }

    if (!usr_par.ib_devname) /*if ib device name is not given by comand line*/{
        DEBUG_LOG ("Device name is not given by command line, taking the first available from device list\n");
        ib_dev = *dev_list;
        if (!ib_dev) {
            fprintf(stderr, "No IB devices found\n");
            return 1;
        }
    } else {
        int i;
        DEBUG_LOG ("Command line Device name \"%s\"\n", usr_par.ib_devname);
        for (i = 0; dev_list[i]; ++i) {
            char *dev_name = (char*)ibv_get_device_name(dev_list[i]);
            DEBUG_LOG ("Device %d name \"%s\"\n", i, dev_name);
            if (!strcmp(dev_name, usr_par.ib_devname))
                break;
        }
        ib_dev = dev_list[i];
        if (!ib_dev) {
            fprintf(stderr, "IB device %s not found\n", usr_par.ib_devname);
            free(usr_par.ib_devname);
            return 1;
        }
    }
    /****************************************************************************************************/
    
    cb = pp_init_ctx(ib_dev, &usr_par);
    if (!cb)
        return 1;

    //routs = pp_post_recv(cb, cb->rx_depth);                   
    //if (routs < cb->rx_depth) {                                
    //    fprintf(stderr, "Couldn't post receive (%d)\n", routs); 
    //    return 1;                                               
    //}                                                           
    
    if (pp_get_port_info(cb->context, usr_par.ib_port, &cb->portinfo)) {
        fprintf(stderr, "Couldn't get port info\n");
        return 1;
    }

    my_dest.lid = cb->portinfo.lid;
    if (cb->portinfo.link_layer != IBV_LINK_LAYER_ETHERNET &&
                            !my_dest.lid) {
        fprintf(stderr, "Couldn't get local LID\n");
        return 1;
    }

    if (usr_par.gidx >= 0) {
        if (ibv_query_gid(cb->context, usr_par.ib_port, usr_par.gidx, &my_dest.gid)) {
            fprintf(stderr, "can't read sgid of index %d\n", usr_par.gidx);
            return 1;
        }
        DEBUG_LOG ("My GID: %02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x\n",
               my_dest.gid.raw[0], my_dest.gid.raw[1], my_dest.gid.raw[2], my_dest.gid.raw[3],
               my_dest.gid.raw[4], my_dest.gid.raw[5], my_dest.gid.raw[6], my_dest.gid.raw[7], 
               my_dest.gid.raw[8], my_dest.gid.raw[9], my_dest.gid.raw[10], my_dest.gid.raw[11],
               my_dest.gid.raw[12], my_dest.gid.raw[13], my_dest.gid.raw[14], my_dest.gid.raw[15] );
    } else {
        memset(&my_dest.gid, 0, sizeof my_dest.gid);
    }

    my_dest.qpn = cb->qp->qp_num;
    my_dest.psn = lrand48() & 0xffffff;
    inet_ntop(AF_INET6, &my_dest.gid, gid, sizeof gid);
    printf("  local address:  LID 0x%04x, QPN 0x%06x, PSN 0x%06x, GID %s\n",
           my_dest.lid, my_dest.qpn, my_dest.psn, gid);


    DEBUG_LOG ("Remote server name \"%s\"\n", usr_par.servername);
    rem_dest = pp_client_exch_dest(cb, usr_par.servername, usr_par.port, &my_dest);
    free(usr_par.servername);

    if (!rem_dest)
        return 1;

    inet_ntop(AF_INET6, &rem_dest->gid, gid, sizeof gid);
    printf("  remote address: LID 0x%04x, QPN 0x%06x, PSN 0x%06x, GID %s\n",
           rem_dest->lid, rem_dest->qpn, rem_dest->psn, gid);

    if (pp_connect_ctx(cb, &usr_par, my_dest.psn, rem_dest))
        return 1;


    if (gettimeofday(&start, NULL)) {
        perror("gettimeofday");
        return 1;
    }

    /****************************************************************************************************
     * The main loop where client and server send and receive "iters" number of messages
     */
    for (cnt = 0; cnt < usr_par.iters; cnt++) {

        char sendmsg[sizeof "0102030405060708:01020304:01020304:01020304"];
        char ackmsg[sizeof "rdma_write completed"];

        // Sending RDMA data (address and rkey) by socket as a triger to start RDMA write operation
        sprintf(sendmsg, "%016llx:%08lx:%08x:%08x", (unsigned long long)cb->gpu_buf, cb->gpu_buf_size, cb->gpu_mr->rkey, cb->qp->qp_num);
        DEBUG_LOG_FAST_PATH("Send message N %d: \"%s\" to the server\n", cnt, sendmsg);
        if (write(cb->sockfd, sendmsg, sizeof sendmsg) != sizeof sendmsg) {
            fprintf(stderr, "Couldn't send RDMA data for iteration\n");
            return 1;
        }
        
        // Wating for confirmation message from the socket that rdma_write from the server has beed completed
        if (recv(cb->sockfd, ackmsg, sizeof(ackmsg), MSG_WAITALL) != sizeof(ackmsg)) {
            perror("client read");
            fprintf(stderr, "Couldn't read \"rdma_write completed\" message\n");
            return 1;
        }

        // Printing received data for debug purpose
        DEBUG_LOG_FAST_PATH("Received ack N %d: \"%s\"\n", cnt, ackmsg);
#ifdef HAVE_CUDA
        if (!cb->use_cuda) {
            DEBUG_LOG_FAST_PATH("Written data \"%s\"\n", (char*)cb->gpu_buf);
        }
#else
    DEBUG_LOG_FAST_PATH("Written data \"%s\"\n", (char*)cb->gpu_buf);
#endif //HAVE_CUDA
    }
    /****************************************************************************************************/

    if (gettimeofday(&end, NULL)) {
        perror("gettimeofday");
        return 1;
    }

    {
        float usec = (end.tv_sec - start.tv_sec) * 1000000 +
            (end.tv_usec - start.tv_usec);
        long long bytes = (long long) usr_par.size * usr_par.iters * 2;

        printf("%lld bytes in %.2f seconds = %.2f Mbit/sec\n",
               bytes, usec / 1000000., bytes * 8. / usec);
        printf("%d iters in %.2f seconds = %.2f usec/iter\n",
               usr_par.iters, usec / 1000000., usec / usr_par.iters);
    }

    if (pp_close_ctx(cb))
        return 1;

    ibv_free_device_list(dev_list);
    free(rem_dest);

    return 0;
}
