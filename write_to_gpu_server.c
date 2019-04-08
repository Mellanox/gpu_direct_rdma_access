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

#include "write_to_gpu.h"

static int debug = 1;
static int debug_fast_path = 1;
#define DEBUG_LOG if (debug) printf
#define DEBUG_LOG_FAST_PATH if (debug_fast_path) printf
#define FDEBUG_LOG if (debug) fprintf
#define FDEBUG_LOG_FAST_PATH if (debug_fast_path) fprintf

#define PING_SQ_DEPTH 64
//#define DC_KEY 0xffeeddcc - server doesn't know DC_KEY, it obtains this from the client by socket

enum {
    SMALL_WRITE_WRID = 1,
    BUFF_WRITE_WRID  = 2,
};

struct rdma_cb {
    struct ibv_context      *context;
    struct ibv_comp_channel *channel;
    struct ibv_pd           *pd;
    struct ibv_mr           *mr;
    struct ibv_cq           *cq;
    struct ibv_qp           *qp;
    struct ibv_qp_ex    *qpex;  /* server only */
    struct mlx5dv_qp_ex *mqpex; /* server only */
    struct ibv_ah       *ah;    /* server only */
    void                    *buf;
    unsigned long           size; // changed from "unsigned long long"
    int                     rx_depth;
    struct ibv_port_attr    portinfo;
    int                     sockfd;
    unsigned long long      gpu_buf_addr;
    unsigned long           gpu_buf_size;
    unsigned long           gpu_buf_rkey;
    unsigned long           rem_dctn; // QP number from client
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
    int                  use_event;
};

struct pingpong_dest {
    int lid;
    int qpn;
    int psn;
    union ibv_gid gid;
};

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

//    cb->ah = ibv_create_ah(cb->pd, &attr.ah_attr); //DC only
//    if (!cb->ah) {
//        perror("ibv_create_ah");
//        ret = errno;
//        return ret;
//    }
//    DEBUG_LOG("created ah (%p)\n", cb->ah);
    if (ibv_modify_qp(cb->qp, &attr, attr_mask)) {
        fprintf(stderr, "Failed to modify QP to RTR\n");
        return 1;
    }

    attr.qp_state       = IBV_QPS_RTS;
    attr.timeout        = 14; //or 16 ?
    attr.retry_cnt      = 7;
    attr.rnr_retry      = 7;
    attr.sq_psn         = my_psn;
    attr.max_rd_atomic  = 1;
    attr_mask = IBV_QP_STATE            |
                IBV_QP_TIMEOUT          |
                IBV_QP_RETRY_CNT        |
                IBV_QP_RNR_RETRY        |
                IBV_QP_SQ_PSN           |
                IBV_QP_MAX_QP_RD_ATOMIC ;
    if (ibv_modify_qp(cb->qp, &attr, attr_mask)) {
        fprintf(stderr, "Failed to modify QP to RTS\n");
        return 1;
    }

    return 0;
}

static struct pingpong_dest *pp_server_exch_dest(struct rdma_cb *cb, struct user_params *usr_par,
                                                 int port, const struct pingpong_dest *my_dest)
{
    struct addrinfo *res, *t;
    struct addrinfo hints = {
        .ai_flags    = AI_PASSIVE,
        .ai_family   = AF_UNSPEC,
        .ai_socktype = SOCK_STREAM
    };
    char *service;
    char msg[sizeof "0000:000000:000000:00000000000000000000000000000000"];
    int  rc, r_size;
    int  tmp_sockfd = -1;
    struct pingpong_dest *rem_dest = NULL;
    char gid[33];

    if (asprintf(&service, "%d", port) < 0)
        return NULL;

    rc = getaddrinfo(NULL, service, &hints, &res);

    if (rc < 0) {
        fprintf(stderr, "%s for port %d\n", gai_strerror(rc), port);
        free(service);
        return NULL;
    }

    for (t = res; t; t = t->ai_next) {
        tmp_sockfd = socket(t->ai_family, t->ai_socktype, t->ai_protocol);
        if (tmp_sockfd >= 0) {
            int optval = 1;

            setsockopt(tmp_sockfd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof optval);

            if (!bind(tmp_sockfd, t->ai_addr, t->ai_addrlen))
                break;
            close(tmp_sockfd);
            tmp_sockfd = -1;
        }
    }

    freeaddrinfo(res);
    free(service);

    if (tmp_sockfd < 0) {
        fprintf(stderr, "Couldn't listen to port %d\n", port);
        return NULL;
    }

    listen(tmp_sockfd, 1);
    cb->sockfd = accept(tmp_sockfd, NULL, 0);
    close(tmp_sockfd);
    if (cb->sockfd < 0) {
        fprintf(stderr, "accept() failed\n");
        return NULL;
    } 

    r_size = recv(cb->sockfd, msg, sizeof(msg), MSG_WAITALL);
    if (r_size != sizeof msg) {
        perror("server read");
        fprintf(stderr, "%d/%d: Couldn't read remote address\n", r_size, (int) sizeof msg);
        return NULL;
    }
    DEBUG_LOG ("exch_dest: after receive \"%s\"\n", msg);

    rem_dest = malloc(sizeof *rem_dest);
    if (!rem_dest) {
        return NULL;
    }

    sscanf(msg, "%x:%x:%x:%s", &rem_dest->lid, &rem_dest->qpn, &rem_dest->psn, gid);
    wire_gid_to_gid(gid, &rem_dest->gid);
    DEBUG_LOG ("Rem GID: %02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x\n",
           rem_dest->gid.raw[0], rem_dest->gid.raw[1], rem_dest->gid.raw[2], rem_dest->gid.raw[3],
           rem_dest->gid.raw[4], rem_dest->gid.raw[5], rem_dest->gid.raw[6], rem_dest->gid.raw[7], 
           rem_dest->gid.raw[8], rem_dest->gid.raw[9], rem_dest->gid.raw[10], rem_dest->gid.raw[11],
           rem_dest->gid.raw[12], rem_dest->gid.raw[13], rem_dest->gid.raw[14], rem_dest->gid.raw[15] );

    if (pp_connect_ctx(cb, usr_par, my_dest->psn, rem_dest)) {
        fprintf(stderr, "Couldn't connect to remote QP\n");
        free(rem_dest);
        return NULL;
    }

    gid_to_wire_gid(&my_dest->gid, gid);
    sprintf(msg, "%04x:%06x:%06x:%s", my_dest->lid, my_dest->qpn, my_dest->psn, gid);
    DEBUG_LOG ("exch_dest:  before send  \"%s\"\n", msg);
    if (write(cb->sockfd, msg, sizeof msg) != sizeof msg) {
        fprintf(stderr, "Couldn't send local address\n");
        free(rem_dest);
        return NULL;
    }

    return rem_dest;
}

static struct rdma_cb *pp_init_ctx(struct ibv_device *ib_dev,
                                   struct user_params *usr_par)
{
    struct rdma_cb *cb;
    int    ret;
    int    page_size;

    cb = calloc(1, sizeof *cb);
    if (!cb)
        return NULL;


    cb->size     = usr_par->size;
    cb->rx_depth = usr_par->rx_depth;
    cb->sockfd   = -1;

    page_size = sysconf(_SC_PAGESIZE);
    cb->buf = memalign(page_size, cb->size);
    if (!cb->buf) {
        fprintf(stderr, "Couldn't allocate work buf.\n");
        goto clean_ctx;
    }

    cb->context = ibv_open_device(ib_dev);
    if (!cb->context) {
        fprintf(stderr, "Couldn't get context for %s\n",
            ibv_get_device_name(ib_dev));
        goto clean_buffer;
    }

    if (usr_par->use_event) {
        cb->channel = ibv_create_comp_channel(cb->context);
        if (!cb->channel) {
            fprintf(stderr, "Couldn't create completion channel\n");
            goto clean_device;
        }
    } else
        cb->channel = NULL;

    cb->pd = ibv_alloc_pd(cb->context);
    if (!cb->pd) {
        fprintf(stderr, "Couldn't allocate PD\n");
        goto clean_comp_channel;
    }

    cb->mr = ibv_reg_mr(cb->pd, cb->buf, cb->size,
                         //IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ );
                         IBV_ACCESS_LOCAL_WRITE);

    if (!cb->mr) {
        fprintf(stderr, "Couldn't register MR\n");
        goto clean_pd;
    }
    
    /* FIXME memset(cb->buf, 0, cb->size); */
    memset(cb->buf, 0x7b, cb->size);

    DEBUG_LOG ("ibv_create_cq(%p, %d, NULL, %p, 0)\n", cb->context, usr_par->rx_depth + 1, cb->channel);
    cb->cq = ibv_create_cq(cb->context, usr_par->rx_depth + 1, NULL, cb->channel, 0);
    if (!cb->cq) {
        fprintf(stderr, "Couldn't create CQ\n");
        goto clean_mr;
    }

    {
//        struct ibv_qp_init_attr attr = {
//            .send_cq = cb->cq,
//            .recv_cq = cb->cq,
//            .cap     = {
//                .max_send_wr  = 1,
//                .max_recv_wr  = rx_depth,
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
        attr_ex.cap.max_send_wr  = 10;
        attr_ex.cap.max_recv_wr  = usr_par->rx_depth;
        attr_ex.cap.max_send_sge = 1;
        attr_ex.cap.max_recv_sge = 1;
        // For RC end //////

        attr_ex.comp_mask |= IBV_QP_INIT_ATTR_PD;
        attr_ex.pd = cb->pd;
        attr_ex.srq = NULL; /* Should use SRQ for server only (for DCT) cb->srq; */
        
//        /* create DCI */
//        attr_dv.comp_mask |= MLX5DV_QP_INIT_ATTR_MASK_DC;
//        attr_dv.dc_init_attr.dc_type = MLX5DV_DCTYPE_DCI;
//
     //   attr_ex.cap.max_send_wr = PING_SQ_DEPTH;
     //   attr_ex.cap.max_send_sge = 1;

        attr_ex.comp_mask |= IBV_QP_INIT_ATTR_SEND_OPS_FLAGS;
        attr_ex.send_ops_flags = IBV_QP_EX_WITH_RDMA_WRITE/* | IBV_QP_EX_WITH_RDMA_READ*/;

        attr_dv.comp_mask |= MLX5DV_QP_INIT_ATTR_MASK_QP_CREATE_FLAGS;
//        attr_dv.create_flags |= MLX5DV_QP_CREATE_DISABLE_SCATTER_TO_CQE; /*driver doesnt support scatter2cqe data-path on DCI yet*/

        //DEBUG_LOG ("ibv_create_qp(%p,%p)\n", cb->pd, &attr);
        //cb->qp = ibv_create_qp(cb->pd, &attr); //todo mlx5dv_create_qp(cb->cm_id->verbs, &attr_ex, &attr_dv);
        DEBUG_LOG ("mlx5dv_create_qp(%p,%p,%p)\n", cb->context, &attr_ex, &attr_dv);
        cb->qp = mlx5dv_create_qp(cb->context, &attr_ex, &attr_dv);
        if (!cb->qp)  {
            fprintf(stderr, "Couldn't create QP\n");
            goto clean_cq;
        }
        cb->qpex = ibv_qp_to_qp_ex(cb->qp);
        if (!cb->qpex)  {
            fprintf(stderr, "Couldn't create QPEX\n");
            goto clean_qp;
        }
        cb->mqpex = mlx5dv_qp_ex_from_ibv_qp_ex(cb->qpex);
        if (!cb->mqpex)  {
            fprintf(stderr, "Couldn't create MQPEX\n");
            goto clean_qp;
        }
        
    }

    {
        struct ibv_qp_attr attr = {
            .qp_state        = IBV_QPS_INIT,
            .pkey_index      = 0,
            .port_num        = usr_par->ib_port,
            //.qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ
            .qp_access_flags = IBV_ACCESS_LOCAL_WRITE
        };

        if (ibv_modify_qp(cb->qp, &attr,
                          IBV_QP_STATE      |
                          IBV_QP_PKEY_INDEX |
                          IBV_QP_PORT       |
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

clean_mr:
    ibv_dereg_mr(cb->mr);

clean_pd:
    ibv_dealloc_pd(cb->pd);

clean_comp_channel:
    if (cb->channel)
        ibv_destroy_comp_channel(cb->channel);

clean_device:
    ibv_close_device(cb->context);

clean_buffer:
    free(cb->buf);

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

    if (ibv_dereg_mr(cb->mr)) {
        fprintf(stderr, "Couldn't deregister MR\n");
        return 1;
    }

    if (ibv_dealloc_pd(cb->pd)) {
        fprintf(stderr, "Couldn't deallocate PD\n");
        return 1;
    }

    if (cb->channel) {
        if (ibv_destroy_comp_channel(cb->channel)) {
            fprintf(stderr, "Couldn't destroy completion channel\n");
            return 1;
        }
    }

    if (ibv_close_device(cb->context)) {
        fprintf(stderr, "Couldn't release context\n");
        return 1;
    }

    free(cb->buf);

    if (cb->sockfd != -1)
        close(cb->sockfd);
    
    free(cb);

    return 0;
}

#define mmin(a, b) a < b ? a : b

static int pp_post_recv(struct rdma_cb *cb, int n)
{
    struct ibv_sge list = {
        .addr   = (uintptr_t) cb->buf,
        .length = cb->size,
        .lkey   = cb->mr->lkey
    };
    struct ibv_recv_wr wr = {
        .wr_id      = 0 /*RECV_WRID*/,
        .sg_list    = &list,
        .num_sge    = 1,
        .next       = NULL
    };
    struct ibv_recv_wr *bad_wr;
    int i;

    for (i = 0; i < n; ++i)
        if (ibv_post_recv(cb->qp, &wr, &bad_wr))
            break;

    return i;
}

static int pp_post_send(struct rdma_cb *cb)
{
//    struct ibv_sge list = {                      
//        .addr   = (uintptr_t) cb->buf,           
//        .length = cb->size,                      
//        .lkey   = cb->mr->lkey                   
//    };                                           
//    struct ibv_send_wr wr = {                    
//        .wr_id      = BUFF_WRITE_WRID,        
//        .sg_list    = &list,                     
//        .num_sge    = 1,                         
//        .opcode     = IBV_WR_RDMA_WRITE,         
//        .send_flags = IBV_SEND_SIGNALED,         
//        .wr.rdma.remote_addr = cb->gpu_buf_addr, 
//        .wr.rdma.rkey        = cb->gpu_buf_rkey, 
//        .next       = NULL                       
//    };                                           
//    struct ibv_send_wr *bad_wr;                  


//    /* 1st small RDMA Write for DCI connect, this will create cqe->ts_start */
    DEBUG_LOG_FAST_PATH("1st small RDMA Write: ibv_wr_start: qpex = %p\n", cb->qpex);
    ibv_wr_start(cb->qpex);
    cb->qpex->wr_id = SMALL_WRITE_WRID;
    cb->qpex->wr_flags = IBV_SEND_SIGNALED;
    DEBUG_LOG_FAST_PATH("1st small RDMA Write: ibv_wr_rdma_write: qpex = %p, rkey = 0x%x, remote buf 0x%llx\n",
                        cb->qpex, cb->gpu_buf_rkey, (unsigned long long)cb->gpu_buf_addr);
    ibv_wr_rdma_write(cb->qpex, cb->gpu_buf_rkey, cb->gpu_buf_addr);
//    mlx5dv_wr_set_dc_addr(cb->mqpex, cb->ah, cb->rem_dctn, DC_KEY);
    DEBUG_LOG_FAST_PATH("1st small RDMA Write: ibv_wr_set_sge: qpex = %p, lkey 0x%x, local buf 0x%llx, size = %u\n",
                        cb->qpex, cb->mr->lkey, (unsigned long long)cb->buf, 1);
    ibv_wr_set_sge(cb->qpex, cb->mr->lkey, (uintptr_t)cb->buf, 1);

    /* 2nd SIZE x RDMA Write, this will create cqe->ts_end */
    cb->qpex->wr_id = BUFF_WRITE_WRID;
    DEBUG_LOG_FAST_PATH("2nd SIZE x RDMA Write: ibv_wr_rdma_write: qpex = %p, rkey = 0x%x, remote buf 0x%llx\n",
                        cb->qpex, cb->gpu_buf_rkey, (unsigned long long)cb->gpu_buf_addr);
    ibv_wr_rdma_write(cb->qpex, cb->gpu_buf_rkey, cb->gpu_buf_addr);
//    mlx5dv_wr_set_dc_addr(cb->mqpex, cb->ah, cb->rem_dctn, DC_KEY);
    DEBUG_LOG_FAST_PATH("2nd SIZE x RDMA Write: ibv_wr_set_sge: qpex = %p, lkey 0x%x, local buf 0x%llx, size = %u\n",
                        cb->qpex, cb->mr->lkey, (unsigned long long)cb->buf, cb->size);
    ibv_wr_set_sge(cb->qpex, cb->mr->lkey, (uintptr_t)cb->buf, (uint32_t)cb->size);
//
    /* ring DB */
    DEBUG_LOG_FAST_PATH("ibv_wr_complete: qpex = %p\n", cb->qpex);
    return ibv_wr_complete(cb->qpex);

//    DEBUG_LOG_FAST_PATH("ibv_post_send IBV_WR_RDMA_WRITE: local buf 0x%llx, lkey 0x%x, remote buf 0x%llx, rkey = 0x%x, size = %u\n",
//                        (unsigned long long)wr.sg_list[0].addr, wr.sg_list[0].lkey,                                                 
//                        (unsigned long long)wr.wr.rdma.remote_addr, wr.wr.rdma.rkey, wr.sg_list[0].length);                         
//    return ibv_post_send(cb->qp, &wr, &bad_wr);                                                                                     
}

static void usage(const char *argv0)
{
    printf("Usage:\n");
    printf("  %s            start a server and wait for connection\n", argv0);
    printf("\n");
    printf("Options:\n");
    printf("  -p, --port=<port>         listen on/connect to port <port> (default 18515)\n");
    printf("  -d, --ib-dev=<dev>        use IB device <dev> (default first device found)\n");
    printf("  -i, --ib-port=<port>      use port <port> of IB device (default 1)\n");
    printf("  -s, --size=<size>         size of message to exchange (default 4096)\n");
    printf("  -m, --mtu=<size>          path MTU (default 1024)\n");
    printf("  -r, --rx-depth=<dep>      number of receives to post at a time (default 500)\n");
    printf("  -n, --iters=<iters>       number of exchanges (default 1000)\n");
    printf("  -e, --events              sleep on CQ events (default poll)\n");
    printf("  -g, --gid-idx=<gid index> local port gid index\n");
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
            { .name = "events",        .has_arg = 0, .val = 'e' },
            { .name = "gid-idx",       .has_arg = 1, .val = 'g' },
            { 0 }
        };

        c = getopt_long(argc, argv, "p:d:i:s:m:r:n:eg:",
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

        case 'e':
            usr_par->use_event = 1;
            break;

        default:
            usage(argv[0]);
            return 1;
        }
    }

    if (optind < argc) {
        usage(argv[0]);
        return 1;
    }

    return 0;
}

static int wait_for_completion_event(struct rdma_cb *cb, int *num_cq_events)
{
    struct ibv_cq *ev_cq;
    void          *ev_ctx;

    DEBUG_LOG_FAST_PATH("Waiting for completion event\n");
    if (ibv_get_cq_event(cb->channel, &ev_cq, &ev_ctx)) {
        fprintf(stderr, "Failed to get cq_event\n");
        return 1;
    }

    (*num_cq_events)++;

    if (ev_cq != cb->cq) {
        fprintf(stderr, "CQ event for unknown CQ %p\n", ev_cq);
        return 1;
    }

    if (ibv_req_notify_cq(cb->cq, 0)) {
        fprintf(stderr, "Couldn't request CQ notification\n");
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
    char                   *ib_devname = NULL;
    //int                     routs;
    int                     scnt, pre_scnt;
    int                     num_cq_events = 0;
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

    if (!ib_devname) /*if ib device name is not given by comand line*/{
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
            fprintf(stderr, "IB device %s not found\n", ib_devname);
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

    if (usr_par.use_event)
        if (ibv_req_notify_cq(cb->cq, 0)) {
            fprintf(stderr, "Couldn't request CQ notification\n");
            return 1;
        }


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
    } else
        memset(&my_dest.gid, 0, sizeof my_dest.gid);

    my_dest.qpn = cb->qp->qp_num;
    my_dest.psn = lrand48() & 0xffffff;
    inet_ntop(AF_INET6, &my_dest.gid, gid, sizeof gid);
    printf("  local address:  LID 0x%04x, QPN 0x%06x, PSN 0x%06x, GID %s\n",
           my_dest.lid, my_dest.qpn, my_dest.psn, gid);


    rem_dest = pp_server_exch_dest(cb, &usr_par, usr_par.port, &my_dest);

    if (!rem_dest) {
        pp_close_ctx(cb);
        return 1;
    }

    inet_ntop(AF_INET6, &rem_dest->gid, gid, sizeof gid);
    printf("  remote address: LID 0x%04x, QPN 0x%06x, PSN 0x%06x, GID %s\n",
           rem_dest->lid, rem_dest->qpn, rem_dest->psn, gid);

    if (gettimeofday(&start, NULL)) {
        perror("gettimeofday");
        return 1;
    }

    scnt = 0;
    pre_scnt = 0;
    /****************************************************************************************************
     * The main loop where we client and server send and receive "iters" number of messages
     */
    while ((scnt < usr_par.iters)||(pre_scnt < usr_par.iters)) {

        int  r_size;
        char receivemsg[sizeof "0102030405060708:01020304:01020304:01020304"];
        char ackmsg[sizeof "rdma_write completed"];

        // Receiving RDMA data (address and rkey) from socket as a triger to start RDMA write operation
        DEBUG_LOG_FAST_PATH("Iteration %d: Waiting to Receive message\n", scnt);
        r_size = recv(cb->sockfd, receivemsg, sizeof(receivemsg), MSG_WAITALL);
        if (r_size != sizeof receivemsg) {
            fprintf(stderr, "Couldn't receive RDMA data for iteration %d\n", scnt);
            return 1;
        }
        sscanf(receivemsg, "%llx:%lx:%lx:%lx", &cb->gpu_buf_addr, &cb->gpu_buf_size, &cb->gpu_buf_rkey, &cb->rem_dctn);
        DEBUG_LOG_FAST_PATH("The message received: \"%s\", gpu_buf_addr = 0x%llx, gpu_buf_size = %lu, gpu_buf_rkey = 0x%lx, rem_dctn = 0x%lx\n",
                            receivemsg, (unsigned long long)cb->gpu_buf_addr, cb->gpu_buf_size, cb->gpu_buf_rkey, cb->rem_dctn);
        cb->size = mmin(cb->size, cb->gpu_buf_size);
        
        // Executing RDMA write
        sprintf((char*)cb->buf, "Write iteration N %d", scnt);
        ret = pp_post_send(cb);
        if (ret) {
            fprintf(stderr, "Couldn't post send: %u\n", ret);
            return 1;
        }
        do {
            struct ibv_wc wc[2];
            int ne, i;

            // Waiting for completion event
            if (usr_par.use_event) {
                ret = wait_for_completion_event(cb, &num_cq_events);
                if (ret) {
                    return 1;
                }
            }
            
            // Polling completion queue
            DEBUG_LOG_FAST_PATH("Polling completion queue\n");
            do {
                DEBUG_LOG_FAST_PATH("Before ibv_poll_cq\n");
                ne = ibv_poll_cq(cb->cq, 2, wc);
                if (ne < 0) {
                    fprintf(stderr, "poll CQ failed %d\n", ne);
                    return 1;
                }
            } while (!usr_par.use_event && ne < 1);

            for (i = 0; i < ne; ++i) {
                if (wc[i].status != IBV_WC_SUCCESS) {
                    fprintf(stderr, "Failed status \"%s\" (%d) for wr_id %d\n",
                            ibv_wc_status_str(wc[i].status),
                            wc[i].status, (int) wc[i].wr_id);
                    return 1;
                }

                switch ((int) wc[i].wr_id) {
                case SMALL_WRITE_WRID:
                    ++pre_scnt;
                    DEBUG_LOG_FAST_PATH("SMALL_WRITE_WRID complrtion: pre_scnt = %d, scnt = %d\n", pre_scnt, scnt);
                    break;
                case BUFF_WRITE_WRID:
                    ++scnt;
                    DEBUG_LOG_FAST_PATH("BUFF_WRITE_WRID complrtion: pre_scnt = %d, scnt = %d\n", pre_scnt, scnt);
                    break;
                default:
                    fprintf(stderr, "Completion for unknown wr_id %d\n", (int) wc[i].wr_id);
                    return 1;
                }
            }
        } while (pre_scnt > scnt);

        // Sending ack-message to the client, confirming that RDMA write has been completet
        if (write(cb->sockfd, "rdma_write completed", sizeof("rdma_write completed")) != sizeof("rdma_write completed")) {
            fprintf(stderr, "Couldn't send \"rdma_write completed\" msg\n");
            return 1;
        }
    }
    /****************************************************************************************************/

    if (gettimeofday(&end, NULL)) {
        perror("gettimeofday");
        return 1;
    }

    {
        float usec = (end.tv_sec - start.tv_sec) * 1000000 +
            (end.tv_usec - start.tv_usec);
        long long bytes = (long long) cb->size * usr_par.iters * 2;

        printf("%lld bytes in %.2f seconds = %.2f Mbit/sec\n",
               bytes, usec / 1000000., bytes * 8. / usec);
        printf("%d iters in %.2f seconds = %.2f usec/iter\n",
               usr_par.iters, usec / 1000000., usec / usr_par.iters);
    }

    ibv_ack_cq_events(cb->cq, num_cq_events);

    if (pp_close_ctx(cb))
        return 1;

    ibv_free_device_list(dev_list);
    free(rem_dest);

    return 0;
}
