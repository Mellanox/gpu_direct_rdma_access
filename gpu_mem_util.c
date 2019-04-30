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
#include <config.h>
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
#endif //HAVE_CUDA

#include "gpu_mem_util.h"

static int debug = 1;
static int debug_fast_path = 1;
#define DEBUG_LOG if (debug) printf
#define DEBUG_LOG_FAST_PATH if (debug_fast_path) printf
#define FDEBUG_LOG if (debug) fprintf
#define FDEBUG_LOG_FAST_PATH if (debug_fast_path) fprintf

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
    
    for (devID = 0; devID < deviceCount; devID++) {
        int pci_bus_id = 0;
        CUCHECK(cuDeviceGetAttribute (&pci_bus_id, CU_DEVICE_ATTRIBUTE_PCI_BUS_ID, devID));
        DEBUG_LOG("Dev ID %d: pci_bus_id = %d\n", devID, pci_bus_id);
    }

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
#ifdef HAVE_CUDA
void work_buffer_free(void *buff, int use_cuda)
#else
void work_buffer_free(void *buff)
#endif //HAVE_CUDA
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

/*----------------------------------------------------------------------------*/

