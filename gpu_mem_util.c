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
/* "/usr/local/cuda/include/" is added to build include path in the Makefile */
#include "cuda.h"
#endif //HAVE_CUDA

#include "gpu_mem_util.h"

extern int debug;
extern int debug_fast_path;

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

static CUcontext cuContext;

/*
 * Debug print information about all available CUDA devices
 */
static void print_gpu_devices_info(void)
{
    int     device_count = 0;
    int     i;
    
    CUCHECK(cuDeviceGetCount(&device_count));
    
    DEBUG_LOG("The number of supporting CUDA devices is %d.\n", device_count);
    
    for (i = 0; i < device_count; i++) {
        CUdevice    cu_dev;
        char        name[128];
        int         pci_bus_id    = 0;
        int         pci_device_id = 0;
        int         pci_func = 0; /*always 0 for CUDA device*/

        CUCHECK(cuDeviceGet(&cu_dev, i));
        CUCHECK(cuDeviceGetName(name, sizeof(name), cu_dev));
        CUCHECK(cuDeviceGetAttribute (&pci_bus_id   , CU_DEVICE_ATTRIBUTE_PCI_BUS_ID   , cu_dev)); /*PCI bus identifier of the device*/
        CUCHECK(cuDeviceGetAttribute (&pci_device_id, CU_DEVICE_ATTRIBUTE_PCI_DEVICE_ID, cu_dev)); /*PCI device (also known as slot) identifier of the device*/

        DEBUG_LOG("device %d, handle %d, name \"%s\", BDF %02x:%02x.%d\n",
                  i, cu_dev, name, pci_bus_id, pci_device_id, pci_func);
    }
}

static int get_gpu_device_id_from_bdf(const char *bdf)
{
    int     given_bus_id = 0;
    int     given_device_id = 0;
    int     given_func = 0;
    int     device_count = 0;
    int     i;
    int     ret_val;
    
                    /*    "3e:02.0"*/
    ret_val = sscanf(bdf, "%x:%x.%x", &given_bus_id, &given_device_id, &given_func);
    if (ret_val != 3){
        fprintf(stderr, "Wrong BDF format \"%s\". Expected format example: \"3e:02.0\", "
                        "where 3e - bus id, 02 - device id, 0 - function\n", bdf);
        return -1;
    }
    if (given_func != 0) {
        fprintf(stderr, "Wrong pci function %d, 0 is expected\n", given_func);
        return -1;
    }
    CUCHECK(cuDeviceGetCount(&device_count));
    
    if (device_count == 0) {
        fprintf(stderr, "There are no available devices that support CUDA\n");
        return -1;
    }

    for (i = 0; i < device_count; i++) {
        CUdevice    cu_dev;
        int         pci_bus_id    = 0;
        int         pci_device_id = 0;

        CUCHECK(cuDeviceGet(&cu_dev, i));
        CUCHECK(cuDeviceGetAttribute (&pci_bus_id   , CU_DEVICE_ATTRIBUTE_PCI_BUS_ID   , cu_dev)); /*PCI bus identifier of the device*/
        CUCHECK(cuDeviceGetAttribute (&pci_device_id, CU_DEVICE_ATTRIBUTE_PCI_DEVICE_ID, cu_dev)); /*PCI device (also known as slot) identifier of the device*/
        if ((pci_bus_id == given_bus_id) && (pci_device_id == given_device_id)){
            return i;
        }
    }
    fprintf(stderr, "Given BDF \"%s\" doesn't match one of GPU devices\n", bdf);
    return -1;
}

static void *init_gpu(size_t gpu_buf_size, const char *bdf)
{
    const size_t    gpu_page_size = 64*1024;
    size_t          aligned_size;
    CUresult        cu_result;

    aligned_size = (gpu_buf_size + gpu_page_size - 1) & ~(gpu_page_size - 1);
    printf("initializing CUDA\n");
    cu_result = cuInit(0);
    if (cu_result != CUDA_SUCCESS) {
        fprintf(stderr, "cuInit(0) returned %d\n", cu_result);
        return NULL;
    }

    if (debug) {
        print_gpu_devices_info();
    }
    
    int dev_id = get_gpu_device_id_from_bdf(bdf);
    if (dev_id < 0) {
        fprintf(stderr, "Wrong device index (%d) obtained from bdf \"%s\"\n",
                dev_id, bdf);
        /* This function returns NULL if there are no CUDA capable devices. */
        return NULL;
    }

    /* Pick up device by given dev_id - an ordinal in the range [0, cuDeviceGetCount()-1] */
    CUdevice    cu_dev;
    CUCHECK(cuDeviceGet(&cu_dev, dev_id));

    DEBUG_LOG("creating CUDA Contnext\n");
    /* Create context */
    cu_result = cuCtxCreate(&cuContext, CU_CTX_MAP_HOST, cu_dev);
    if (cu_result != CUDA_SUCCESS) {
        fprintf(stderr, "cuCtxCreate() error=%d\n", cu_result);
        return NULL;
    }

    DEBUG_LOG("making it the current CUDA Context\n");
    cu_result = cuCtxSetCurrent(cuContext);
    if (cu_result != CUDA_SUCCESS) {
        fprintf(stderr, "cuCtxSetCurrent() error=%d\n", cu_result);
        return NULL;
    }

    DEBUG_LOG("cuMemAlloc() of a %zd bytes GPU buffer\n", aligned_size);
    CUdeviceptr d_A;
    cu_result = cuMemAlloc(&d_A, aligned_size);
    if (cu_result != CUDA_SUCCESS) {
        fprintf(stderr, "cuMemAlloc error=%d\n", cu_result);
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
void *work_buffer_alloc(size_t length, int use_cuda, const char *bdf)
{
    void    *buff = NULL;

    if (use_cuda) {
        /* Mem allocation on GPU */
#ifdef HAVE_CUDA
        buff = init_gpu(length, bdf);
#else
        fprintf(stderr, "Can't init GPU, HAVE_CUDA mode isn't set");
#endif //HAVE_CUDA
        if (!buff) {
            fprintf(stderr, "Couldn't allocate work buffer on GPU.\n");
            return NULL;
        }
    } else {
        /* Mem allocation on CPU */
        int page_size = sysconf(_SC_PAGESIZE);
        buff = memalign(page_size, length);
        if (!buff) {
            fprintf(stderr, "Couldn't allocate work buffer on CPU.\n");
            return NULL;
        }
        DEBUG_LOG("memory buffer(%p) allocated\n", buff);
    }
    return buff;
}

/****************************************************************************************
 * CPU or GPU memory free, according to HAVE_CUDA pre-compile option and use_cuda flag
 ****************************************************************************************/
void work_buffer_free(void *buff, int use_cuda)
{
    if (use_cuda) {
#ifdef HAVE_CUDA
        free_gpu(buff);
#else
        fprintf(stderr, "Can't free GPU, HAVE_CUDA mode isn't set");
#endif //HAVE_CUDA
    } else {
        DEBUG_LOG("free memory buffer(%p)\n", buff);
        free(buff);
    }
}

/*----------------------------------------------------------------------------*/

