IDIR = .
CC = gcc
ODIR = obj

ifeq ($(USE_CUDA),1)
  CFLAGS = -I$(IDIR) -g -DHAVE_CUDA
  LIBS = -Wall -lrdmacm -libverbs -lmlx5 -lcuda
else
  CFLAGS = -I$(IDIR) -g
  LIBS = -Wall -lrdmacm -libverbs -lmlx5
endif

OEXE_CLT = write_to_gpu_client
OEXE_SRV = write_to_gpu_server

DEPS = rdma_write_to_gpu.h
DEPS += ibv_helper.h
DEPS += khash.h
DEPS += gpu_mem_util.h
DEPS += utils.h

OBJS = rdma_write_to_gpu.o
OBJS += gpu_mem_util.o
OBJS += utils.o

$(ODIR)/%.o: %.c $(DEPS)
	$(CC) -c -o $@ $< $(CFLAGS)

all : make_odir $(OEXE_CLT) $(OEXE_SRV)

make_odir: $(ODIR)/

$(OEXE_SRV) : $(patsubst %,$(ODIR)/%,$(OBJS)) $(ODIR)/write_to_gpu_server.o
	$(CC) -o $@ $^ $(CFLAGS) $(LIBS)

$(OEXE_CLT) : $(patsubst %,$(ODIR)/%,$(OBJS)) $(ODIR)/write_to_gpu_client.o
	$(CC) -o $@ $^ $(CFLAGS) $(LIBS)

$(ODIR)/:
	mkdir -p $@

.PHONY: clean

clean :
	rm -f $(OEXE_CLT) $(OEXE_SRV) $(ODIR)/*.o *~ core.* $(IDIR)/*~

