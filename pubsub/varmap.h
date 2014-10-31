#ifndef varmap_H_DEFINED
#define varmap_H_DEFINED

#if defined(__cplusplus) || defined(c_plusplus)
extern "C" {
#endif

#include "common.h"
#include <stdlib.h>
#include <stdarg.h>
#include <sys/mman.h>
#include <linux/mman.h>
#include <sys/types.h>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>

struct varmap {
    uint64_t len;
    uint64_t max;
    int fd;
    int garbage;  //just keep this extra 4 byte to 8 byte align the rest
    char buf[0];
} __attribute__((packed));

struct varmap_subset {
    struct varmap **buf;
    size_t offst;
    size_t len;
};

static inline void *varmap_mmap2(size_t *len, 
                                 size_t *max, 
                                 int * file_handle, 
                                 const char * filename,
                                 const int flags,
                                 const mode_t mode)
{
    //fprintf(stderr,"%s\n",filename);
    *file_handle=open(filename, flags, mode);
    if (*file_handle==-1) {
        perror("varmap_mmap2(), open failed"); 
        segfault(FN,"File will not open.");
    }
    *max = *len = lseek64(*file_handle, 0, SEEK_END);
    if(*max==0) {
        *max=sysconf(_SC_PAGESIZE);
        if (ftruncate(*file_handle,*max)){ perror("failed truncate"); segfault(FN,"Could not enlarge file.");}
    }
    int tmpflags = flags & (O_RDONLY | O_WRONLY | O_RDWR);
    int prot = ((tmpflags==O_RDONLY || tmpflags==O_RDWR)?PROT_READ:0);
    prot    |= ((tmpflags==O_WRONLY || tmpflags==O_RDWR)?PROT_WRITE:0);
    void * ptr = mmap(NULL,*max, prot,MAP_SHARED,*file_handle,0);
    if(ptr==MAP_FAILED) { ptr=NULL; perror("mmap failed\n"); segfault(FN,"NULL"); }// graceful failure? return NULL.
    return ptr;
}

static inline void *varmap_mmap(size_t *len, 
                                size_t *max, 
                                int * file_handle, 
                                const char * filename)
{
    return varmap_mmap2(len, max, file_handle, filename, O_RDWR | O_CREAT, 
                   S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH); 
}

static inline uint32_t varmap_freemap(void *ptr, 
                                      size_t newsize, 
                                      size_t size, 
                                      int file_handle)
{
    uint32_t rslt = 0;
    if(!ptr) { segfault(FN, "freeing NULL!"); }
    if (ftruncate(file_handle,newsize)) { 
        perror("resize the varmap file on exit."); rslt|=1;
    }
    if (munmap(ptr,size)) { perror("error unmapping buffer file"); rslt|=2; }
    if (close(file_handle)) { perror("IO problem closing buffer file"); rslt|=4; }
    
    return rslt;
}

static inline void *varmap_mremap(void *ptr, size_t oldsize, size_t newsize, int file_handle)
{
    if (ftruncate(file_handle,newsize)) { segfault(FN, "failed truncate"); }
    ptr = mremap(ptr, oldsize, newsize, MREMAP_MAYMOVE);

    if(ptr==MAP_FAILED) { segfault(FN, "file buffer remap failed"); }
    return ptr;
}

static inline size_t varmap_avail(struct varmap *b)
{
    return b->max - b->len;
}

#define varmap_DONT_INFLATE 0
#define varmap_INFLATE 1
static inline void varmap_adj_max_2(struct varmap **b, 
                                    const size_t requested_size, 
                                    const char inflate)
{
    const int overhead = sizeof(struct varmap);
    size_t s = requested_size+overhead;
    if(requested_size > (*b)->max) {
        if(inflate) { 
            //serv_err("%s() requested_size:%llu (*b)->max:%llu\n", FN,
                     //typULL(requested_size), typULL((*b)->max));
            s = round_up_to_power_2(s); 
            if(s<requested_size) { segfault(FN, "overflow"); }
        }
    }
    else { (*b)->len = MIN((*b)->len, requested_size); }
    *b = (struct varmap *) 
          varmap_mremap(*b, (*b)->max+overhead, s, (*b)->fd);
    if(!(*b)) { perror(FN); segfault(FN, "varmap_mremap failed"); }
    
    //(*b)->buf = ((char *)(*b)) + overhead;
    (*b)->max = s-overhead;
}

static inline void varmap_adj_max(struct varmap **b, 
                                  const size_t requested_size)
{
    varmap_adj_max_2(b, requested_size, varmap_INFLATE);
}

static inline void varmap_make_avail(struct varmap **b, 
                                     const size_t requested_size)
{
    const size_t total = (*b)->len + requested_size;
    if(total > (*b)->max) { varmap_adj_max(b, total); }
}

static inline void varmap_set_len(struct varmap **b, 
                                  const size_t len)
{
    if(len > (*b)->max) { varmap_adj_max(b, len); }
    (*b)->len = len;
}

static inline
struct varmap *varmap_init2(char * filename,
                            const int flags, 
                            const mode_t mode)
{
    const size_t overhead = sizeof(struct varmap);
    
    struct varmap tmp;
    BZERO(&tmp);
    size_t tlen, tmax;
    struct varmap * b = 
          (struct varmap *) 
          varmap_mmap2(&tlen, &tmax, &tmp.fd, filename, flags, mode);
    tmp.len=tlen;
    tmp.max=tmax;
    if(!b) { segfault(FN, filename); }
    if(tmp.len < overhead) {
        tmp.len = 0;
        if(tmp.max<overhead) { 
            segfault(FN, "isn't pagesize>sizeof(struct varmap)?!"); 
        }
        *b = tmp;
    }
    else {
        if(b->len > tmp.max-overhead) { segfault(FN, "bad looking varmap header"); } 
    }
    //b->buf = ((char *)b)+overhead;
    b->max = tmp.max-overhead;
    b->fd = tmp.fd;
    
    return b;
}

static inline
struct varmap *varmap_init(char * filename)
{
    return varmap_init2(filename, O_RDWR | O_CREAT, 
                        S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
}

static inline void varmap_clear(struct varmap ** b)
{
    (*b)->len = 0;
}

static inline uint32_t varmap_close(struct varmap ** b)
{
    const int overhead = sizeof(struct varmap);
    uint32_t rslt = 0;
    
    if(*b) { 
        rslt |= 
              varmap_freemap(*b, (*b)->len+overhead, (*b)->max+overhead,
                             (*b)->fd); 
    } 
    
    *b = NULL; //overkill, mainly we just want to set buf=NULL to avoid mem bugs.
    return rslt;
}

static inline size_t varmap_reserve(struct varmap **b, 
                                    const size_t inc)
{
    const size_t len = (*b)->len;
    varmap_set_len(b, (*b)->len+inc);
    return len;
}

static inline void * varmap_varr(struct varmap *b, 
                                 const size_t offst)
{
    if(!b->buf) { return NULL; }
    return (char *)b->buf + offst;
}

static inline char * varmap_carr(struct varmap *b, 
                                 const size_t offst)
{
    if(!b->buf) { return NULL; }
    return b->buf+offst;
}

#define VARMAP_ARR_2(buf, offst, type) ((type *) varmap_varr(buf, offst))
#define VARMAP_ARR(buf, type) VARMAP_ARR_2(buf, 0, type)

static inline void varmap_append_no_term(struct varmap **b, 
                                         const void *ptr, 
                                         const size_t size,
                                         int sync_flag)
{
    size_t offst = (*b)->len;
    
    varmap_make_avail(b, size);
    memcpy((*b)->buf+offst, ptr, size);
    
    if(sync_flag) { 
        const uintptr_t file_offst = ((uintptr_t)(*b)->buf)+offst;
        const uintptr_t page_aligned = 
              file_offst & 
              (~(uintptr_t)(sysconf(_SC_PAGE_SIZE)-1));
        if(msync((void *)page_aligned, file_offst+size-page_aligned, 
                 sync_flag)) {
            perror("Failed to sync varmap");
            segfault(FN, "varmap couldn't sync at 1, bye now.\n");
        }
    }
    
    //using flag MS_ASYNC gives great throughput, but if the below
    //   sync happens before the above sync, and hardware/kernel
    //   explodes right then, we will endup with corruption
    //   at the end of the file.
    (*b)->len+=size;
    
    if(sync_flag) { 
        if(msync((*b), sizeof(struct varmap), sync_flag)) {
            perror("Failed to sync varmap");
            segfault(FN, "varmap couldn't sync at 2, bye now.\n");
        }
    }
}

static inline void varmap_append_c(struct varmap **b, 
                                   const char c,
                                   const int sync)
{
    varmap_append_no_term(b, &c, 1, sync);
}

static inline void varmap_append(struct varmap **b, 
                                 const void *ptr, 
                                 const size_t size,
                                 const int sync)
{
    varmap_append_no_term(b, ptr, size, sync);
 
    varmap_make_avail(b, 1);
    *varmap_carr(*b,(*b)->len) = 0;
}

#define varmap_APPEND(b, v, sync) \
varmap_append_no_term(b, v, sizeof(*(v)), sync)

static inline void varmap_append_nt(struct varmap **b, 
                                    const char *ptr,
                                    const int sync)
{
    varmap_append(b, ptr, strlen(ptr), sync);
}

static inline void varmap_append_nt_chain(struct varmap **b, 
                                          const char *ptr,
                                          const int sync)
{
    varmap_append_no_term(b, ptr, strlen(ptr)+1, sync);
}

#if defined(__cplusplus) || defined(c_plusplus)
}
#endif

#endif //varmap_H_DEFINED
