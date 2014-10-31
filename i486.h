#ifndef FUTEX_I386_H
#define FUTEX_I386_H

#if defined(__cplusplus) || defined(c_plusplus)
extern "C" {
#endif

#include "common.h"

#define YW_INIT_SEM { 1, 0 }
struct semaphore {
    int32_t resource;
    int32_t waiters;
};

static __inline__ int 
__futex_waiter_inc(struct semaphore *sem)
{
    unsigned char overflow;
    __asm__ __volatile__("lock; incl %0;"
                         "sets %1;"
                               :"=m" (sem->waiters), "=r" (overflow) 
                               : "m" (sem->waiters) 
                               : "memory", "cc"
                         );
    
    if(overflow) { segfault(FN, "overflow"); }
    
    return overflow;
}

static __inline__ int 
__futex_waiter_dec(struct semaphore *sem)
{
    unsigned char underflow;
    __asm__ __volatile__("lock; decl %0;"
                         "sets %1;"
                               :"=m" (sem->waiters), "=r" (underflow) 
                               : "m" (sem->waiters) 
                               : "memory", "cc"
                         );
    
    if(underflow) { segfault(FN, "underflow"); }
    
    return underflow;
}

static __inline__ int32_t 
__waiter_count(struct semaphore *sem)
{
    int32_t waiters;
    __asm__ __volatile__("movl %1, %0;"
                               : "=r" (waiters) 
                               : "r" (sem->waiters) 
                               : "memory"
                         );
    
    return waiters;
}

/* Atomic dec: return new value. */
static __inline__ int 
__futex_down(struct semaphore *sem,
             int32_t *resource_cpy)
{
    __asm__ __volatile__("movl %0, %%eax;"
                         "1:"
                         "movl %%eax, %1;"
                         "decl %1;"
                         "js 2f;"
                         "lock; cmpxchgl %1, %0;"
                         "jnz 1b;"
                         "2:"
                               :"=m" (sem->resource), "=r" (*resource_cpy)
                               : "m" (sem->resource) 
                               : "eax", "memory", "cc"
                         );
    
    if(*resource_cpy < 0) { return -1; }
    
    return 0;
}

#ifdef DEBUG__MODE
static __inline__ void 
__futex_up(struct semaphore *sem)
{
    int32_t resource_cpy;
    __asm__ __volatile__("movl %0, %%eax;"
                         "1:"
                         "movl %%eax, %1;"
                         "incl %1;"
                         "js 2f;"
                         "lock; cmpxchgl %1, %0;"
                         "jnz 1b;"
                         "2:"
                               :"=m" (sem->resource), "=r" (resource_cpy)
                               : "m" (sem->resource) 
                               : "eax", "memory", "cc"
                         );
}
#else
static __inline__ void 
__futex_up(struct semaphore *sem)
{
    unsigned char overflow;
    __asm__ __volatile__("lock; incl %0;"
                         "sets %1;"
                               :"=m" (sem->resource), "=r" (overflow) 
                               : "m" (sem->resource) 
                               : "memory", "cc"
                         );
    
    if(overflow) { segfault(FN, "overflow"); }
}
#endif

#if __x86_64__
/* Commit the write, so it happens before we send the semaphore to
   anyone else */
static __inline__ void 
__futex_commit(void)
{
    /* Probably overkill, but some non-Intel clones support
       out-of-order stores, according to 2.5.5-pre1's
       linux/include/asm-i386/system.h */
    __asm__ __volatile__ ("lock; addq $0,0(%%rsp)": : :"memory");
}
#else
static __inline__ void 
__futex_commit(void)
{
    /* Probably overkill, but some non-Intel clones support
       out-of-order stores, according to 2.5.5-pre1's
       linux/include/asm-i386/system.h */
    __asm__ __volatile__ ("lock; addl $0,0(%%esp)": : :"memory");
}
#endif

#if __x86_64__
static __inline__ int 
__atomic64_inc(uint64_t counter)
{
    unsigned char overflow;
    __asm__ __volatile__("lock; incq %0;"
                         "sets %1;"
                               :"=m" (counter), "=r" (overflow) 
                               : "m" (counter) 
                               : "memory", "cc"
                         );
    
    if(overflow) { segfault(FN, "overflow"); }
    
    return overflow;
}
static __inline__ int 
__atomic64_dec(uint64_t counter)
{
    unsigned char underflow;
    __asm__ __volatile__("lock; decq %0;"
                         "sets %1;"
                               :"=m" (counter), "=r" (underflow) 
                               : "m" (counter) 
                               : "memory", "cc"
                         );
    
    if(underflow) { segfault(FN, "underflow"); }
    
    return underflow;
}
#endif

#if defined(__cplusplus) || defined(c_plusplus)
}
#endif

#endif // FUTEX_I386_H
