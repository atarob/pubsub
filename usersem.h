#ifndef _USERSEM_H
#define _USERSEM_H

/*
 * Userspace Semaphores
 */
#include "common.h"
#include <stdint.h>
#include <linux/futex.h>
#include <sys/time.h>
#include <unistd.h> /* size_t */
#include <sys/syscall.h>
#include <stdio.h>
#include "i486.h"

#if defined(__cplusplus) || defined(c_plusplus)
extern "C" {
#endif

static inline
void futex_init(struct semaphore *sem)
{
    //fprintf(stderr, "%s %p\n", msg, sem);
    sem->resource = 1;
    sem->waiters = 0;
    
    __futex_commit();
}

static inline int futex(int32_t *uaddr, int op, int32_t val, const struct timespec *timeout,
                        int *uaddr2, int val3)
{
    return syscall(SYS_futex , uaddr, op, val, timeout, uaddr2, val3);
}

static inline int32_t futex_up(struct semaphore *sem)
{
    __futex_up(sem);
    if(__waiter_count(sem) == 0 || sem->resource==0) { return 0; }
    
    //TODO why do we need the commit? original code only used it in futex_init
    __futex_commit();
    int32_t woken = futex(&sem->resource, FUTEX_WAKE, sem->resource, NULL, NULL, 0);
    
    return woken;
}

static inline int futex_down_timeout_fairness(struct semaphore *sem, 
                                              struct timespec *rel,
                                              const char fairness)
{
    int ret=0;
    uint8_t first=1;
    uint8_t waiter_incremented=0;
    /* Returns new value */
    do {
        int32_t resource_cpy;
        if(__futex_down(sem, &resource_cpy)==0) { 
            if(!first || fairness==0 || resource_cpy>sem->waiters) { ret=0; break; }
            futex_up(sem); //decided to be fair and line up
        }
        
        if(first) { 
            __futex_waiter_inc(sem); 
            first=0;
            waiter_incremented=1;
        }
        
        //        debug_printf("sleepin: sem:%p, res:%03d, waiters:%03d\n",sem,sem->resource,sem->waiters);  
        
        errno = 0;
        ret = futex(&sem->resource, FUTEX_WAIT, 0, rel, NULL, 0);
        
    } while(ret == 0 || //got woken
            errno == EWOULDBLOCK); //resource changed from 0
    
    if(ret && errno == ETIMEDOUT) { ret = 1; }
    if(waiter_incremented) { __futex_waiter_dec(sem); }
    
    return ret;
}

static inline int futex_down_timeout(struct semaphore *sem, 
                                     struct timespec *rel)
{
    return futex_down_timeout_fairness(sem, rel, 0);
}

static inline int futex_down_timeout_fair(struct semaphore *sem, 
                                          struct timespec *rel)
{
    return futex_down_timeout_fairness(sem, rel, 1);
}

static inline int futex_down_fair(struct semaphore *sem)
{
    return futex_down_timeout_fairness(sem, NULL, 1);
}

static inline int futex_down(struct semaphore *sem)
{
    return futex_down_timeout(sem, NULL);
}

static inline int futex_trydown(struct semaphore *sem)
{
    int32_t resource_cpy;
    return __futex_down(sem, &resource_cpy);
}

//// Read Write locks (&trylocks, no timeout/fairness yet) - i486.h semaphore version:
struct rwlock {
    struct semaphore main;
    // Readers get the main lock just to increament the read counter,
    // Writers hold the main lock, blocking new readers, until they have 
    // finished writing (first waiting for all readers to finish).
    struct semaphore write;
    // the "write.resource" is actually the read counter.
    // write locks can only complete when this is zero.
};
static inline void readwrite_init(struct rwlock * lock)
{
    futex_init(&lock->main);
    futex_init(&lock->write);
    lock->write.resource=0; // no readers initially.
}
static inline void read_lock(struct rwlock * lock) // blocking
{
    while(futex_down(&lock->main)){ perror(FN);}
    __futex_up(&lock->write); // increment reader count.
    futex_up(&lock->main);
}
static inline int read_trylock(struct rwlock * lock) // non-blocking
{
    if (futex_trydown(&lock->main)){return -1;}
    __futex_up(&lock->write);
    futex_up(&lock->main);
    return 0;
}
static inline void read_unlock(struct rwlock * lock)
{
    int resource;
    if(__futex_down(&lock->write,&resource)) { 
        segfault(FN, "should never block, resource upped on lock."); 
    }
    if((lock->write.waiters>0)&&(resource==0)){
        __futex_commit();
        futex(&lock->write.resource, FUTEX_WAKE, 1, NULL, NULL, 0);
    } // should never be more than one "write" waiter. Others stuck on "main".
}
static inline int read2write_trylock(struct rwlock * lock) // blocking but can fail
{
    if (futex_trydown(&lock->main)) return -1;
    int resource;
    if(__futex_down(&lock->write,&resource)) { 
        segfault(FN, "should never block, resource upped on lock."); 
    }
    uint32_t readers = lock->write.resource;
    while (readers>0){ 
        __futex_waiter_inc(&lock->write);
        futex(&lock->write.resource, FUTEX_WAIT, readers, NULL, NULL, 0);
        __futex_waiter_dec(&lock->write);
        readers=lock->write.resource;
    }
    return 0;
}    
static inline void write_lock(struct rwlock * lock) // blocking
{
    while(futex_down(&lock->main)){ perror(FN);}
    uint32_t readers = lock->write.resource;
    while (readers>0){ 
        __futex_waiter_inc(&lock->write);
        futex(&lock->write.resource, FUTEX_WAIT, readers, NULL, NULL, 0);
        __futex_waiter_dec(&lock->write);
        readers=lock->write.resource;
    }
}
static inline int write_trylock(struct rwlock * lock) // non-blocking
{
    if (futex_trydown(&lock->main)){return -1;}
    if (lock->write.resource>0) {
        futex_up(&lock->main);
        return -1;
    }
    return 0;
}
static inline int write_trylockrelease(struct rwlock *lock, struct rwlock *oldlock)
{
    if (futex_trydown(&lock->main)) {return -1;}
    read_unlock(oldlock);
    uint32_t readers = lock->write.resource;
    while (readers>0){ 
        __futex_waiter_inc(&lock->write);
        futex(&lock->write.resource, FUTEX_WAIT, readers, NULL, NULL, 0);
        __futex_waiter_dec(&lock->write);
        readers=lock->write.resource;
    }
    return 0;
}
static inline void write_unlock(struct rwlock * lock)
{
    futex_up(&lock->main);
}
static inline void check_rwlock(struct rwlock * lock, char * lockname )
{
    if (lock->main.resource!=1 || lock->main.waiters!=0
        || lock->write.resource!=0 || lock->write.waiters!=0) {
        fprintf(stderr,"Lock Report for %s: Resource=%d, Waiters=%d, Readers=%d, Write_wait=%d.\n",
            lockname,lock->main.resource,lock->main.waiters,lock->write.resource,lock->write.waiters);
    }
}
struct rec_futex {
    struct semaphore resource_lock;
    struct semaphore control_lock;
    uint64_t id;
    uint64_t bucket;
};

static inline void rec_futex_init(struct rec_futex * lock) {
    futex_init(&lock->resource_lock);
    futex_init(&lock->control_lock);
    lock->id = 0;
    lock->bucket = 0;
}

static inline void rec_futex_down(struct rec_futex * lock, const uint64_t id) {
    if (id == UINT64_MAX) {
        segfault(FN, "UINT64_MAX is not an allowed id");
    }
    while (futex_down(&lock->control_lock)) { perror(FN); }
    if (futex_trydown(&lock->resource_lock)) {
        if (lock->id == id) {
            //debug_printf(" %llu ++ bucket, id == id, have lock\n", typULL(id));
            uint64_t bucket_cpy = lock->bucket;
            lock->bucket++; 
            if (lock->bucket > 1) { segfault(FN, "lock->bucket>1"); }
            if (lock->bucket <= bucket_cpy) { segfault(FN, "overflow"); }
        }
        else {
            //debug_printf(" %llu got != id, other has lock\n", typULL(id));
            futex_up(&lock->control_lock);
            while (futex_down(&lock->resource_lock)) { perror(FN);}
            while (futex_down(&lock->control_lock)) {perror(FN);}
            //debug_printf(" %llu assigning lock, got after contention\n", typULL(id));
            lock->id = id;
        }
    }
    else {
        //debug_printf(" %llu assigning id, have lock\n", typULL(id));
        lock->id = id; 
    }
    futex_up(&lock->control_lock);
    //debug_printf(" %llu control unlocked\n", typULL(id));
}

static inline void rec_futex_up(struct rec_futex * lock) {
    while (futex_down(&lock->control_lock)) {perror(FN);}
    if (lock->bucket == 0) {
        //debug_printf(" %llu unlocking and clearing id\n", typULL(lock->id));
        lock->id = UINT64_MAX;
        futex_up(&lock->resource_lock);
    }
    else {
        //debug_printf(" %llu -- bucket\n", typULL(lock->id));
        uint64_t bucket_cpy = lock->bucket; 
        lock->bucket--;
        if (lock->bucket >= bucket_cpy) { segfault(FN, "underflow"); }
    }
    futex_up(&lock->control_lock); 
    //debug_printf(" %llu done unlocking\n", typULL(lock->id));
}

////////////////////////////////////
#if defined(__cplusplus) || defined(c_plusplus)
}
#endif

#endif /* _USERSEM_H */
