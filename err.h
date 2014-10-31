#ifndef __ERR_H
#define __ERR_H

#include "common.h" 
#include "usersem.h" 

#if defined(__cplusplus) || defined(c_plusplus)
extern "C" {
#endif

extern struct semaphore stderr_lock;
extern struct semaphore stdout_lock;

static inline
void serv_perror(const char * msg)
{
    const int cpy = errno;
    while(futex_down(&stderr_lock)) { }
    errno = cpy;
    perror(msg);
    futex_up(&stderr_lock);
}
               
static inline
void serv_error(const char * format, ...)
{
    va_list ap;
    while(futex_down(&stderr_lock)) { }
    
    va_start(ap, format);
    vfprintf(stderr, format, ap);
    va_end(ap);
    
    futex_up(&stderr_lock);
}

static inline
void dbg_printf(const char *func, 
                const int line, 
                const char * format, ...)
{
#ifdef DEBUG__MODE
    while(futex_down(&stderr_lock)) { }
    
    fprintf(stderr, "#thread:%ld, %s() line:%d|",
            syscall(SYS_gettid), func, line);
    
    va_list ap;
    va_start(ap, format);
    vfprintf(stderr, format, ap);
    va_end(ap);
    
    fprintf(stderr, "\n");
    
    futex_up(&stderr_lock);
#endif
}


static inline
void serv_segfault(const char * msg, const char * msg2)
{
    while(futex_down(&stderr_lock)) { }
    
    segfault(msg, msg2);
    
    futex_up(&stderr_lock);
}

static inline void futex_down2(struct semaphore *sem,
                               const char *msg)
{
    while(futex_down(sem)) { serv_perror(msg); }
}

#if defined(__cplusplus) || defined(c_plusplus)
}
#endif
#endif /* __ERR_H */
