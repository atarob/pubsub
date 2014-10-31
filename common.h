#ifndef YW_COMMON_H_DEFINED
#define YW_COMMON_H_DEFINED

#include <stdint.h>
#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <stddef.h>
#include <time.h>
#include <sys/time.h>
#include <stdarg.h>

#if defined(__cplusplus) || defined(c_plusplus)
extern "C" {
#endif

#ifndef _GNU_SOURCE 
#define _GNU_SOURCE
#endif

#define FN __func__
#define LN __LINE__

#ifndef  UINT64_MAX
#define  UINT64_MAX (((uint64_t) 0)-1)
#endif

#ifndef  INT64_MAX
#define  INT64_MAX ( (int64_t)(UINT64_MAX>>1) )
#endif

#ifndef  UINT32_MAX
#define  UINT32_MAX (((uint32_t) 0)-1)
#endif

#ifndef  INT32_MAX
#define  INT32_MAX ( (int32_t)(UINT32_MAX>>1) )
#endif

#define BZERO(x) memset( (x), 0, sizeof(*(x)) )

#define STRINGIZE(x) #x
#define STRINGIZE_VAL(x) STRINGIZE(x)

#define ROUND_UP_TO_MULTIPLE(a,b) \
( ( (a) % (b) == 0) ? (a) : ( (a) + ( (b) - ( (a) % (b) ) ) ) )

static inline size_t round_up_to_power_2(size_t v)
{
    v--;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
#ifdef __x86_64
        v |= v >> 32;
#endif
    v++;
    
    return v;
}

#define MIN(x,y) (((x)<(y))?(x):(y))
#define MAX(x,y) (((x)>(y))?(x):(y))

static inline
void segfault(const char * msg,
              const char * msg2) 
{
    fprintf(stderr, "%s:%s\nself destruct demanded. inducing segmentation fault\n", msg,msg2);
    
    *( (char *) NULL ) = 0; //KABOOOOM!!!
}

static inline uint64_t gtime_usec()
{
    struct timeval tv;
    uint64_t rslt;
    if(gettimeofday(&tv, 0) != 0) { 
        perror("gtime"); 
        return 0; 
    }
    rslt = 
          (uint64_t)tv.tv_sec * 1000000 + 
          (uint64_t)tv.tv_usec;
    
#if (CLOCKS_PER_SEC != 1000000)
    rslt = (uint64_t) ( (double)rslt/ (double)CLOCKS_PER_SEC * (double)1000000 );
#endif
    
    return rslt;
}

#if defined(__cplusplus) || defined(c_plusplus)
}
#endif


#endif //YW_COMMON_H_DEFINED
