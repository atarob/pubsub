#ifndef cvector_H_DEFINED
#define cvector_H_DEFINED

#include "common.h"
#include <vector>

using namespace std;

static inline size_t cvector_append(vector<char> *v, 
                                    const void *buf, 
                                    size_t len)
{
    const size_t offst = v->size();
    if(len) {
        v->resize(offst+len);
        memcpy(&(*v)[offst], buf, len);
    }
    return offst;
}

static inline void cvector_nt(vector<char> *v)
{
    const size_t sz = v->size();
    v->reserve(sz+1);
    (*v)[sz]=0;
}

#define CVECTOR_APPEND(v, ptr) \
cvector_append(v, (void *)ptr, sizeof(*ptr))

void cvector_vsprintf(vector<char> *v, 
                      const char * format, 
                      const va_list *ap_cpy);

void cvector_sprintf(vector<char> *v, const char * format, ...);


#endif //cvector_H_DEFINED
