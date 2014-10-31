#include "cvector.h"
#include <unistd.h>

using namespace std;

void cvector_vsprintf(vector<char> *v, 
                      const char * format, 
                      const va_list *ap_cpy)
{
    size_t size_needed = v->capacity() - v->size(); 
    int counter = 0;
    const size_t sz = v->size();
    do {  //should run at most twice
        if (counter > 1) {
            segfault(FN, "shouldn't run more than twice");  
        }
        counter++;
        
        v->resize(sz + size_needed);
        
        va_list ap;
        va_copy(ap, *ap_cpy);
        char *buf = &(*v)[sz];
        size_needed = vsnprintf(buf, size_needed, format, ap);
        va_end(ap);
        
        if(size_needed < 0) {
            fprintf(stderr, "%s() vsnprintf returned negative! IO error.\n", FN);
        }
        size_needed++; //bytes needed including terminator
    } while(size_needed > v->size() - sz);
    
    v->resize(v->size()-1);   //-1 to leave the null beyond len
}

void cvector_sprintf(vector<char> *v, const char * format, ...)
{
    va_list ap;
    va_start(ap, format);
    cvector_vsprintf(v, format, &ap);
    va_end(ap);
}
