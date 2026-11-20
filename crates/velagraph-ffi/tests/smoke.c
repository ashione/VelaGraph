#include "velagraph.h"
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
_Static_assert(sizeof(vg_reply)==272,"ABI reply layout");
_Static_assert(offsetof(vg_reply,error)==16,"ABI error offset");
_Static_assert(sizeof(vg_stats)==32,"ABI stats layout");
_Static_assert(sizeof(vg_buffer_view)==24,"ABI borrowed view layout");
static uint64_t le64(const uint8_t *p) { uint64_t n=0; for(unsigned i=0;i<8;i++) n|=((uint64_t)p[i])<<(i*8); return n; }
#define REQUIRE(x) do {if(!(x)){fprintf(stderr,"failed at line %d\n",__LINE__);return 1;}}while(0)
int main(void){
 REQUIRE(vg_abi_version()==VG_ABI_VERSION);
 vg_stats before,after;REQUIRE(vg_runtime_stats(&before)==VG_OK);
 const char *load="{\"backend\":\"memory\",\"identity\":\"c-smoke\",\"vertices\":[{\"id\":9007199254740993,\"properties\":{\"n\":{\"Integer\":9223372036854775807}}}]}";
 vg_reply reply;
 REQUIRE(vg_call(1,VG_OPEN,0,(const uint8_t*)load,strlen(load),0,1000,0,&reply)==VG_OK);
 vg_handle store=reply.value;
 const char *query="{\"queries\":[{\"text\":\"g.V(9007199254740993).values('n')\"}]}";
 REQUIRE(vg_call(1,VG_QUERY_BATCH,store,(const uint8_t*)query,strlen(query),0,1000,4096,&reply)==VG_OK);
 uint64_t size=0;REQUIRE(vg_buffer_size(reply.buffer,&size)==VG_OK);
 char *output=(char*)malloc((size_t)size+1);REQUIRE(output!=NULL);
 memset(output,'x',(size_t)size+1);
 REQUIRE(vg_buffer_copy(reply.buffer,(uint8_t*)output,size-1)==VG_BUFFER_TOO_SMALL);
 REQUIRE(output[0]=='x');
 REQUIRE(vg_buffer_copy(reply.buffer,(uint8_t*)output,size)==VG_OK);output[size]='\0';
 REQUIRE(strstr(output,"9223372036854775807")!=NULL);free(output);
 REQUIRE(vg_release(reply.buffer)==VG_OK);
 REQUIRE(vg_call(VG_ABI_VERSION,VG_QUERY_PACKED,store,(const uint8_t*)query,strlen(query),0,1000,4096,&reply)==VG_OK);
 vg_buffer_view view; REQUIRE(vg_buffer_acquire(reply.buffer,&view)==VG_OK);
 REQUIRE(vg_release(reply.buffer)==VG_OK);
 REQUIRE(vg_release(store)==VG_OK);REQUIRE(vg_release(store)==VG_INVALID_HANDLE);
 REQUIRE(view.len==104 && memcmp(view.data,"VGQR0001",8)==0);
 REQUIRE(le64(view.data+80)==3 && le64(view.data+88)==UINT64_C(9223372036854775807));
 REQUIRE(vg_release(view.lease)==VG_OK);
 REQUIRE(vg_runtime_stats(&after)==VG_OK);
 REQUIRE(before.registered_handles==after.registered_handles);
 REQUIRE(before.wire_reserved_bytes==after.wire_reserved_bytes);
 REQUIRE(before.plan_reserved_bytes==after.plan_reserved_bytes);
 puts("C ABI smoke passed");return 0;
}
