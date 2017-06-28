#include "wt_internal.h"
#include </opt/ibm/capikv/include/capiblock.h>
#include <sys/errno.h>
#include <math.h>
#include <stdlib.h>
#include <semaphore.h>  /* required for semaphores */

/***
*
* Logging implementation using CAPI-Flash
*
* 1:
* 2:
* 3:
*Check WT connection string.
*Each txn_commit calls write. It will briefly lock to allocate offset and block until CAPI writeBlock succeeds.
*
***/

/**/
const char* WT_CAPI_DEVICES[2]={"/dev/sg7", "/dev/sg8"}; 
static size_t *utility_buffer;
long utility_chunk = -1;
size_t totalBlocksWritten = 0;

/**/
chunk_id_t chunks[WT_CAPI_NUMBER_OF_CHUNKS];
pthread_spinlock_t lock;
/* semaphore for shared static local variable in thread_func()
 *   see the explanation in thread_func() comments
 */
static sem_t count_sem;
time_t epoch;
WT_CAPI_FREELIST *freelist;
WT_CAPI_FREELIST *inUse;
WT_CAPI_SEGMENT *active;
WT_LSN context;
WT_LSN checkpoint; 

/*
* Utility method to check if CAPI Logging is enabled
* WiredTiger's regular journal prevails if both are configured
*/
int
__wt_checkConfig(WT_SESSION_IMPL *session, const char *cfg[])
{
  WT_CONNECTION_IMPL *conn;
  WT_CONFIG_ITEM cval;
  bool enabled = false;
  conn = S2C(session);

  /* Check if CAPI is configured */
  WT_RET(__wt_config_gets(session, cfg, "capi.enabled", &cval));
  if (cval.val != 0) {
   WT_CAPI_PRINT("CAPILOG:CAPILOG is enabled!\n");
  }
  enabled = cval.val != 0;

  /* Check if WiredTiger's journaling is enabled */
  WT_RET(__wt_config_gets(session, cfg, "log.enabled", &cval));
  if (cval.val == 0) {
    WT_CAPI_PRINT("CAPILOG:WiredTiger journaling is enabled!\n");
  }else{
    if (enabled) {
      enabled = false;
    }
  }

  return (enabled);
}

/*
* iniialize the chunks
*/
int
__wt_capilog_init(WT_SESSION_IMPL *session, const char *cfg[])
{
  if (!__wt_checkConfig(session,cfg)) {
    WT_CAPI_PRINT("CAPILOG:CAPILOG not enabled!\n");
    return (0);
  }
  utility_buffer = malloc(4096);
  epoch = time(NULL);
  WT_CONNECTION_IMPL *conn;
  conn = S2C(session);
  FLD_SET(conn->log_flags, WT_CONN_LOG_CAPI_ENABLED);

  /*Initialize capiblock*/
  int rc = cblk_init(NULL,0);
  if (rc) {
    WT_CAPI_PRINT("CAPILOG:cblk_init failed with rc = %d and errno = %d\n", rc, errno);
  }

  int length = (sizeof(WT_CAPI_DEVICES)/sizeof(*WT_CAPI_DEVICES));
  /*create defined number of chunks*/
  for (int i=0;i<WT_CAPI_NUMBER_OF_CHUNKS;i++) {
	  /*open chunks in persistent mode*/
	  chunk_id_t chunk_id = cblk_open(WT_CAPI_DEVICES[i%length],WT_CAPI_NUMBER_OF_ASYNC_PER_CHUNK,O_RDWR,0,0);
	  if (chunk_id==NULL_CHUNK_ID) {
      WT_CAPI_PRINT("CAPILOG:open chunk failed error code = %d\n",errno);
	  }
	  chunks[i] = chunk_id;
  }
  utility_chunk = chunks[0];

  freelist = malloc(sizeof(WT_CAPI_FREELIST));
  inUse = malloc(sizeof(WT_CAPI_FREELIST));

  __wt_capi_initList(freelist);
  __wt_capi_initList(inUse);
  if (sem_init(&count_sem, 0, WT_CAPI_OUTSTANDING_WRITES) == -1)
  {
    WT_CAPI_PRINT("sem_init: failed: %s\n", strerror(errno)); 
  }else{
    WT_CAPI_PRINT("semaphore initilized!!"); 
  }
  for (int i=0;i<WT_CAPI_NUMBER_OF_SEGMENTS;i++) {
		WT_CAPI_SEGMENT *seg = malloc(sizeof(WT_CAPI_SEGMENT));
		seg->offset = -1;
		seg->bk_position = i;
		seg->unique_id= -1;
		__wt_capi_insertBack(freelist,seg);
  }

  //Activate the first segment and start taking writes
  __wt_capilog_activateNextSegment();

  return (0);
}

/*
* Write to the current avaiable log segment
* tx_log_commit(session,record,null,flag for sync)
*/
int
__wt_capilog_write(WT_SESSION_IMPL *session, WT_ITEM *record, WT_LSN *lsnp,
		   uint32_t flags)
{
 
  /*fill segment id - size - crc data - crc*/
  int write_offset = 0;
  int record_size_in_blocks = (int) ceil((double) record->size / (WT_CAPI_BLOCK_SIZE));
  back:
  pthread_spin_lock(&lock);
  if (!__wt_capilog_hasCapacity(active,record_size_in_blocks)) {
    if(!__wt_capilog_activateNextSegment()){
      pthread_spin_unlock(&lock);
      WT_CAPI_PRINT("CAPILOG:Out of space! Releasing the lock");
      goto back;
    };
  }
  write_offset=active->offset;
  active->offset+=record_size_in_blocks;
  totalBlocksWritten+=record_size_in_blocks;
  pthread_spin_unlock(&lock);
  /*
  * WT_ITEM includues a WT_LOG_RECORD struct in data field.
  */
  /*
  * We need to add our own fields from CAPI-Flash paper.
  * UNIQUE ID - SIZE - CRC - DATA - CRC 
  */

  /*We ask WT_LOG to align buffer to 4096 so this shold be good.*/
  //uint32_t mx = *((uint32_t *) record->size); 
  WT_LOG_RECORD *myrec;
  myrec = (WT_LOG_RECORD *) record->data;
  myrec->unique_id = active->unique_id;
  myrec->len = record->size;
  //20 byte
  myrec->checksum = __wt_checksum(myrec, 8);

  sem_wait(&count_sem);
  
  //WT_CAPI_PRINT("CAPILOG:Writing = %d  at %d \n",(int)record_size_in_blocks,(int) write_offset);
  int rc = cblk_write(chunks[active->offset%WT_CAPI_NUMBER_OF_CHUNKS],(void *) myrec, write_offset, record_size_in_blocks, 0);
  sem_post(&count_sem);   
  
  //WT_CAPI_PRINT("!!!CAPILOG:write end");

  //WT_CAPI_PRINT("memlen %d MX %zu , CAPILOG: write_offset:%d - record_size_in_blocks:%d\n",((WT_LOG_RECORD *) record->data)->mem_len,record->size, write_offset,record_size_in_blocks);
  //WT_CAPI_PRINT("!!!CAPILOG:stuff");
  if (rc==0) {
    WT_CAPI_PRINT("CAPILOG:writeBlock failed rc= %d\n",errno);
  }
  
  return (0);
}

void
__wt_capilog_saveContext()
{
  //acquire lock
  struct timespec start, end;
  uint64_t lat;
  clock_gettime(CLOCK_REALTIME, &start);
  pthread_spin_lock(&lock);
  context.l.file=active->unique_id;
  context.l.offset=active->offset;
  pthread_spin_unlock(&lock);
  clock_gettime(CLOCK_REALTIME, &end);
  lat = (end.tv_sec*1000000000ULL + end.tv_nsec) - (start.tv_sec*1000000000ULL + start.tv_nsec);
  WT_CAPI_PRINT("save context took %f ms\n",(float)lat/1000000.0);
  //release lock
//  return (0);
  
}


/*
* Remove the segment from the freelist
*/
void
__wt_capilog_discardFlushed()
{
  struct timespec start, end;
  uint64_t lat;
  clock_gettime(CLOCK_REALTIME, &start);
  pthread_spin_lock(&lock);
  checkpoint.l.file = context.l.file;
  checkpoint.l.offset = context.l.offset;

  context.l.file=active->unique_id;
  context.l.offset=active->offset;

  WT_CAPI_PRINT("checkpoint offset %d\n",checkpoint.l.file);
  WT_CAPI_PRINT("context offset %d\n",context.l.file);
  WT_CAPI_PRINT("before freelist size:%d\n",freelist->size);
  WT_CAPI_PRINT("before inuse size:%d\n",inUse->size);
  
  WT_CAPI_FREELIST_NODE *node = inUse->head; //retrive oldest active segment
  while(node!=NULL){
    if(node->item->unique_id < checkpoint.l.file){ 
      //if unique id of segment smaller than checkpoint offset
        WT_CAPI_FREELIST_NODE *nextnode = node->next; //if we are removing save the next node so that we can continue
        utility_buffer[0] = 0L;
        //delete segment from inUse list
        WT_CAPI_SEGMENT* seg = __wt_capi_deleteItem(inUse,node);
        //mark book-keeping area
        //int rc = cblk_write(utility_chunk,utility_buffer,WT_CAPI_START_BLOCK_ADDR+seg->bk_position, 1, 0); 
        //if (rc==0) 
        //{
        //  WT_CAPI_PRINT("CAPILOG:writeBlock failed rc= %d\n",errno);
        //}
       __wt_capi_insertBack(freelist,seg); // return segment to freelist
       node = nextnode;
       continue;
    }
    node=node->next;
  }
  WT_CAPI_PRINT("after freelist size:%d\n",freelist->size);
  WT_CAPI_PRINT("after inuse size:%d\n",inUse->size);
  pthread_spin_unlock(&lock);
  clock_gettime(CLOCK_REALTIME, &end);
  lat = (end.tv_sec*1000000000ULL + end.tv_nsec) - (start.tv_sec*1000000000ULL + start.tv_nsec);
  WT_CAPI_PRINT("discard flush took %f ms\n",(float)lat/1000000.0);

//  return (0);
}

/*
* Recovery
*/
int
__wt_capilog_recover()
{
  return (0);
}

/*Utility functions*/

bool 
__wt_capilog_hasCapacity(WT_CAPI_SEGMENT* segment, size_t blocks)
{
  return ((segment->offset+blocks) <= segment->max_offset);
}

bool
__wt_capilog_activateNextSegment()
{
  
  //get the next item from freelist
  if(__wt_capi_isEmpty(freelist))
  {
    return false;
  }

  WT_CAPI_SEGMENT* seg = __wt_capi_deleteFirst(freelist);
  __wt_capi_insertBack(inUse,seg);
  //set its unique id and reset the start offset
  seg->unique_id = epoch++;
  seg->offset = WT_CAPI_START_BLOCK_ADDR + WT_CAPI_NUMBER_OF_SEGMENTS + (seg->bk_position * WT_CAPI_NUMBER_OF_BLOCKS_PER_SEGMENT);
  seg->max_offset = seg->offset + WT_CAPI_NUMBER_OF_BLOCKS_PER_SEGMENT;
  utility_buffer[0] = seg->unique_id;

  sem_wait(&count_sem);         /* start of critical section */
  int rc = cblk_write(utility_chunk,utility_buffer,WT_CAPI_START_BLOCK_ADDR+seg->bk_position, 1, 0);
  sem_post(&count_sem);         /* end of critical section */

  WT_CAPI_PRINT("sizeof:%zu CAPILOG:Activating new segment bk position:%d - unique_id:%zu - totalBlocksWritten:%zu \n",sizeof(size_t),seg->bk_position,seg->unique_id,totalBlocksWritten);
  if (rc==0) {
   WT_CAPI_PRINT("CAPILOG:writeBlock failed rc= %d\n",errno);
  }
  active = seg;
  return true;
}
