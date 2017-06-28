#define	WT_CAPI_NUMBER_OF_CHUNKS	32
#define	WT_CAPI_BLOCK_SIZE	4096
#define	WT_CAPI_START_BLOCK_ADDR	0
#define	WT_CAPI_NUMBER_OF_ASYNC_PER_CHUNK	256
#define	WT_CAPI_NUMBER_OF_BLOCKS_PER_SEGMENT	128000
#define	WT_CAPI_NUMBER_OF_SEGMENTS	512
#define	WT_CAPI_OUTSTANDING_WRITES 128

#define	WT_CAPI_DEBUG	1
#define WT_CAPI_PRINT(...) \
            do { if (WT_CAPI_DEBUG) fprintf(stderr, ##__VA_ARGS__); } while (0)
            
struct __wt_capi_freelist_node{
	WT_CAPI_SEGMENT *item;
	struct __wt_capi_freelist_node *next;
	struct __wt_capi_freelist_node *prev;
};

struct __wt_capi_freelist{
	WT_CAPI_FREELIST_NODE *head;
	WT_CAPI_FREELIST_NODE *tail;
	int size;
};

struct __wt_capi_segment {
	size_t offset; /*current offset of the segment*/
	size_t unique_id; /*Unique id that represents this segment*/
	int bk_position; /*Position in book-keeping area*/
	size_t max_offset;
};