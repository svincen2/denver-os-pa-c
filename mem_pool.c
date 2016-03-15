/*
 * Created by Ivo Georgiev on 2/9/16.
 */

#include <stdlib.h>
#include <assert.h>
#include <stdio.h> // for perror()

#include "mem_pool.h"

/*************/
/*           */
/* Constants */
/*           */
/*************/
static const float      MEM_FILL_FACTOR                 = 0.75;
static const unsigned   MEM_EXPAND_FACTOR               = 2;

static const unsigned   MEM_POOL_STORE_INIT_CAPACITY    = 20;
static const float      MEM_POOL_STORE_FILL_FACTOR      = 0.75;
static const unsigned   MEM_POOL_STORE_EXPAND_FACTOR    = 2;

static const unsigned   MEM_NODE_HEAP_INIT_CAPACITY     = 40;
static const float      MEM_NODE_HEAP_FILL_FACTOR       = 0.75;
static const unsigned   MEM_NODE_HEAP_EXPAND_FACTOR     = 2;

static const unsigned   MEM_GAP_IX_INIT_CAPACITY        = 40;
static const float      MEM_GAP_IX_FILL_FACTOR          = 0.75;
static const unsigned   MEM_GAP_IX_EXPAND_FACTOR        = 2;


/*********************/
/*                   */
/* Type declarations */
/*                   */
/*********************/
typedef struct _node {
    alloc_t alloc_record;
    unsigned used;
    unsigned allocated;
    struct _node *next, *prev; // doubly-linked list for gap deletion
} node_t, *node_pt;


typedef struct _gap {
    size_t size;
    node_pt node;
} gap_t, *gap_pt;


typedef struct _pool_mgr {
    pool_t pool;
    node_pt node_heap;
    unsigned total_nodes;
    unsigned used_nodes;
    gap_pt gap_ix;
    unsigned gap_ix_capacity;
} pool_mgr_t, *pool_mgr_pt;



/***************************/
/*                         */
/* Static global variables */
/*                         */
/***************************/
static pool_mgr_pt *pool_store = NULL; // an array of pointers, only expand
static unsigned pool_store_size = 0;
static unsigned pool_store_capacity = 0;



/********************************************/
/*                                          */
/* Forward declarations of static functions */
/*                                          */
/********************************************/
static alloc_status _mem_resize_pool_store();
static alloc_status _mem_resize_node_heap(pool_mgr_pt pool_mgr);
static alloc_status _mem_resize_gap_ix(pool_mgr_pt pool_mgr);
static alloc_status
        _mem_add_to_gap_ix(pool_mgr_pt pool_mgr,
                           size_t size,
                           node_pt node);
static alloc_status
        _mem_remove_from_gap_ix(pool_mgr_pt pool_mgr,
                                size_t size,
                                node_pt node);
static alloc_status _mem_sort_gap_ix(pool_mgr_pt pool_mgr);


// My functions.
static void _mem_new_node_heap(node_pt*, unsigned int, pool_pt);
static void _mem_new_pool(pool_pt, unsigned int, alloc_policy);
static void _mem_new_gap_ix(gap_pt*, node_pt);
static void _init_node(node_pt);
static unsigned _all_pool_mgr_freed();
static void _set_pool_mgr_to_null(pool_mgr_pt);
static node_pt _find_first_fit_node(pool_mgr_pt, size_t);
static node_pt _find_best_fit_node(pool_mgr_pt, size_t);
static node_pt _find_unused_node(pool_mgr_pt);


/****************************************/
/*                                      */
/* Definitions of user-facing functions */
/*                                      */
/****************************************/
alloc_status mem_init() {

    // ensure that it's called only once until mem_free
    if(pool_store) return ALLOC_CALLED_AGAIN;

    // allocate the pool store with initial capacity
    // note: holds pointers only, other functions to allocate/deallocate
    pool_store = (pool_mgr_pt*) calloc(
        MEM_POOL_STORE_INIT_CAPACITY, sizeof(pool_mgr_pt));

    pool_store_capacity = MEM_POOL_STORE_INIT_CAPACITY;
    pool_store_size = 0;
    return ALLOC_OK;
}


alloc_status mem_free() {
    // ensure that it's called only once for each mem_init
    if (!pool_store) return ALLOC_CALLED_AGAIN;

    // make sure all pool managers have been deallocated
    if (!_all_pool_mgr_freed()) return ALLOC_FAIL;

    // can free the pool store array
    free(pool_store);

    // update static variables
    pool_store_size = 0;
    pool_store_capacity = 0;
    pool_store = NULL;

    return ALLOC_OK;
}


// Checks if all pool managers have been deallocated.
static unsigned _all_pool_mgr_freed()
{
    for (int i = 0; i < pool_store_size; ++i)
    {
        if (pool_store[i])
        {
            return 0; // False.
        }
    }
    return 1; // True.
}


pool_pt mem_pool_open(size_t size, alloc_policy policy) {

    // make sure there the pool store is allocated
    if (!pool_store) return NULL;

    // expand the pool store, if necessary
    _mem_resize_pool_store();

    // allocate a new mem pool mgr
    // check success, on error return null.
    pool_mgr_pt new_pool_mgr = calloc(1, sizeof(pool_mgr_t));
    if (!new_pool_mgr) return NULL;
    
    // allocate a new memory pool
    // check success, on error deallocate mgr and return null
    _mem_new_pool(&new_pool_mgr->pool, size, policy);
    if (!(&new_pool_mgr->pool))
    {
        free(new_pool_mgr);
        return NULL;
    }

    // allocate a new node heap
    // check success, on error deallocate mgr/pool and return null.
    _mem_new_node_heap(&new_pool_mgr->node_heap, size, &new_pool_mgr->pool);
    if (!new_pool_mgr->node_heap)
    {
        free(&new_pool_mgr->pool);
        free(new_pool_mgr);
        return NULL;
    }

    // allocate a new gap index
    // check success, on error deallocate mgr/pool/heap and return null.
    _mem_new_gap_ix(&new_pool_mgr->gap_ix, new_pool_mgr->node_heap);
    if (!new_pool_mgr->gap_ix)
    {
        free(new_pool_mgr->node_heap);
        free(&new_pool_mgr->pool);
        free(new_pool_mgr);
        return NULL;
    }
    
    
    // assign all the pointers and update meta data:
    //   initialize top node of node heap        **DONE**
    //   initialize top node of gap index        **DONE**
    //   initialize pool mgr                    **DONE**
    //   link pool mgr to pool store            **DONE**
    // return the address of the mgr, cast to (pool_pt)
    new_pool_mgr->used_nodes = 1;    // One gap when first initialized.
    new_pool_mgr->total_nodes = MEM_NODE_HEAP_INIT_CAPACITY;
    new_pool_mgr->gap_ix_capacity = MEM_GAP_IX_INIT_CAPACITY;
    pool_store[pool_store_size++] = new_pool_mgr;
    return (pool_pt) new_pool_mgr;
}


void _mem_new_pool(pool_pt pool, unsigned int size, alloc_policy policy)
{    
    pool->mem = (char*) calloc(size, sizeof(char));
    pool->policy = policy;
    pool->total_size = size;
    pool->alloc_size = 0;
    pool->num_allocs = 0;
    pool->num_gaps = 1;
}


void _mem_new_node_heap(node_pt *node_heap, unsigned size, pool_pt pool)
{
    *node_heap = (node_pt) calloc(MEM_NODE_HEAP_INIT_CAPACITY, sizeof(node_t));
    node_pt top_node = *node_heap;
    top_node->alloc_record.size = size;
    top_node->alloc_record.mem = pool->mem;
    top_node->used = 1;
    top_node->allocated = 0;
}


void _mem_new_gap_ix(gap_pt *gap_ix, node_pt node)
{
    *gap_ix = (gap_pt) calloc(MEM_GAP_IX_INIT_CAPACITY, sizeof(gap_t));
    gap_pt top_gap = *gap_ix;
    top_gap[0].size = node->alloc_record.size;
    top_gap[0].node = node;
}


alloc_status mem_pool_close(pool_pt pool) {

    // check if this pool is allocated
    if (!pool) return ALLOC_NOT_FREED;

    // get mgr from pool by casting the pointer to (pool_mgr_pt)
    pool_mgr_pt pool_manager = (pool_mgr_pt) pool;

    // check if pool has only one gap
    if (!pool->num_gaps == 1) return ALLOC_NOT_FREED;

    // check if it has zero allocations
    if (!pool->num_allocs == 0) return ALLOC_NOT_FREED;

    // free memory pool
    free(pool->mem);

    // free node heap
    free(pool_manager->node_heap);
    pool_manager->node_heap = NULL;

    // free gap index
    free(pool_manager->gap_ix);
    pool_manager->gap_ix = NULL;

    // find mgr in pool store and set to null
    _set_pool_mgr_to_null(pool_manager);

    // note: don't decrement pool_store_size, because it only grows
    // free mgr
    free(pool_manager);

    return ALLOC_OK;
}


static void _set_pool_mgr_to_null(pool_mgr_pt pool_mgr)
{

    for (int i = 0; i < pool_store_capacity; ++i)
    {
        if (pool_store[i] == pool_mgr)
        {
            pool_store[i] = NULL;
            break;
        }
    }
}


alloc_pt mem_new_alloc(pool_pt pool, size_t size) {

    // check if any gaps, return null if none
    if (pool->num_gaps == 0) return NULL;

    // get mgr from pool by casting the pointer to (pool_mgr_pt)
    pool_mgr_pt pool_manager = (pool_mgr_pt) pool;

    // check used nodes fewer than total nodes, quit on error
    if (pool_manager->used_nodes >= pool_manager->total_nodes) return NULL;

    // expand node heap, if necessary, quit on error
    _mem_resize_node_heap(pool_manager);

    // get a node for allocation:
    node_pt node = NULL;

    // if FIRST_FIT, then find the first sufficient node in the node heap
    if (pool->policy == FIRST_FIT)
    {
        node = _find_first_fit_node(pool_manager, size);
    }
    // if BEST_FIT, then find the first sufficient node in the gap index
    else if (pool->policy == BEST_FIT)
    {
        node = _find_best_fit_node(pool_manager, size);
    }

    // check if node found
    if (node == NULL) return NULL;

    // update metadata (num_allocs, alloc_size)
    pool->num_allocs++;
    pool->alloc_size += size;

    // calculate the size of the remaining gap, if any
    unsigned remaining = node->alloc_record.size - size;

    // remove node from gap index
    _mem_remove_from_gap_ix(pool_manager, size, node);

    // convert gap_node to an allocation node of given size
    node->allocated = 1;
    node->alloc_record.size = size;

    // adjust node heap:
    //   if remaining gap, need a new node
    if (remaining)
    {
        // find an unused one in the node heap
        node_pt unused_node = _find_unused_node(pool_manager);
        
        // make sure one was found
        assert(unused_node);

        // initialize it to a gap node
        unused_node->alloc_record.size = remaining;
        unused_node->alloc_record.mem = node->alloc_record.mem + size;
        unused_node->allocated = 0;
        unused_node->used = 1;

        // update linked list (new node right after the node for allocation)
        unused_node->prev = node;
        unused_node->next = node->next;
        if(node->next)
        {
            node->next->prev = unused_node;
        }
        node->next = unused_node;

        // add to gap index
        _mem_add_to_gap_ix(pool_manager, remaining, unused_node);

        // check if successful  ??

        pool_manager->used_nodes++;
    }
    // return allocation record by casting the node to (alloc_pt)
    return (alloc_pt) node;
}


// Finds the first node in the node heap with enough size.
static node_pt _find_first_fit_node(pool_mgr_pt pool_mgr, size_t size)
{
    node_pt node = NULL;
    for (int i = 0; i < pool_mgr->total_nodes; ++i)
    {
        node = &pool_mgr->node_heap[i];
        if (node->used && !node->allocated && 
           node->alloc_record.size >= size)
        {
            break;
        }
    }
    return (node->alloc_record.size >= size ? node : NULL);
}


// Finds the best fit node for the given size, in the given pool (manager).
static node_pt _find_best_fit_node(pool_mgr_pt pool_mgr, size_t size)
{
    node_pt node = NULL;
    gap_pt gap_index = pool_mgr->gap_ix;
    for (int i = 0; i < pool_mgr->gap_ix_capacity; ++i)
    {
        if (gap_index[i].size >= size)
        {
            node = gap_index[i].node;
            break;
        }
    }
    return node;
}


static node_pt _find_unused_node(pool_mgr_pt pool_mgr)
{
    node_pt node = NULL;
    for (int i = 0; i < pool_mgr->total_nodes; ++i)
    {
        if (!pool_mgr->node_heap[i].used)
        {
            node = &pool_mgr->node_heap[i];
            break;
        }
    }
    return node;
}


alloc_status mem_del_alloc(pool_pt pool, alloc_pt alloc) {
    // get mgr from pool by casting the pointer to (pool_mgr_pt)
    pool_mgr_pt pool_mgr = (pool_mgr_pt) pool;

    // get node from alloc by casting the pointer to (node_pt)
    node_pt node_to_delete = (node_pt) alloc;

    // convert to gap node
    node_to_delete->allocated = 0;

    // update metadata (num_allocs, alloc_size)
    pool->num_allocs--;
    pool->alloc_size -= alloc->size;

    // if the next node in the list is also a gap, merge into node-to-delete
    if (node_to_delete->next && !node_to_delete->next->allocated)
    {
        // add the size to the node-to-delete
        alloc->size += node_to_delete->next->alloc_record.size;

        // remove the next node from gap index
        _mem_remove_from_gap_ix(pool_mgr,
                                node_to_delete->next->alloc_record.size,
                                node_to_delete->next);

        // update node as unused
        node_to_delete->next->used = 0;
        node_to_delete->next->alloc_record.size = 0;
        node_to_delete->next->alloc_record.mem = NULL;

        // update metadata (used nodes)
        pool_mgr->used_nodes--;

        // update linked list:
        if (node_to_delete->next->next)
        {
            node_to_delete->next->next->prev = node_to_delete;
            node_to_delete->next = node_to_delete->next->next;
        }
        else
        {
            node_to_delete->next->prev = NULL;
            node_to_delete->next = NULL;
        }
    }

    // this merged node-to-delete might need to be added to the gap index
    // but one more thing to check...
    // if the previous node in the list is also a gap, merge into previous!
    if (node_to_delete->prev && !node_to_delete->prev->allocated)
    {
        node_pt previous = node_to_delete->prev;

        // remove the previous node from gap index
        alloc_status status =
            _mem_remove_from_gap_ix(pool_mgr,
                                    previous->alloc_record.size,
                                    previous);
        // check success
        if (status == ALLOC_FAIL) return status;

        // add the size of node-to-delete to the previous
        previous->alloc_record.size += alloc->size;

        // update node-to-delete as unused
        node_to_delete->used = 0;
        node_to_delete->alloc_record.size = 0;
        node_to_delete->alloc_record.mem = NULL;

        // update metadata (used_nodes)
        pool_mgr->used_nodes--;

        // update linked list
        if (node_to_delete->next)
        {
            previous->next = node_to_delete->next;
            node_to_delete->next->prev = previous;
        }
        else
        {
            previous->next = NULL;
        }
        node_to_delete->next = NULL;
        node_to_delete->prev = NULL;

        // change the node to add to the previous node!
        node_to_delete = previous;
    }

    // add the resulting node to the gap index
    _mem_add_to_gap_ix(pool_mgr,
                       node_to_delete->alloc_record.size,
                       node_to_delete);

    return ALLOC_OK;
}


void mem_inspect_pool(pool_pt pool,
                      pool_segment_pt *segments,
                      unsigned *num_segments) {

    // get the mgr from the pool
    pool_mgr_pt pool_mgr = (pool_mgr_pt) pool;

    // allocate the segments array with size == used_nodes
    pool_segment_pt segs = 
        (pool_segment_pt)calloc(pool_mgr->used_nodes, sizeof(pool_segment_t));

    // check successful
    if (!segs) return;

    // loop through the node heap and the segments array
    //    for each node, write the size and allocated in the segment
    node_pt node = pool_mgr->node_heap;
    int next = 0;
    for (; node; node = node->next)
    {
        segs[next].size = node->alloc_record.size;
        segs[next].allocated = node->allocated;
        ++next;
    }
    *num_segments = pool_mgr->used_nodes;
    *segments = segs;
}



/***********************************/
/*                                 */
/* Definitions of static functions */
/*                                 */
/***********************************/
static alloc_status _mem_resize_pool_store() {
    // check if necessary
    float fill_factor = (float)pool_store_size / pool_store_capacity;
    if (fill_factor > MEM_POOL_STORE_FILL_FACTOR)
    {
        unsigned new_cap = pool_store_capacity * MEM_POOL_STORE_EXPAND_FACTOR;
        pool_store = (pool_mgr_pt*)
            realloc(pool_store, sizeof(pool_mgr_pt) * new_cap);
            
        for (int i = pool_store_size; i < new_cap; ++i)
        {
            pool_store[i] = NULL;
        }
        pool_store_capacity = new_cap;
    }
    return ALLOC_OK;
}


static alloc_status _mem_resize_node_heap(pool_mgr_pt pool_mgr) {
    
    float fill_factor = (float) pool_mgr->used_nodes / pool_mgr->total_nodes;
    if (fill_factor > MEM_NODE_HEAP_FILL_FACTOR)
    {
        unsigned new_cap = pool_mgr->total_nodes * MEM_NODE_HEAP_EXPAND_FACTOR;

        pool_mgr->node_heap =
            (node_pt) realloc(pool_mgr->node_heap, sizeof(node_t) * new_cap);

        for (int i = pool_mgr->total_nodes; i < new_cap; ++i)
        {
            node_pt node = &pool_mgr->node_heap[i];
            _init_node(node);
        }
        pool_mgr->total_nodes = new_cap;
    }
    return ALLOC_OK;
}


// Initialize the given node with all zeros or NULLs.
static void _init_node(node_pt node)
{
    node->used = 0;
    node->allocated = 0;
    node->next = NULL;
    node->prev = NULL;
    node->alloc_record.size = 0;
    node->alloc_record.mem = NULL;
}


static alloc_status _mem_resize_gap_ix(pool_mgr_pt pool_mgr) {
    
    float fill_factor =
        (float) pool_mgr->pool.num_gaps / pool_mgr->gap_ix_capacity;
        
    if (fill_factor > MEM_GAP_IX_FILL_FACTOR)
    {
        unsigned new_cap = pool_mgr->gap_ix_capacity * MEM_GAP_IX_EXPAND_FACTOR;
        pool_mgr->gap_ix =
            (gap_pt) realloc(pool_mgr->gap_ix, sizeof(gap_t) * new_cap);

        for (int i = pool_mgr->pool.num_gaps; i < new_cap; ++i)
        {
            pool_mgr->gap_ix[i].size = 0;
            pool_mgr->gap_ix[i].node = NULL;
        }
        pool_mgr->gap_ix_capacity = new_cap;
    }
    return ALLOC_OK;
}


static alloc_status _mem_add_to_gap_ix(pool_mgr_pt pool_mgr,
                                       size_t size,
                                       node_pt node) {

    // expand the gap index, if necessary (call the function)
    _mem_resize_gap_ix(pool_mgr);

    // add the entry at the end
    gap_pt gap = &pool_mgr->gap_ix[pool_mgr->pool.num_gaps];
    gap->size = size;
    gap->node = node;

    // update metadata (num_gaps)
    pool_mgr->pool.num_gaps++;

    // sort the gap index (call the function)
    alloc_status status = _mem_sort_gap_ix(pool_mgr);

    if (status == ALLOC_FAIL) return status;
    return ALLOC_OK;
}


static alloc_status _mem_remove_from_gap_ix(pool_mgr_pt pool_mgr,
                                            size_t size,
                                            node_pt node) {
    // find the position of the node in the gap index
    int index = -1;
    for (int i = 0; i < pool_mgr->gap_ix_capacity; ++i)
    {
        if (pool_mgr->gap_ix[i].node == node)
        {
            index = i;
            break;
        }
    }

    if (index < 0) return ALLOC_FAIL;

    // loop from there to the end of the array:
    for (int i = index; i < pool_mgr->gap_ix_capacity - 1; ++i)
    {
        // pull the entries (i.e. copy over) one position up
        // this effectively deletes the chosen node
        pool_mgr->gap_ix[i] = pool_mgr->gap_ix[i + 1];
    }

    // update metadata (num_gaps)
    pool_mgr->pool.num_gaps--;

    // zero out the element at position num_gaps!
    gap_pt last = &pool_mgr->gap_ix[pool_mgr->pool.num_gaps];
    last->size = 0;
    last->node = NULL;
    return ALLOC_OK;
}


// note: only called by _mem_add_to_gap_ix, which appends a single entry
static alloc_status _mem_sort_gap_ix(pool_mgr_pt pool_mgr) {
    // the new entry is at the end, so "bubble it up"
    // loop from num_gaps - 1 until but not including 0:
    for (int i = pool_mgr->pool.num_gaps - 1; i > 0; --i)
    {
        // if the size of the current entry is less than the previous (u - 1)
        if(pool_mgr->gap_ix[i].size < pool_mgr->gap_ix[i - 1].size)
        {
            // swap them (by copying) (remember to use a temporary variable)
            gap_t temp = pool_mgr->gap_ix[i];
            pool_mgr->gap_ix[i] = pool_mgr->gap_ix[i - 1];
            pool_mgr->gap_ix[i - 1] = temp;
        }
        else if(pool_mgr->gap_ix[i].size == pool_mgr->gap_ix[i - 1].size)
        {
            // compare memory addresses.
            if(pool_mgr->gap_ix[i].node->alloc_record.mem < pool_mgr->gap_ix[i - 1].node->alloc_record.mem)
            {
                gap_t temp = pool_mgr->gap_ix[i];
                pool_mgr->gap_ix[i] = pool_mgr->gap_ix[i - 1];
                pool_mgr->gap_ix[i - 1] = temp;
            }
        }
    }
    return ALLOC_OK;
}
