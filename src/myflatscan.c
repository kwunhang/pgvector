#include "postgres.h"

#include <float.h>

#include "access/relscan.h"
#include "catalog/pg_operator_d.h"
#include "catalog/pg_type_d.h"
#include "lib/pairingheap.h"
#include "myflat.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "utils/memutils.h"


#define GetScanList(ptr) pairingheap_container(MyflatScanList, ph_node, ptr)


/*
 * Get lists and sort by distance
 */
static void
GetScanLists(IndexScanDesc scan, Datum value)
{
    MyflatScanOpaque so = (MyflatScanOpaque) scan->opaque;
    BlockNumber nextblkno = MYFLAT_HEAD_BLKNO;

    while (BlockNumberIsValid(nextblkno))
	{
		Buffer		cbuf;
		Page		cpage;
		OffsetNumber maxoffno;

        cbuf = ReadBuffer(scan->indexRelation, nextblkno);
		LockBuffer(cbuf, BUFFER_LOCK_SHARE);
		cpage = BufferGetPage(cbuf);

		maxoffno = PageGetMaxOffsetNumber(cpage);

        OffsetNumber offno = FirstOffsetNumber;

        MyflatScan scan = (MyflatScan) PageGetItem(cpage, PageGetItemId(cpage, offno));
        MyflatScanList *scanlist;

        scanlist = &so->lists[0];
        scanlist->startPage = scan->startPage;

        /* Add to heap */
        pairingheap_add(so->listQueue, &scanlist->ph_node);
        
        nextblkno = MyflatPageGetOpaque(cpage)->nextblkno;

        UnlockReleaseBuffer(cbuf);
    }

    so->listPages[0] = GetScanList(pairingheap_remove_first(so->listQueue))->startPage;

    Assert(pairingheap_is_empty(so->listQueue));

}


/*
 * Get items
 */
static void
GetScanItems(IndexScanDesc scan, Datum value)
{
    MyflatScanOpaque so = (MyflatScanOpaque) scan->opaque;
    TupleDesc	tupdesc = RelationGetDescr(scan->indexRelation);
    TupleTableSlot *slot = so->vslot;
    int			batchProbes = 0;

    tuplesort_reset(so->sortstate);

    BlockNumber searchPage = so->listPages[0];
    /* Search all entry pages  */
    while (BlockNumberIsValid(searchPage))
    {
        Buffer		buf;
        Page		page;
        OffsetNumber maxoffno;

        buf = ReadBufferExtended(scan->indexRelation, MAIN_FORKNUM, searchPage, RBM_NORMAL, so->bas);
        LockBuffer(buf, BUFFER_LOCK_SHARE);
        page = BufferGetPage(buf);
        maxoffno = PageGetMaxOffsetNumber(page);

        for (OffsetNumber offno = FirstOffsetNumber; offno <= maxoffno; offno = OffsetNumberNext(offno))
        {
            IndexTuple	itup;
            Datum		datum;
            bool		isnull;
            ItemId		itemid = PageGetItemId(page, offno);

            itup = (IndexTuple) PageGetItem(page, itemid);
            datum = index_getattr(itup, 1, tupdesc, &isnull);

            /*
            * Add virtual tuple
            *
            * Use procinfo from the index instead of scan key for
            * performance
            */
            ExecClearTuple(slot);
            slot->tts_values[0] = so->distfunc(so->procinfo, so->collation, datum, value);
            slot->tts_isnull[0] = false;
            slot->tts_values[1] = PointerGetDatum(&itup->t_tid);
            slot->tts_isnull[1] = false;
            ExecStoreVirtualTuple(slot);

            tuplesort_puttupleslot(so->sortstate, slot);
        }

        searchPage = MyflatPageGetOpaque(page)->nextblkno;

        UnlockReleaseBuffer(buf);
    }
    
    tuplesort_performsort(so->sortstate);

// #if defined(MYFLAT_MEMORY)
// 	elog(INFO, "memory: %zu MB", MemoryContextMemAllocated(CurrentMemoryContext, true) / (1024 * 1024));
// #endif
}


/*
 * Get scan value
 */
static Datum
GetScanValue(IndexScanDesc scan)
{
	MyflatScanOpaque so = (MyflatScanOpaque) scan->opaque;
	Datum		value;

	if (scan->orderByData->sk_flags & SK_ISNULL)
	{
		value = PointerGetDatum(NULL);
		so->distfunc = ZeroDistance;
	}
	else
	{
		value = scan->orderByData->sk_argument;
		so->distfunc = FunctionCall2Coll;

		/* Value should not be compressed or toasted */
		Assert(!VARATT_IS_COMPRESSED(DatumGetPointer(value)));
		Assert(!VARATT_IS_EXTENDED(DatumGetPointer(value)));

		/* Normalize if needed */
		if (so->normprocinfo != NULL)
		{
			MemoryContext oldCtx = MemoryContextSwitchTo(so->tmpCtx);

			value = MyflatNormValue(so->typeInfo, so->collation, value);

			MemoryContextSwitchTo(oldCtx);
		}
	}

	return value;
}

/*
 * Initialize scan sort state
 */
static Tuplesortstate *
InitScanSortState(TupleDesc tupdesc)
{
	AttrNumber	attNums[] = {1};
	Oid			sortOperators[] = {Float8LessOperator};
	Oid			sortCollations[] = {InvalidOid};
	bool		nullsFirstFlags[] = {false};

	return tuplesort_begin_heap(tupdesc, 1, attNums, sortOperators, sortCollations, nullsFirstFlags, work_mem, NULL, false);
}


/*
 * Prepare for an index scan
 */
IndexScanDesc
myflatbeginscan(Relation index, int nkeys, int norderbys)
{
    IndexScanDesc scan;
    MyflatScanOpaque so;
    int			unused;
	int			dimensions;
	MemoryContext oldCtx;

    scan = RelationGetIndexScan(index, nkeys, norderbys);

    /* Get unused and dimensions from metapage */
    MyflatGetMetaPageInfo(index, &unused, &dimensions);

    so = (MyflatScanOpaque) palloc(sizeof(MyflatScanOpaqueData));
    so->typeInfo = MyflatGetTypeInfo(index);
    so->first = true;
    so->dimensions = dimensions;


    so->procinfo = index_getprocinfo(index, 1, MYFLAT_DISTANCE_PROC);
	so->normprocinfo = MyflatOptionalProcInfo(index, MYFLAT_NORM_PROC);
	so->collation = index->rd_indcollation[0];

    so->tmpCtx = AllocSetContextCreate(CurrentMemoryContext,
									   "Myflat scan temporary context",
									   ALLOCSET_DEFAULT_SIZES);

	oldCtx = MemoryContextSwitchTo(so->tmpCtx);

    /* Create tuple description for sorting */
	so->tupdesc = CreateTemplateTupleDesc(2);
	TupleDescInitEntry(so->tupdesc, (AttrNumber) 1, "distance", FLOAT8OID, -1, 0);
	TupleDescInitEntry(so->tupdesc, (AttrNumber) 2, "heaptid", TIDOID, -1, 0);

    /* Prep sort */
	so->sortstate = InitScanSortState(so->tupdesc);

	/* Need separate slots for puttuple and gettuple */
	so->vslot = MakeSingleTupleTableSlot(so->tupdesc, &TTSOpsVirtual);
	so->mslot = MakeSingleTupleTableSlot(so->tupdesc, &TTSOpsMinimalTuple);

    /*
	 * Reuse same set of shared buffers for scan
	 *
	 * See postgres/src/backend/storage/buffer/README for description
	 */
	so->bas = GetAccessStrategy(BAS_BULKREAD);


// TODO: check usage of these
	so->listQueue = pairingheap_allocate(CompareLists, scan);
	so->listPages = palloc(sizeof(BlockNumber));
	so->listIndex = 0;
	so->lists = palloc(sizeof(MyflatScanList));
// 

	MemoryContextSwitchTo(oldCtx);

	scan->opaque = so;

	return scan;

}

/*
 * Start or restart an index scan
 */
void
myflatrescan(IndexScanDesc scan, ScanKey keys, int nkeys, ScanKey orderbys, int norderbys)
{
    MyflatScanOpaque so = (MyflatScanOpaque) scan->opaque;

    so->first = true;
    pairingheap_reset(so->listQueue);
    so->listIndex = 0;

	if (keys && scan->numberOfKeys > 0)
		memmove(scan->keyData, keys, scan->numberOfKeys * sizeof(ScanKeyData));

	if (orderbys && scan->numberOfOrderBys > 0)
		memmove(scan->orderByData, orderbys, scan->numberOfOrderBys * sizeof(ScanKeyData));
}

// TODO
/*
 * Fetch the next tuple in the given scan
 */
bool
myflatgettuple(IndexScanDesc scan, ScanDirection dir)
{
    MyflatScanOpaque so = (MyflatScanOpaque) scan->opaque;
    ItemPointer heaptid;
    bool        isnull;

    /*
	 * Index can be used to scan backward, but Postgres doesn't support
	 * backward scan on operators
	 */
    Assert(ScanDirectionIsForward(dir));

    if (so->first)
	{
		Datum		value;

		/* Count index scan for stats */
		pgstat_count_index_scan(scan->indexRelation);

		/* Safety check */
		if (scan->orderByData == NULL)
			elog(ERROR, "cannot scan myflat index without order");

		/* Requires MVCC-compliant snapshot as not able to pin during sorting */
		/* https://www.postgresql.org/docs/current/index-locking.html */
		if (!IsMVCCSnapshot(scan->xs_snapshot))
			elog(ERROR, "non-MVCC snapshots are not supported with myflat");

		value = GetScanValue(scan);
		MyflatBench("GetScanLists", GetScanLists(scan, value));
		MyflatBench("GetScanItems", GetScanItems(scan, value));
		so->first = false;
		so->value = value;
	}

    while (!tuplesort_gettupleslot(so->sortstate, true, false, so->mslot, NULL))
	{
		MyflatBench("GetScanItems", GetScanItems(scan, so->value));
	}

	heaptid = (ItemPointer) DatumGetPointer(slot_getattr(so->mslot, 2, &isnull));

	scan->xs_heaptid = *heaptid;
	scan->xs_recheck = false;
	scan->xs_recheckorderby = false;
	return true;
}

/*
 * End a scan and release resources
 */
void
myflatendscan(IndexScanDesc scan)
{
    MyflatScanOpaque so = (MyflatScanOpaque) scan->opaque;

	/* Free any temporary files */
	tuplesort_end(so->sortstate);

	MemoryContextDelete(so->tmpCtx);

	pfree(so);
	scan->opaque = NULL;
}