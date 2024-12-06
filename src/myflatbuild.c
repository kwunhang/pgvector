#include "postgres.h"

#include <float.h>

#include "access/table.h"
#include "access/tableam.h"
#include "access/parallel.h"
#include "access/xact.h"
#include "bitvec.h"
#include "catalog/index.h"
#include "catalog/pg_operator_d.h"
#include "catalog/pg_type_d.h"
#include "commands/progress.h"
#include "halfvec.h"
#include "myflat.h"
#include "miscadmin.h"
#include "optimizer/optimizer.h"
#include "storage/bufmgr.h"
#include "tcop/tcopprot.h"
#include "utils/memutils.h"
#include "vector.h"

#if PG_VERSION_NUM >= 140000
#include "utils/backend_progress.h"
#else
#include "pgstat.h"
#endif

#if PG_VERSION_NUM >= 140000
#include "utils/backend_status.h"
#include "utils/wait_event.h"
#endif

// #define PARALLEL_KEY_IVFFLAT_SHARED		UINT64CONST(0xA000000000000001)
// #define PARALLEL_KEY_TUPLESORT			UINT64CONST(0xA000000000000002)
// #define PARALLEL_KEY_IVFFLAT_CENTERS	UINT64CONST(0xA000000000000003)
// #define PARALLEL_KEY_QUERY_TEXT			UINT64CONST(0xA000000000000004)

/*
 * Add tuple to sort
 */
static void
AddTuple(Relation index, ItemPointer tid, Datum *values, MyflatBuildState * buildstate)
{

	TupleTableSlot *slot = buildstate->slot;
	/* Detoast once for all calls */
	Datum		value = PointerGetDatum(PG_DETOAST_DATUM(values[0]));

#ifdef MYFLAT_DEBUG

#endif

	/* Create a virtual tuple */
    ExecClearTuple(slot);
    
    /* Store TID in first column */
	slot->tts_values[0] = Int32GetDatum(0);
	slot->tts_isnull[0] = false;
    slot->tts_values[1] = PointerGetDatum(tid);
	slot->tts_isnull[1] = false;
	slot->tts_values[2] = value;
	slot->tts_isnull[2] = false;
    
    ExecStoreVirtualTuple(slot);

	/*
	 * Add tuple to sort
	 *
	 * tuplesort_puttupleslot comment: Input data is always copied; the caller
	 * need not save it.
	 */
	tuplesort_puttupleslot(buildstate->sortstate, slot);

	buildstate->indtuples++;
}


/*
 * Callback for table_index_build_scan
 */
static void
BuildCallback(Relation index, ItemPointer tid, Datum *values,
			  bool *isnull, bool tupleIsAlive, void *state)
{
	MyflatBuildState *buildstate = (MyflatBuildState *) state;
	MemoryContext oldCtx;

	/* Skip nulls */
	if (isnull[0])
		return;

	/* Use memory context since detoast can allocate */
	oldCtx = MemoryContextSwitchTo(buildstate->tmpCtx);

	/* Add tuple to sort */
	AddTuple(index, tid, values, buildstate);

	/* Reset memory context */
	MemoryContextSwitchTo(oldCtx);
	MemoryContextReset(buildstate->tmpCtx);
}

/*
 * Get next tuple from sort state
 */
static void
GetNextTuple(Tuplesortstate *sortstate, TupleDesc tupdesc, TupleTableSlot *slot, IndexTuple *itup)
{
    if (tuplesort_gettupleslot(sortstate, true, false, slot, NULL))
    {
		Datum       value;
		bool        isnull;

        /* Get vector from slot (second column) */
        value = slot_getattr(slot, 3, &isnull);
        
        /* Form the index tuple */
        *itup = index_form_tuple(tupdesc, &value, &isnull);
		(*itup)->t_tid = *((ItemPointer) DatumGetPointer(slot_getattr(slot, 2, &isnull)));
    }
    else
    {
        *itup = NULL;
    }
}

/*
 * Create initial entry pages
 */
static void
InsertTuples(Relation index, MyflatBuildState * buildstate, ForkNumber forkNum)
{
	IndexTuple  itup = NULL;
    int64       inserted = 0;

    TupleTableSlot *slot = MakeSingleTupleTableSlot(buildstate->sortdesc, &TTSOpsMinimalTuple);
    TupleDesc   tupdesc = buildstate->tupdesc;

    /* Update progress */
    pgstat_progress_update_param(PROGRESS_CREATEIDX_SUBPHASE, PROGRESS_MYFLAT_PHASE_LOAD);  // Loading tuples

    pgstat_progress_update_param(PROGRESS_CREATEIDX_TUPLES_TOTAL, buildstate->indtuples);

    /* Get first tuple */
    GetNextTuple(buildstate->sortstate, tupdesc, slot, &itup);

	int i = 0;

    Buffer      buf;
    Page        page;
    GenericXLogState *state;
	BlockNumber startPage;
	BlockNumber insertPage;


	/* Can take a while, so ensure we can interrupt */
	/* Needs to be called when no buffer locks are held */
	CHECK_FOR_INTERRUPTS();

	/* Create first page */
    buf = MyflatNewBuffer(index, forkNum);
    MyflatInitRegisterPage(index, &buf, &page, &state);

	startPage = BufferGetBlockNumber(buf);

    /* Add all tuples */
    while (itup != NULL)
    {
        /* Check for free space */
        Size    itemsz = MAXALIGN(IndexTupleSize(itup));

        if (PageGetFreeSpace(page) < itemsz)
            MyflatAppendPage(index, &buf, &page, &state, forkNum);

        /* Add the item */
        if (PageAddItem(page, (Item) itup, itemsz, InvalidOffsetNumber, false, false) == InvalidOffsetNumber)
            elog(ERROR, "failed to add index item to \"%s\"", RelationGetRelationName(index));

        pfree(itup);

        /* Update progress */
        pgstat_progress_update_param(PROGRESS_CREATEIDX_TUPLES_DONE, ++inserted);

        /* Get next tuple */
        GetNextTuple(buildstate->sortstate, tupdesc, slot, &itup);
    }

	insertPage = BufferGetBlockNumber(buf);

    /* Commit last page */
    MyflatCommitBuffer(buf, state);

    /* Set the start and insert pages */
    MyflatUpdateScan(index, buildstate->listInfo[i], insertPage, InvalidBlockNumber, startPage, forkNum);
}


/*
 * Initialize the build state
 */
static void
InitBuildState(MyflatBuildState * buildstate, Relation heap, Relation index, IndexInfo *indexInfo)
{
	buildstate->heap = heap;
	buildstate->index = index;
	buildstate->indexInfo = indexInfo;
	buildstate->typeInfo = NULL;
	buildstate->tupdesc = RelationGetDescr(index);

	buildstate->check = MyflatGetCheck(index);
	buildstate->dimensions = TupleDescAttr(index->rd_att, 0)->atttypmod;

	/* Disallow varbit since require fixed dimensions */
	if (TupleDescAttr(index->rd_att, 0)->atttypid == VARBITOID)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("type not supported for myflat index")));

	/* Require column to have dimensions to be indexed */
	if (buildstate->dimensions < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("column does not have dimensions")));

	// if (buildstate->dimensions > buildstate->typeInfo->maxDimensions)
	// 	ereport(ERROR,
	// 			(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
	// 			 errmsg("column cannot have more than %d dimensions for myflat index", buildstate->typeInfo->maxDimensions)));

	buildstate->reltuples = 0;
	buildstate->indtuples = 0;

	// Create tuple description for sorting (no sorting and just for store in myflat)
    buildstate->sortdesc = CreateTemplateTupleDesc(3);
	TupleDescInitEntry(buildstate->sortdesc, (AttrNumber) 1, "plain_list", INT4OID, -1, 0);
    TupleDescInitEntry(buildstate->sortdesc, (AttrNumber) 2, "tid", TIDOID, -1, 0);
    TupleDescInitEntry(buildstate->sortdesc, (AttrNumber) 3, "vector", 
                      buildstate->tupdesc->attrs[0].atttypid, -1, 0);

	buildstate->slot = MakeSingleTupleTableSlot(buildstate->sortdesc, &TTSOpsVirtual);


	buildstate->listInfo = palloc(sizeof(ScanInfo));

	buildstate->tmpCtx = AllocSetContextCreate(CurrentMemoryContext,
											   "Myflat build temporary context",
											   ALLOCSET_DEFAULT_SIZES);


	buildstate->myflatleader = NULL;
}

/*
 * Free resources
 */
static void
FreeBuildState(MyflatBuildState * buildstate)
{
	pfree(buildstate->listInfo);

	MemoryContextDelete(buildstate->tmpCtx);
}

/*
 * Create the metapage
 */
static void
CreateMetaPage(Relation index, int dimensions, int check, ForkNumber forkNum)
{
	Buffer		buf;
	Page		page;
	GenericXLogState *state;
	MyflatMetaPage metap;

	buf = MyflatNewBuffer(index, forkNum);
	MyflatInitRegisterPage(index, &buf, &page, &state);


	/* Set metapage data */
	metap = MyflatPageGetMeta(page);
	metap->magicNumber = MYFLAT_MAGIC_NUMBER;
	metap->version = MYFLAT_VERSION;
	metap->dimensions = dimensions;
	metap->check = check;
	((PageHeader) page)->pd_lower =
		((char *) metap + sizeof(MyflatMetaPageData)) - (char *) page;
	
	MyflatCommitBuffer(buf, state);
}

/*
 * Create scan pages
 */
static void
CreateScanPages(Relation index, int dimensions,
				int check, ForkNumber forkNum, ScanInfo * *listInfo)
{
	Buffer		buf;
	Page		page;
	GenericXLogState *state;
	Size		scanSize;
	MyflatScan scan;

	int i = 0;

	scanSize = MAXALIGN(MYFLAT_SCAN_SIZE);
	scan = palloc0(scanSize);

	buf = MyflatNewBuffer(index, forkNum);
	MyflatInitRegisterPage(index, &buf, &page, &state);

	OffsetNumber offno;

	/* Zero memory for scan */
	MemSet(scan, 0, scanSize);

	/* Load scan */
	scan->startPage = InvalidBlockNumber;
	scan->insertPage = InvalidBlockNumber;

	/* Ensure free space */
	if (PageGetFreeSpace(page) < scanSize)
		MyflatAppendPage(index, &buf, &page, &state, forkNum);

	/* Add the item */
	offno = PageAddItem(page, (Item) scan, scanSize, InvalidOffsetNumber, false, false);
	if (offno == InvalidOffsetNumber)
		elog(ERROR, "failed to add index item to \"%s\"", RelationGetRelationName(index));

	/* Save location info */
	(*listInfo)[i].blkno = BufferGetBlockNumber(buf);
	(*listInfo)[i].offno = offno;

	MyflatCommitBuffer(buf, state);

	pfree(scan);
}

/*
 * Initialize build sort state
 */
static Tuplesortstate *
InitBuildSortState(TupleDesc tupdesc, int memory, SortCoordinate coordinate)
{
	AttrNumber	attNums[] = {1};
	Oid			sortOperators[] = {Int4LessOperator};
	Oid			sortCollations[] = {InvalidOid};
	bool		nullsFirstFlags[] = {false};

	return tuplesort_begin_heap(tupdesc, 1, attNums, sortOperators, sortCollations, nullsFirstFlags, memory, coordinate, false);
}

/*
 * Scan table for tuples to index
 */
static void
AssignTuples(MyflatBuildState * buildstate)
{
	int			parallel_workers = 0;
	SortCoordinate coordinate = NULL;

	pgstat_progress_update_param(PROGRESS_CREATEIDX_SUBPHASE, PROGRESS_MYFLAT_PHASE_LOAD);

	/* Calculate parallel workers */
	if (buildstate->heap != NULL)
		parallel_workers = plan_create_index_workers(RelationGetRelid(buildstate->heap), RelationGetRelid(buildstate->index));

	/* Set up coordination state if at least one worker launched */
	if (buildstate->myflatleader)
	{
		coordinate = (SortCoordinate) palloc0(sizeof(SortCoordinateData));
		coordinate->isWorker = false;
		coordinate->nParticipants = buildstate->myflatleader->nparticipanttuplesorts;
		coordinate->sharedsort = buildstate->myflatleader->sharedsort;
	}

	/* Begin serial/leader tuplesort */
	/* Just for store the tuple */
	// buildstate->sortstate = tuplesort_begin_heap(buildstate->sortdesc,
	// 							0,     // no sort keys
	// 							NULL,  // no attribute numbers
	// 							NULL,  // no sort operators
	// 							NULL,  // no collations
	// 							NULL,  // no nullsFirst flags
	// 							maintenance_work_mem,
	// 							coordinate,  // no parallel coordination
	// 							false);// no random access needed
	buildstate->sortstate = InitBuildSortState(buildstate->sortdesc, maintenance_work_mem, coordinate);


	/* Add tuples to sort */
	if (buildstate->heap != NULL)
	{
		buildstate->reltuples = table_index_build_scan(buildstate->heap, buildstate->index, buildstate->indexInfo,
														   true, true, BuildCallback, (void *) buildstate, NULL);

	}
}


// TODO: in progress of creating entry
/*
 * Create entry pages
 */
static void
CreateEntryPages(MyflatBuildState * buildstate, ForkNumber forkNum)
{
	/* Assign */
	MyflatBench("assign tuples", AssignTuples(buildstate));

	MyflatBench("sort tuples", tuplesort_performsort(buildstate->sortstate));

	/* Load */
	MyflatBench("load tuples", InsertTuples(buildstate->index, buildstate, forkNum));

	/* End sort */
	tuplesort_end(buildstate->sortstate);

	/* End parallel build */
	// if (buildstate->myflatleader)
	// 	MyflatEndParallel(buildstate->ivfleader);
}

// TODO: Build the index (I think need to build the entry)
// Built the scan pages (same as list pages in IVFFLAT, but just array size of 1)
// TODO: Create the entry pages (done)
/*
 * Build the index
 */
static void
BuildIndex(Relation heap, Relation index, IndexInfo *indexInfo,
		   MyflatBuildState * buildstate, ForkNumber forkNum)
{
	InitBuildState(buildstate, heap, index, indexInfo);

	/* Create pages */
	CreateMetaPage(index, buildstate->dimensions, buildstate->check, forkNum);
	CreateScanPages(index, buildstate->dimensions, buildstate->check, forkNum, &buildstate->listInfo);
	CreateEntryPages(buildstate, forkNum);

	/* Write WAL for initialization fork since GenericXLog functions do not */
	if (forkNum == INIT_FORKNUM)
		log_newpage_range(index, forkNum, 0, RelationGetNumberOfBlocksInFork(index, forkNum), true);

	FreeBuildState(buildstate);
}


/*
 * Build the index for a logged table
 */
IndexBuildResult *
myflatbuild(Relation heap, Relation index, IndexInfo *indexInfo)
{
	IndexBuildResult *result;
	MyflatBuildState buildstate;

	BuildIndex(heap, index, indexInfo, &buildstate, MAIN_FORKNUM);

	result = (IndexBuildResult *) palloc(sizeof(IndexBuildResult));
	result->heap_tuples = buildstate.reltuples;
	result->index_tuples = buildstate.indtuples;

	return result;
}

/*
 * Build the index for an unlogged table
 */
void
myflatbuildempty(Relation index)
{
	IndexInfo  *indexInfo = BuildIndexInfo(index);
	MyflatBuildState buildstate;

	BuildIndex(NULL, index, indexInfo, &buildstate, INIT_FORKNUM);
}