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
 * Create the metapage
 */
static void
CreateMetaPage(Relation index, int dimensions, int lists, ForkNumber forkNum)
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
	metap->unused = 0;
	((PageHeader) page)->pd_lower =
		((char *) metap + sizeof(MyflatMetaPageData)) - (char *) page;
	
	MyflatCommitBuffer(buf, state);
}

// TODO: Build the index (I think need to build the entry)
/*
 * Build the index
 */
static void
BuildIndex(Relation heap, Relation index, IndexInfo *indexInfo,
		   MyflatBuildState * buildstate, ForkNumber forkNum)
{
	InitBuildState(buildstate, heap, index, indexInfo);

	/* Create pages */
	CreateMetaPage(index, buildstate->dimensions, buildstate->lists, forkNum);
	// CreateListPages(index, buildstate->centers, buildstate->dimensions, buildstate->lists, forkNum, &buildstate->listInfo);
	// CreateEntryPages(buildstate, forkNum);

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