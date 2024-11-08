#ifndef MYFLAT_H
#define MYFLAT_H

#include "postgres.h"

#include "access/genam.h"
#include "access/generic_xlog.h"
#include "access/parallel.h"
#include "lib/pairingheap.h"
#include "nodes/execnodes.h"
#include "port.h"				/* for random() */
#include "utils/sampling.h"
#include "utils/tuplesort.h"
#include "vector.h"

#if PG_VERSION_NUM >= 150000
#include "common/pg_prng.h"
#endif

#ifdef MYFLAT_BENCH
#include "portability/instr_time.h"
#endif

#define MYFLAT_MAX_DIM 2000
/* Support functions */
#define MYFLAT_VERSION	1
#define MYFLAT_MAGIC_NUMBER 0x2BCA1387
#define MYFLAT_PAGE_ID	0xFFAC

/* MYFFlat parameters */
#define MYFLAT_MIN_RANDOM_RATIO		1
#define MYFLAT_MAX_RANDOM_RATIO		100
#define MYFLAT_DEFAULT_RANDOM_RATIO	50

/* Variables */
extern int	myflat_random_ratio;
extern int  myflat_max_random_ratio;


typedef enum MyflatIterativeScanMode
{
	MYFLAT_ITERATIVE_SCAN_OFF,
	MYFLAT_ITERATIVE_SCAN_RELAXED
}			MyflatIterativeScanMode;

/* MyFlat index options */
typedef struct MyflatOptions
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	int			unused;			/* unused variable */
}			MyflatOptions;


// use in buildstate for parallel use
// typedef struct MyflatLeader
// {
// 	ParallelContext *pcxt;
// 	int			nparticipanttuplesorts;
// 	MyflatShared *Myshared;
// 	Sharedsort *sharedsort;
// 	Snapshot	snapshot;
// 	char	   *ivfcenters;
// }			MyflatLeader;


typedef struct MyflatTypeInfo
{
	int			maxDimensions;
	Datum		(*normalize) (PG_FUNCTION_ARGS);
	Size		(*itemSize) (int dimensions);
}			MyflatTypeInfo;

typedef struct MyflatBuildState
{
	/* Info */
	Relation	heap;
	Relation	index;
	IndexInfo  *indexInfo;
	const		MyflatTypeInfo *typeInfo;
	TupleDesc	tupdesc;

	/* Settings */
	int			dimensions;
	int			unused;

	/* Statistics */
	double		indtuples;
	double		reltuples;

	/* Support functions */
	// FmgrInfo   *procinfo;
	// FmgrInfo   *normprocinfo;
	// FmgrInfo   *kmeansnormprocinfo;
	// Oid			collation;

	/* Variables */
	// VectorArray samples;
	// VectorArray centers;
	// ListInfo   *listInfo;

	/* Sampling */
	// BlockSamplerData bs;
	// ReservoirStateData rstate;
	// int			rowstoskip;

	/* Sorting */
	// Tuplesortstate *sortstate;
	// TupleDesc	sortdesc;
	// TupleTableSlot *slot;

	/* Memory */
	MemoryContext tmpCtx;

	/* Parallel builds */
	// MyflatLeader *myflatleader;
}			MyflatBuildState;


typedef struct MyflatMetaPageData
{
	uint32		magicNumber;
	uint32		version;
	uint16		dimensions;
	uint16		unused;
}			MyflatMetaPageData;

typedef MyflatMetaPageData * MyflatMetaPage;

typedef struct MyflatPageOpaqueData
{
	BlockNumber nextblkno;
	uint16		unused;
	uint16		page_id;		/* for identification of MYFLAT indexes */
}			MyflatPageOpaqueData;

typedef MyflatPageOpaqueData * MyflatPageOpaque;

/* Methods */

/* Index access methods */
IndexBuildResult *myflatbuild(Relation heap, Relation index, IndexInfo *indexInfo);
void		myflatbuildempty(Relation index);
bool		myflatinsert(Relation index, Datum *values, bool *isnull, ItemPointer heap_tid, Relation heap, IndexUniqueCheck checkUnique
#if PG_VERSION_NUM >= 140000
						  ,bool indexUnchanged
#endif
						  ,IndexInfo *indexInfo
);
IndexBulkDeleteResult *myflatbulkdelete(IndexVacuumInfo *info, IndexBulkDeleteResult *stats, IndexBulkDeleteCallback callback, void *callback_state);
IndexBulkDeleteResult *myflatvacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats);
IndexScanDesc myflatbeginscan(Relation index, int nkeys, int norderbys);
void		myflatrescan(IndexScanDesc scan, ScanKey keys, int nkeys, ScanKey orderbys, int norderbys);
bool		myflatgettuple(IndexScanDesc scan, ScanDirection dir);
void		myflatendscan(IndexScanDesc scan);

#endif
