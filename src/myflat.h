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
#define MYFLAT_DISTANCE_PROC 1
#define MYFLAT_NORM_PROC 2
#define MYFLAT_TYPE_INFO_PROC 3 /* for regconize bit, half vec, sparse vec */

#define MYFLAT_VERSION	1
#define MYFLAT_MAGIC_NUMBER 0x2BCA1387
#define MYFLAT_PAGE_ID	0xFFAC

/* Preserved page numbers */
#define MYFLAT_METAPAGE_BLKNO	0
#define MYFLAT_HEAD_BLKNO		1	/* scan page */

/* MYFFlat parameters */
#define MYFLAT_MIN_RANDOM_RATIO		1
#define MYFLAT_MAX_RANDOM_RATIO		100
#define MYFLAT_DEFAULT_RANDOM_RATIO	50

/* Build phases */
/* PROGRESS_CREATEIDX_SUBPHASE_INITIALIZE is 1  */
#define PROGRESS_MYFLAT_PHASE_LOAD	2

#define MYFLAT_SCAN_SIZE	(sizeof(MyflatScanData) )

#define MyflatPageGetOpaque(page)	((MyflatPageOpaque) PageGetSpecialPointer(page))
#define MyflatPageGetMeta(page)		((MyflatMetaPageData *) PageGetContents(page))

#ifdef MYFLAT_BENCH
#define MyflatBench(name, code) \
	do { \
		instr_time	start; \
		instr_time	duration; \
		INSTR_TIME_SET_CURRENT(start); \
		(code); \
		INSTR_TIME_SET_CURRENT(duration); \
		INSTR_TIME_SUBTRACT(duration, start); \
		elog(INFO, "%s: %.3f ms", name, INSTR_TIME_GET_MILLISEC(duration)); \
	} while (0)
#else
#define MyflatBench(name, code) (code)
#endif

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
	int			check;			/* unused variable */
}			MyflatOptions;


typedef struct ScanInfo
{
	BlockNumber blkno;
	OffsetNumber offno;
}			ScanInfo;

// use in buildstate for parallel use
typedef struct MyflatLeader
{
	ParallelContext *pcxt;
	int			nparticipanttuplesorts;
	// MyflatShared *Myshared;
	Sharedsort *sharedsort;
	Snapshot	snapshot;
	// char	   *ivfcenters;
}			MyflatLeader;


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
	int			check;

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
	ScanInfo   *listInfo;

	/* Sampling */
	// BlockSamplerData bs;
	// ReservoirStateData rstate;
	// int			rowstoskip;

	/* Sorting */
	Tuplesortstate *sortstate;
	TupleDesc	sortdesc;
	TupleTableSlot *slot;

	/* Memory */
	MemoryContext tmpCtx;

	/* Parallel builds */
	MyflatLeader *myflatleader;
}			MyflatBuildState;


typedef struct MyflatMetaPageData
{
	uint32		magicNumber;
	uint32		version;
	uint16		dimensions;
	uint16		check;
}			MyflatMetaPageData;

typedef MyflatMetaPageData * MyflatMetaPage;

typedef struct MyflatPageOpaqueData
{
	BlockNumber nextblkno;
	uint16		unused;
	uint16		page_id;		/* for identification of MYFLAT indexes */
}			MyflatPageOpaqueData;

typedef MyflatPageOpaqueData * MyflatPageOpaque;

typedef struct MyflatScanData
{
	BlockNumber startPage;
	BlockNumber insertPage;
}			MyflatScanData;

typedef MyflatScanData * MyflatScan;


typedef struct MyflatScanList
{
	pairingheap_node ph_node;
	BlockNumber startPage;
	double		distance;
}			MyflatScanList;


typedef struct MyflatScanOpaqueData
{
	const		MyflatTypeInfo *typeInfo;
	int			dimensions;
	bool		first;
	Datum		value;
	MemoryContext tmpCtx;

	/* Sorting */
	Tuplesortstate *sortstate;
	TupleDesc	tupdesc;
	TupleTableSlot *vslot;
	TupleTableSlot *mslot;
	BufferAccessStrategy bas;

	/* Support functions */
	FmgrInfo   *procinfo;
	FmgrInfo   *normprocinfo;
	Oid			collation;
	Datum		(*distfunc) (FmgrInfo *flinfo, Oid collation, Datum arg1, Datum arg2);

	/* Lists */
	pairingheap *listQueue;
	BlockNumber *listPages;
	MyflatScanList *lists;
}			MyflatScanOpaqueData;

typedef MyflatScanOpaqueData * MyflatScanOpaque;

/* Methods */
FmgrInfo 	*MyflatOptionalProcInfo(Relation index, uint16 procnum);
Datum		MyflatNormValue(const MyflatTypeInfo * typeInfo, Oid collation, Datum value);
bool 		MyflatCheckNorm(FmgrInfo *procinfo, Oid collation, Datum value);
int			MyflatGetCheck(Relation index);
void		MyflatGetMetaPageInfo(Relation index, int *check, int *dimensions);
void		MyflatUpdateScan(Relation index, ScanInfo listInfo, BlockNumber insertPage, BlockNumber originalInsertPage, BlockNumber startPage, ForkNumber forkNum);
void		MyflatCommitBuffer(Buffer buf, GenericXLogState *state);
void		MyflatAppendPage(Relation index, Buffer *buf, Page *page, GenericXLogState **state, ForkNumber forkNum);
Buffer		MyflatNewBuffer(Relation index, ForkNumber forkNum);
void		MyflatInitPage(Buffer buf, Page page);
void		MyflatInitRegisterPage(Relation index, Buffer *buf, Page *page, GenericXLogState **state);
void		MyflatInit(void);
const		MyflatTypeInfo *MyflatGetTypeInfo(Relation index);

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
