#include "postgres.h"

#include "access/generic_xlog.h"
#include "bitvec.h"
#include "catalog/pg_type.h"
#include "fmgr.h"
#include "halfutils.h"
#include "halfvec.h"
#include "myflat.h"
#include "storage/bufmgr.h"


/*
 * Get the "unused" in the index
 */
int
MyflatGetUnused(Relation index)
{
	MyflatOptions *opts = (MyflatOptions *) index->rd_options;

	if (opts)
		return opts->unused;

	return MYFLAT_DEFAULT_RANDOM_RATIO;
}


/*
 * Get proc
 */
FmgrInfo *
MyflatOptionalProcInfo(Relation index, uint16 procnum)
{
	if (!OidIsValid(index_getprocid(index, 1, procnum)))
		return NULL;

	return index_getprocinfo(index, 1, procnum);
}

/*
 * Normalize value
 */
Datum
MyflatNormValue(const MyflatTypeInfo * typeInfo, Oid collation, Datum value)
{
	return DirectFunctionCall1Coll(typeInfo->normalize, collation, value);
}

/*
 * Check if non-zero norm
 */
bool
MyflatCheckNorm(FmgrInfo *procinfo, Oid collation, Datum value)
{
	return DatumGetFloat8(FunctionCall1Coll(procinfo, collation, value)) > 0;
}

/*
 * New buffer
 */
Buffer
MyflatNewBuffer(Relation index, ForkNumber forkNum)
{
	Buffer		buf = ReadBufferExtended(index, forkNum, P_NEW, RBM_NORMAL, NULL);

	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
	return buf;
}


/*
 * Init page
 */
void
MyflatInitPage(Buffer buf, Page page)
{
	PageInit(page, BufferGetPageSize(buf), sizeof(MyflatPageOpaqueData));
	MyflatPageGetOpaque(page)->nextblkno = InvalidBlockNumber;
	MyflatPageGetOpaque(page)->page_id = MYFLAT_PAGE_ID;
}

/*
 * Init and register page
 */
void
MyflatInitRegisterPage(Relation index, Buffer *buf, Page *page, GenericXLogState **state)
{
	*state = GenericXLogStart(index);
	*page = GenericXLogRegisterBuffer(*state, *buf, GENERIC_XLOG_FULL_IMAGE);
	MyflatInitPage(*buf, *page);
}

/*
 * Commit buffer
 */
void
MyflatCommitBuffer(Buffer buf, GenericXLogState *state)
{
	GenericXLogFinish(state);
	UnlockReleaseBuffer(buf);
}

/*
 * Add a new page
 *
 * The order is very important!!
 */
void
MyflatAppendPage(Relation index, Buffer *buf, Page *page, GenericXLogState **state, ForkNumber forkNum)
{
	/* Get new buffer */
	Buffer		newbuf = MyflatNewBuffer(index, forkNum);
	Page		newpage = GenericXLogRegisterBuffer(*state, newbuf, GENERIC_XLOG_FULL_IMAGE);

	/* Update the previous buffer */
	MyflatPageGetOpaque(*page)->nextblkno = BufferGetBlockNumber(newbuf);

	/* Init new page */
	MyflatInitPage(newbuf, newpage);

	/* Commit */
	GenericXLogFinish(*state);

	/* Unlock */
	UnlockReleaseBuffer(*buf);

	*state = GenericXLogStart(index);
	*page = GenericXLogRegisterBuffer(*state, newbuf, GENERIC_XLOG_FULL_IMAGE);
	*buf = newbuf;
}

/*
 * Get the metapage info
 */
void
MyflatGetMetaPageInfo(Relation index, int *unused, int *dimensions)
{
	Buffer		buf;
	Page		page;
	MyflatMetaPage metap;

	buf = ReadBuffer(index, MYFLAT_METAPAGE_BLKNO);
	LockBuffer(buf, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buf);
	metap = MyflatPageGetMeta(page);

	if (unlikely(metap->magicNumber != MYFLAT_MAGIC_NUMBER))
		elog(ERROR, "myflat index is not valid");

	if (unused != NULL)
		*unused = metap->unused;

	if (dimensions != NULL)
		*dimensions = metap->dimensions;

	UnlockReleaseBuffer(buf);
}



/*
 * Update the start or insert page of Scan
 */
MyflatUpdateScan(Relation index, ListInfo listInfo,
				  BlockNumber insertPage, BlockNumber originalInsertPage,
				  BlockNumber startPage, ForkNumber forkNum)
{
	Buffer		buf;
	Page		page;
	GenericXLogState *state;
	MyflatScan scan;
	bool		changed = false;

	buf = ReadBufferExtended(index, forkNum, listInfo.blkno, RBM_NORMAL, NULL);
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
	state = GenericXLogStart(index);
	page = GenericXLogRegisterBuffer(state, buf, 0);
	scan = (MyflatScan) PageGetItem(page, PageGetItemId(page, listInfo.offno));

	if (BlockNumberIsValid(insertPage) && insertPage != scan->insertPage)
	{
		/* Skip update if insert page is lower than original insert page  */
		/* This is needed to prevent insert from overwriting vacuum */
		if (!BlockNumberIsValid(originalInsertPage) || insertPage >= originalInsertPage)
		{
			scan->insertPage = insertPage;
			changed = true;
		}
	}

	if (BlockNumberIsValid(startPage) && startPage != scan->startPage)
	{
		scan->startPage = startPage;
		changed = true;
	}

	/* Only commit if changed */
	if (changed)
		MyflatCommitBuffer(buf, state);
	else
	{
		GenericXLogAbort(state);
		UnlockReleaseBuffer(buf);
	}
}

PGDLLEXPORT Datum l2_normalize(PG_FUNCTION_ARGS);