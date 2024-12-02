#include "postgres.h"

#include "access/generic_xlog.h"
#include "commands/vacuum.h"
#include "myflat.h"
#include "storage/bufmgr.h"

/*
 * Bulk delete tuples from the index
 */


// TODO

IndexBulkDeleteResult *
myflatbulkdelete(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
				  IndexBulkDeleteCallback callback, void *callback_state)
{
    Relation index = info->index;
    BlockNumber blkno = MYFLAT_HEAD_BLKNO;
    BufferAccessStrategy bas = GetAccessStrategy(BAS_BULKREAD);

    if (stats == NULL)
		stats = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));

	/* Iterate over scan pages */
	while (BlockNumberIsValid(blkno))
    {
        Buffer		cbuf;
		Page		cpage;
		OffsetNumber coffno;
		OffsetNumber cmaxoffno;
		BlockNumber scanStartPage;
		ListInfo	listInfo;

        cbuf = ReadBuffer(index, blkno);
		LockBuffer(cbuf, BUFFER_LOCK_SHARE);
		cpage = BufferGetPage(cbuf);

		/* Get the start page of scan */
        coffno = FirstOffsetNumber;
        MyflatScan scan = (MyflatScan) PageGetItem(cpage, PageGetItemId(cpage, coffno));
        scanStartPage = scan->startPage;

        listInfo.blkno = blkno;
        /* move to next blk end while */
        blkno = MyflatPageGetOpaque(cpage)->nextblkno;

        UnlockReleaseBuffer(cbuf);


        BlockNumber searchPage = scanStartPage;
        BlockNumber insertPage = InvalidBlockNumber;
        /* Iterate over entry pages */
        while (BlockNumberIsValid(searchPage))
        {
            Buffer		buf;
            Page		page;
            GenericXLogState *state;
            OffsetNumber offno;
            OffsetNumber maxoffno;
            OffsetNumber deletable[MaxOffsetNumber];
            int			ndeletable;

            vacuum_delay_point();

            buf = ReadBufferExtended(index, MAIN_FORKNUM, searchPage, RBM_NORMAL, bas);

            /*
                * ambulkdelete cannot delete entries from pages that are
                * pinned by other backends
                *
                * https://www.postgresql.org/docs/current/index-locking.html
                */
            LockBufferForCleanup(buf);

            state = GenericXLogStart(index);
            page = GenericXLogRegisterBuffer(state, buf, 0);

            maxoffno = PageGetMaxOffsetNumber(page);
            ndeletable = 0;

            /* Find deleted tuples */
            for (offno = FirstOffsetNumber; offno <= maxoffno; offno = OffsetNumberNext(offno))
            {
                IndexTuple	itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, offno));
                ItemPointer htup = &(itup->t_tid);

                if (callback(htup, callback_state))
                {
                    deletable[ndeletable++] = offno;
                    stats->tuples_removed++;
                }
                else
                    stats->num_index_tuples++;
            }

            /* Set to first free page */
            /* Must be set before searchPage is updated */
            if (!BlockNumberIsValid(insertPage) && ndeletable > 0)
                insertPage = searchPage;

            searchPage = MyflatPageGetOpaque(page)->nextblkno;

            if (ndeletable > 0)
            {
                /* Delete tuples */
                PageIndexMultiDelete(page, deletable, ndeletable);
                GenericXLogFinish(state);
            }
            else
                GenericXLogAbort(state);

            UnlockReleaseBuffer(buf);
        }

        /*
        * Update after all tuples deleted.
        *
        * We don't add or delete items from lists pages, so offset won't
        * change.
        */
        if (BlockNumberIsValid(insertPage))
        {
            listInfo.offno = coffno;
            MyflatUpdateScan(index, listInfo, insertPage, InvalidBlockNumber, InvalidBlockNumber, MAIN_FORKNUM);
        }
    }
    FreeAccessStrategy(bas);

	return stats;
}

/*
 * Clean up after a VACUUM operation
 */
IndexBulkDeleteResult *
myflatvacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats)
{
	Relation	rel = info->index;

	if (info->analyze_only)
		return stats;

	/* stats is NULL if ambulkdelete not called */
	/* OK to return NULL if index not changed */
	if (stats == NULL)
		return NULL;

	stats->num_pages = RelationGetNumberOfBlocks(rel);

	return stats;
}