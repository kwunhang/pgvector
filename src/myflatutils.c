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
