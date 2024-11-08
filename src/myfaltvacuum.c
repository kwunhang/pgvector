#include "postgres.h"

#include "access/generic_xlog.h"
#include "commands/vacuum.h"
#include "myflat.h"
#include "storage/bufmgr.h"

/*
 * Bulk delete tuples from the index
 */


// TODO