#include "postgres.h"

#include <float.h>

#include "access/amapi.h"
#include "access/reloptions.h"
#include "commands/progress.h"
#include "commands/vacuum.h"
#include "myflat.h"
#include "utils/float.h"
#include "utils/guc.h"
#include "utils/selfuncs.h"
#include "utils/spccache.h"

#if PG_VERSION_NUM < 150000
#define MarkGUCPrefixReserved(x) EmitWarningsOnPlaceholders(x)
#endif

static const struct config_enum_entry myflat_iterative_scan_options[] = {
	{"off", MYFLAT_ITERATIVE_SCAN_OFF, false},
	{"relaxed_order", MYFLAT_ITERATIVE_SCAN_RELAXED, false},
	{NULL, 0, false}
};


int			myflat_random_ratio;
int         myflat_max_random_ratio;
static relopt_kind myflat_relopt_kind;


/*
 * Initialize index options and variables
 */
void
MyflatInit(void)
{
	myflat_relopt_kind = add_reloption_kind();

	add_int_reloption(myflat_relopt_kind, "check", "Check variable",
					  MYFLAT_DEFAULT_RANDOM_RATIO, MYFLAT_MIN_RANDOM_RATIO, MYFLAT_MAX_RANDOM_RATIO, AccessExclusiveLock);

	DefineCustomIntVariable("myflat.random_ratio", "Sets the ratio of random pick",
							"Valid range is 1..random_ratio.", &myflat_random_ratio,
							MYFLAT_DEFAULT_RANDOM_RATIO, MYFLAT_MIN_RANDOM_RATIO, MYFLAT_MAX_RANDOM_RATIO, PGC_USERSET, 0, NULL, NULL, NULL);

	MarkGUCPrefixReserved("myflat");
}


/*
 * Get the name of index build phase
 */
static char *
myflatbuildphasename(int64 phasenum)
{
	switch (phasenum)
	{
		case PROGRESS_CREATEIDX_SUBPHASE_INITIALIZE:
			return "initializing";
		case PROGRESS_MYFLAT_PHASE_LOAD:
			return "loading tuples";
		default:
			return NULL;
	}
}

/*
 * Estimate the cost of an index scan
 */
static void
myflatcostestimate(PlannerInfo *root, IndexPath *path, double loop_count,
					Cost *indexStartupCost, Cost *indexTotalCost,
					Selectivity *indexSelectivity, double *indexCorrelation,
					double *indexPages)
{
	GenericCosts costs;
	int			check;
	int			ramdom_ratio=1;
	double		ratio;
	// double		sequentialRatio = 0.5;
	double		startupPages;
	double		spc_seq_page_cost;
	Relation	index;

	/* Never use index without order */
	if (path->indexorderbys == NULL)
	{
		*indexStartupCost = get_float8_infinity();
		*indexTotalCost = get_float8_infinity();
		*indexSelectivity = 0;
		*indexCorrelation = 0;
		*indexPages = 0;
#if PG_VERSION_NUM >= 180000
		/* See "On disable_cost" thread on pgsql-hackers */
		path->path.disabled_nodes = 2;
#endif
		return;
	}

	MemSet(&costs, 0, sizeof(costs));

	genericcostestimate(root, path, loop_count, &costs);

	index = index_open(path->indexinfo->indexoid, NoLock);
	MyflatGetMetaPageInfo(index, &check, NULL);
	index_close(index, NoLock);

	/* Get the ratio of lists that we need to visit */
	ratio = ((double) ramdom_ratio) / 100;
	if (ratio > 1.0)
		ratio = 1.0;

	get_tablespace_page_costs(path->indexinfo->reltablespace, NULL, &spc_seq_page_cost);

	/* Startup cost is cost before returning the first row */
	costs.indexStartupCost = costs.indexTotalCost * ratio;

	/* Adjust cost if needed since TOAST not included in seq scan cost */
	startupPages = costs.numIndexPages * ratio;
	if (startupPages > path->indexinfo->rel->pages && ratio < 0.5)
	{
        /* Change all page cost from random to sequential (ignored) */
		/* Remove cost of extra pages */
		costs.indexStartupCost -= (startupPages - path->indexinfo->rel->pages) * spc_seq_page_cost;
	}

	*indexStartupCost = costs.indexStartupCost;
	*indexTotalCost = costs.indexTotalCost;
	*indexSelectivity = costs.indexSelectivity;
	*indexCorrelation = costs.indexCorrelation;
	*indexPages = costs.numIndexPages;
}

/*
 * Parse and validate the reloptions
 */
static bytea *
myflatoptions(Datum reloptions, bool validate)
{
	static const relopt_parse_elt tab[] = {
		{"check", RELOPT_TYPE_INT, offsetof(MyflatOptions, check)},
	};

	return (bytea *) build_reloptions(reloptions, validate,
									  myflat_relopt_kind,
									  sizeof(MyflatOptions),
									  tab, lengthof(tab));
}


/*
 * Validate catalog entries for the specified operator class
 */
static bool
myflatvalidate(Oid opclassoid)
{
	return true;
}

/*
 * Define index handler
 *
 * See https://www.postgresql.org/docs/current/index-api.html
 */
FUNCTION_PREFIX PG_FUNCTION_INFO_V1(myflathandler);
Datum
myflathandler(PG_FUNCTION_ARGS)
{
    IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);

    amroutine->amstrategies = 0;
	amroutine->amsupport = 5;
	amroutine->amoptsprocnum = 0;
	amroutine->amcanorder = false;
	amroutine->amcanorderbyop = true;
	amroutine->amcanbackward = false;	/* can change direction mid-scan */
	amroutine->amcanunique = false;
	amroutine->amcanmulticol = false;
	amroutine->amoptionalkey = true;
	amroutine->amsearcharray = false;
	amroutine->amsearchnulls = false;
	amroutine->amstorage = false;
	amroutine->amclusterable = false;
	amroutine->ampredlocks = false;
	amroutine->amcanparallel = false;
#if PG_VERSION_NUM >= 170000
	amroutine->amcanbuildparallel = false;
#endif
    amroutine->amcaninclude = false;
    amroutine->amusemaintenanceworkmem = false; /* not used during VACUUM */
#if PG_VERSION_NUM >= 160000
	amroutine->amsummarizing = false;
#endif
	amroutine->amparallelvacuumoptions = VACUUM_OPTION_PARALLEL_BULKDEL;
	amroutine->amkeytype = InvalidOid;

	/* Interface functions */
	amroutine->ambuild = myflatbuild;
	amroutine->ambuildempty = myflatbuildempty;
    amroutine->aminsert = myflatinsert;
#if PG_VERSION_NUM >= 170000
	amroutine->aminsertcleanup = NULL;
#endif
    amroutine->ambulkdelete = myflatbulkdelete;
    amroutine->amvacuumcleanup = myflatvacuumcleanup;
    amroutine->amcanreturn = NULL;
    amroutine->amcostestimate = myflatcostestimate;
    amroutine->amoptions = myflatoptions;
    amroutine->amproperty = NULL;
    amroutine->ambuildphasename = myflatbuildphasename;
    amroutine->amvalidate = myflatvalidate;
#if PG_VERSION_NUM >= 140000
	amroutine->amadjustmembers = NULL;
#endif
    amroutine->ambeginscan = myflatbeginscan;
    amroutine->amrescan = myflatrescan;
    amroutine->amgettuple = myflatgettuple;
    amroutine->amgetbitmap = NULL;
    amroutine->amendscan = myflatendscan;
    amroutine->ammarkpos = NULL;
    amroutine->amrestrpos = NULL;

    /* Interface functions to support parallel index scans */
    amroutine->amestimateparallelscan = NULL;
    amroutine->aminitparallelscan = NULL;
    amroutine->amparallelrescan = NULL;

    PG_RETURN_POINTER(amroutine);
}