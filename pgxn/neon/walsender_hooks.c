#include "walsender_hooks.h"
#include "postgres.h"
#include "fmgr.h"
#include "access/xlogdefs.h"
#include "replication/walsender.h"
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xlogreader.h"
#include "miscadmin.h"
#include "utils/wait_event.h"

#include "neon_walreader.h"
#include "walproposer.h"

static NeonWALReader *wal_reader = NULL;
extern XLogRecPtr WalSndWaitForWal(XLogRecPtr loc);
extern bool GetDonorShmem(XLogRecPtr *donor_lsn);

static XLogRecPtr NeonWALReadWaitForWAL(XLogRecPtr loc)
{
    while(!NeonWALReaderUpdateDonor(wal_reader))
        pg_usleep(1000);

    return WalSndWaitForWal(loc);
}

static int NeonWALPageRead(
    XLogReaderState *xlogreader,
    XLogRecPtr targetPagePtr,
    int reqLen,
    XLogRecPtr targetRecPtr,
    char *readBuf)
{
    XLogRecPtr flushptr = NeonWALReadWaitForWAL(targetPagePtr + reqLen);
    int count;
    if (flushptr < targetPagePtr + reqLen)
	return -1;
    if (targetPagePtr + XLOG_BLCKSZ <= flushptr)
	count = XLOG_BLCKSZ;
    else
	count = flushptr - targetPagePtr;

    if(targetPagePtr != NeonWALReaderGetRemLsn(wal_reader))
    {
	NeonWALReaderResetRemote(wal_reader);
    }
    static WaitEventSet *wait_event_set;
    for(;;)
    {
	NeonWALReadResult res = NeonWALRead(
	    wal_reader,
	    readBuf,
	    targetPagePtr,
	    count,
	    walprop_pg_get_timeline_id());
	if(res == NEON_WALREAD_SUCCESS)
	{
	    if (NeonWALReaderEvents(wal_reader) == 0 && wait_event_set != NULL)
	    {
		FreeWaitEventSet(wait_event_set);
		wait_event_set = NULL;
	    }
	    xlogreader->seg.ws_tli = NeonWALReaderGetSegment(wal_reader)->ws_tli;
	    xlogreader->seg.ws_segno = NeonWALReaderGetSegment(wal_reader)->ws_segno;
	    xlogreader->seg.ws_file = NeonWALReaderGetSegment(wal_reader)->ws_file;
	    return count;
	}
	if(res == NEON_WALREAD_ERROR)
	{
	    return -1;
	}

	{
	    pgsocket sock = NeonWALReaderSocket(wal_reader);
	    WaitEvent event;
	    long timeout_ms = 1000;
                
	    if (wait_event_set == NULL)
	    {
		wait_event_set = CreateWaitEventSet(TopMemoryContext, 3);
		AddWaitEventToSet(wait_event_set, WL_LATCH_SET, PGINVALID_SOCKET, MyLatch, NULL);
		AddWaitEventToSet(wait_event_set, WL_EXIT_ON_PM_DEATH, PGINVALID_SOCKET, NULL, NULL);
		AddWaitEventToSet(wait_event_set, WL_SOCKET_WRITEABLE | WL_SOCKET_READABLE, sock, NULL, NULL);
	    }
	    WaitEventSetWait(wait_event_set, timeout_ms, &event, 1, WAIT_EVENT_WAL_SENDER_MAIN);
	}
    }
}

static void NeonWALReadSegmentOpen(XLogReaderState *xlogreader, XLogSegNo nextSegNo, TimeLineID *tli_p)
{
    neon_wal_segment_open(wal_reader, nextSegNo, tli_p);
    xlogreader->seg.ws_file = NeonWALReaderGetSegment(wal_reader)->ws_file;
}

static void NeonWALReadSegmentClose(XLogReaderState *xlogreader)
{
    neon_wal_segment_close(wal_reader);
    xlogreader->seg.ws_file = NeonWALReaderGetSegment(wal_reader)->ws_file;
}

void NeonOnDemandXLogReaderRoutines(XLogReaderRoutine *xlr)
{
    if(!wal_reader)
    {
        XLogRecPtr epochStartLsn = *(volatile XLogRecPtr *)&GetWalpropShmemState()->propEpochStartLsn;
        wal_reader = NeonWALReaderAllocate(wal_segment_size, epochStartLsn, "[walsender] ");
    }
    xlr->page_read = NeonWALPageRead;
    xlr->segment_open = NeonWALReadSegmentOpen;
    xlr->segment_close = NeonWALReadSegmentClose;
}

