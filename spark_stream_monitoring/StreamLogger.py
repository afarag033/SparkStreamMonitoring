import logging
import os
from logging.handlers import TimedRotatingFileHandler
from spark_stream_monitoring.EventsFormatter import EventsFormatter, EventTypes

class TimedRotatingFileWithHeaderHandler(TimedRotatingFileHandler):
    def __init__(self, filename, when='midnight', interval=1, backupCount=0, encoding=None, delay=False, utc=False, atTime=None, errors=None, header=''):
        self.header = header
        super().__init__(filename, when, interval, backupCount, encoding, delay, utc, atTime, errors)
    def _open(self):
        stream = super()._open()
        if self.header and stream.tell() == 0:
            stream.write(self.header + self.terminator)
            stream.flush()
        return stream

class StreamLogger():
    
    def __init__(self, audit_dir=''):
        '''
        Constructor for the StreamLogger class.
        '''
        self._start_events_log_file = os.path.join(audit_dir, "start_events/query_start_events.csv")
        self._progress_events_log_file = os.path.join(audit_dir, "progress_events/query_progress_events.csv")
        self._termination_events_log_file = os.path.join(audit_dir, "termination_events/query_termination_events.csv")
        
        os.makedirs(os.path.dirname(self._start_events_log_file), exist_ok=True)
        os.makedirs(os.path.dirname(self._progress_events_log_file), exist_ok=True)
        os.makedirs(os.path.dirname(self._termination_events_log_file), exist_ok=True)
        
        self._headers = {
            EventTypes.QUERY_START : ["id", "runId", "name", "timestamp"],
            EventTypes.QUERY_PROGRESS : ["id", "runId", "name", "timestamp", "batchId", "batchDuration", ["durationMs", "addBatch"], 
                                         ["durationMs", "commitOffsets"], ["durationMs", "getBatch"], ["durationMs", "latestOffset"], 
                                         ["durationMs", "queryPlanning"], ["durationMs", "triggerExecution"], ["durationMs", "walCommit"], 
                                         "eventTime", "stateOperators", "sources", ["sink", "description"], ["sink", "numOutputRows"],
                                         "observedMetrics", "numInputRows", "inputRowsPerSecond", "processedRowsPerSecond"],
            EventTypes.QUERY_TERMINATION : ["id", "runId", "exception"]
        }
        
        logging.basicConfig(level=logging.DEBUG, format='%(message)s')
        self._eventFormatter = EventsFormatter(self._headers)
        
        # # create a file handler per event type
        self._start_fh = TimedRotatingFileWithHeaderHandler(self._start_events_log_file, header=self._eventFormatter.format_header(EventTypes.QUERY_START))
        self._progress_fh = TimedRotatingFileWithHeaderHandler(self._progress_events_log_file, header=self._eventFormatter.format_header(EventTypes.QUERY_PROGRESS))
        self._termination_fh = TimedRotatingFileWithHeaderHandler(self._termination_events_log_file, header=self._eventFormatter.format_header(EventTypes.QUERY_TERMINATION))
        
        # # create logger per event type
        self._start_events_logger = logging.getLogger("start_events")
        self._progress_events_logger = logging.getLogger("progress_events")
        self._termination_events_logger = logging.getLogger("termination_events")
        
        # # add each file handler to its associated logger
        self._start_events_logger.addHandler(self._start_fh)
        self._progress_events_logger.addHandler(self._progress_fh)
        self._termination_events_logger.addHandler(self._termination_fh)

    def log(self, event_type: EventTypes, event):
        if event_type == EventTypes.QUERY_START:
            self._start_events_logger.debug(self._eventFormatter.format(event_type, event))
        elif event_type == EventTypes.QUERY_PROGRESS:
            self._progress_events_logger.debug(self._eventFormatter.format(event_type, event))
        elif event_type == EventTypes.QUERY_TERMINATION:
            self._termination_events_logger.debug(self._eventFormatter.format(event_type, event))
        else:
            print("Unsupported Event Type")
