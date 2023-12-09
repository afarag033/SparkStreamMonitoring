import csv
import io
import json
from enum import Enum
from typing import List, Dict
from dateutil import parser, tz

class EventTypes(Enum):
    QUERY_START = "START"
    QUERY_PROGRESS = "PROGRESS"
    QUERY_TERMINATION = "TERMINATION"

class EventsFormatter():
    def __init__(self, headers: Dict[EventTypes, List]):
        '''
        Constructor for the EventsFormatter class.
        
        Parameters
        ----------
        headers : Dict[EventTypes, List[str]]
            Dictionary of EventTypes as Keys and list of event columns to be formatted.
        '''
        self._headers = headers
        self._local_zone = tz.tzlocal()
        self.output = io.StringIO()
        self.writer = csv.writer(self.output, quoting=csv.QUOTE_NONNUMERIC)

    def format_header(self, event_type: EventTypes):
        self.writer.writerow(['_'.join(key) if isinstance(key, list) else key for key in self._headers[event_type]])
        data = self.output.getvalue()
        self.output.truncate(0)
        self.output.seek(0)
        return data.strip()

    def _read_progress(self, event):
        event_data = json.loads(event.progress.json)
        return [self._localize_timestamp(event_data.get(key)) if key == 'timestamp'
                # for some reason, batchDuration, observedMetrics and eventTime are not present in progress parsed json so, getting them directly from progress object
                else event.progress.batchDuration if key == 'batchDuration'  
                else event.progress.observedMetrics if key == 'observedMetrics'  
                else event.progress.eventTime if key == 'eventTime'  
                else self._deep_get(event_data, key) 
                for key in self._headers[EventTypes.QUERY_PROGRESS]] 

    def _localize_timestamp(self, timestamp):
        '''
        Localize timestamps that are in ISO8601 format, i.e. UTC timestamps.

        Parameters
        ----------
        timestamp: str
            "2016-12-25T20:54:20.827Z" timestamp in ISO8601 format.
        '''
        return parser.parse(timestamp).astimezone(self._local_zone).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] 

    def _deep_get(self, _dict, keys, default=None):
        if isinstance(keys, str):
            return _dict.get(keys, default)
        for key in keys:
            if isinstance(_dict, dict):
                _dict = _dict.get(key, default)
            else:
                return default
        return _dict
    
    def format(self, event_type: EventTypes, event):
        """
        Called when a StreamingEvent needs to be formatted to one csv row.

        Parameters
        ----------
        event: can be one of the classes:
                    class:`pyspark.sql.streaming.listener.QueryStartedEvent`
                    class:`pyspark.sql.streaming.listener.QueryProgressEvent`
                    class:`pyspark.sql.streaming.listener.QueryTerminatedEvent`
        """
        if event_type == EventTypes.QUERY_START:
            self.writer.writerow([event.id, event.runId, event.name, self._localize_timestamp(event.timestamp)])
        elif event_type == EventTypes.QUERY_PROGRESS:
            self.writer.writerow(self._read_progress(event))
        elif event_type == EventTypes.QUERY_TERMINATION:
            self.writer.writerow([event.id, event.runId, event.exception])
        else:
            raise TypeError("Only instances from QueryStartedEvent, QueryProgressEvent and QueryTerminatedEvent are allowed")
        
        data = self.output.getvalue()
        self.output.truncate(0)
        self.output.seek(0)
        return data.strip()