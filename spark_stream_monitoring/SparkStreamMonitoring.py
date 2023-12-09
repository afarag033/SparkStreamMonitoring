import os
try:
    from pyspark.sql.streaming import StreamingQueryListener
except ImportError as e:
    print("Failed to import required module.\n")
    print(e)
    raise e

from spark_stream_monitoring.StreamLogger import StreamLogger
from spark_stream_monitoring.EventsFormatter import EventTypes

class SparkStreamMonitoring(StreamingQueryListener):
    
    def __init__(self, target_location=''):
        '''
        Constructor for the SparkStreamMonitoring class.

        Parameters
        ----------
        target_location : string
            The target folder where auding logs will be written to.
        '''
        self._logger = StreamLogger(os.path.join(target_location + "streaming_audit"))

    def onQueryStarted(self, event):
        """
        Called when a query is started.

        Parameters
        ----------
        event: :class:`pyspark.sql.streaming.listener.QueryStartedEvent`
            The properties are available as the same as Scala API.

        Notes
        -----
        This is called synchronously with
        meth:`pyspark.sql.streaming.DataStreamWriter.start`,
        that is, ``onQueryStart`` will be called on all listeners before
        ``DataStreamWriter.start()`` returns the corresponding
        :class:`pyspark.sql.streaming.StreamingQuery`.
        Do not block in this method as it will block your query.
        """
        self._logger.log(EventTypes.QUERY_START, event)

    def onQueryProgress(self, event):
        """
        Called when there is some status update (ingestion rate updated, etc.)

        Parameters
        ----------
        event: :class:`pyspark.sql.streaming.listener.QueryProgressEvent`
            The properties are available as the same as Scala API.

        Notes
        -----
        This method is asynchronous. The status in
        :class:`pyspark.sql.streaming.StreamingQuery` will always be
        latest no matter when this method is called. Therefore, the status
        of :class:`pyspark.sql.streaming.StreamingQuery`.
        may be changed before/when you process the event.
        For example, you may find :class:`StreamingQuery`
        is terminated when you are processing `QueryProgressEvent`.
        """
        self._logger.log(EventTypes.QUERY_PROGRESS, event)

    def onQueryTerminated(self, event):
        """
        Called when a query is stopped, with or without error.

        Parameters
        ----------
        event: :class:`pyspark.sql.streaming.listener.QueryTerminatedEvent`
            The properties are available as the same as Scala API.
        """
        self._logger.log(EventTypes.QUERY_TERMINATION, event)
