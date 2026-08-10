import logging
from datetime import datetime, timedelta
from apscheduler.schedulers.blocking import BlockingScheduler


from kafka_excel_sim.message_builder import MessageBuilder
from kafka_excel_sim.message_schedule import MessageSchedule
from kafka_excel_sim.message_sender import MessageSender
from kafka_excel_sim.message import Message

logger: logging.Logger = logging.getLogger(__name__)


class Scheduler:
    """Drives the simulation loop.

    Iterates through an ordered list of MessageSchedule entries, waits
    until each one's offset_seconds has elapsed since simulation start,
    then triggers the build → send pipeline.

    Attributes:
        schedule: Ordered list of MessageSchedule entries.
        builder:  MessageBuilder used to construct each Message.
        sender:   MessageSender used to deliver each Message.
    """

    def __init__(
        self,
        schedule: list[MessageSchedule],
        builder: MessageBuilder,
        sender: MessageSender,
    ) -> None:
        self.schedule = schedule
        self.builder = builder
        self.sender = sender

    def run(self) -> None:
        """Start the simulation.

        Records the current time as t=0, then for each entry:
          1. Sleeps until the entry's offset_seconds has passed.
          2. Calls builder.build() to get a Message.
          3. Calls sender.send() to deliver it.

        Raises:
            Any exception propagated from builder or sender.
        """
        logger.info(f"Starting simulation with {len(self.schedule)} messages")
        
        aps = BlockingScheduler()
        start_time: datetime = datetime.now() + timedelta(seconds=5)
        max_offset = max(entry.offset_seconds for entry in self.schedule)
        shutdown_time = start_time + timedelta(seconds=max_offset + 1)

        logger.debug(f"Simulation start: {start_time}, shutdown: {shutdown_time}")

        aps.add_job(
            func=self._shutdown,
            trigger='date',
            run_date=shutdown_time,
            args = [aps]
        )

        for entry in self.schedule:
            run_at: datetime = start_time + timedelta(seconds=entry.offset_seconds)
            aps.add_job(
                func = self._send,
                trigger = 'date',
                run_date = run_at,
                args = [entry]
            )

        aps.start()

    def _send(self, entry: MessageSchedule):
        logger.info(f"Sending message: offset={entry.offset_seconds}s, type={entry.message_type}")
        message: Message = self.builder.build(entry)
        self.sender.send(message)
        logger.debug(f"Message sent to topic={message.topic}")
    
    def _shutdown(self, scheduler: BlockingScheduler):
        logger.info("Simulation complete, shutting down")
        scheduler.shutdown(wait=False)