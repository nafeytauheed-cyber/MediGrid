"""Worker process: four consumer threads, outbox publisher, reaper and cache manager."""
import threading
import time

from .. import db, log, mq
from .consumers import ConsumerThread
from .loops import CacheManager, OutboxPublisher, Reaper


def main():
    db.wait_for_db()
    mq.wait_for_broker()
    threads = [CacheManager(), OutboxPublisher(), Reaper()] + [ConsumerThread(g) for g in mq.GROUPS]
    for t in threads:
        t.start()
    log.info("worker started", threads=[t.name for t in threads])
    while True:
        time.sleep(5)
        dead = [t.name for t in threads if not t.is_alive()]
        if dead:
            log.error("worker thread died; exiting for restart", threads=dead)
            raise SystemExit(1)


if __name__ == "__main__":
    main()
