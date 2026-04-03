#!/usr/bin/env python
"""Process management for per-site background workers.

Three process families are provided:

- :class:`GnrCronHandler` — wraps a single :class:`GnrCron` process and
  monitors it, restarting it if it exits unexpectedly.
- :class:`GnrWorkerPool` — manages a pool of :class:`GnrWorker` processes
  that consume items from a shared multiprocessing queue.
- :class:`GnrDaemonServiceManager` — loads daemon-enabled services from the
  site database and keeps them running.
"""

from __future__ import annotations

import os
import threading
import time
from datetime import datetime
from multiprocessing import Manager, Process, cpu_count, get_logger
from typing import Any


class GnrCronHandler:
    """Owns a single cron subprocess and restarts it if it dies.

    :param parent: The parent object (site register or daemon).
    :param sitename: Genropy site identifier.
    :param batch_pars: Dict of batch parameters; ``interval`` key controls
        how often the cron process ticks (default 60 s).
    :param batch_queue: Shared :class:`multiprocessing.Queue` for task items.
    :param monitor_interval: How often (in seconds) the monitor thread checks
        that the cron process is still alive (default 10 s).
    """

    def __init__(
        self,
        parent: Any,
        sitename: str | None = None,
        interval: int | None = None,
        batch_queue: Any = None,
        batch_pars: dict | None = None,
        monitor_interval: int | None = None,
    ) -> None:
        self.parent = parent
        self.sitename = sitename
        self.batch_pars = batch_pars
        self.batch_queue = batch_queue
        self.interval: int = batch_pars.get("interval", 60)
        self.monitor_interval: int = monitor_interval or 10
        self.monitor_thread: threading.Thread | None = None
        self.monitor_running: bool = False
        self.cron_process: Process | None = None

    def start(self) -> None:
        """Start the cron subprocess and the background monitor thread."""
        self.startCronProcess()
        self.monitor_running = True
        self.monitor_thread = threading.Thread(target=self.monitorCronProcess)
        self.monitor_thread.daemon = True
        self.monitor_thread.start()

    def terminate(self) -> None:
        """Stop the cron subprocess and the monitor thread."""
        if self.cron_process:
            self.cron_process.terminate()
        self.monitor_running = False

    def is_alive(self) -> bool | None:
        if self.cron_process:
            return self.cron_process.is_alive()
        return None

    def startCronProcess(self) -> None:
        self.cron_process = Process(
            name=f"cron_{self.sitename}",
            target=self.runCronProcess,
            args=(self.sitename, self.interval, self.batch_queue),
        )
        self.cron_process.daemon = True
        self.cron_process.start()

    @staticmethod
    def runCronProcess(
        sitename: str | None = None,
        interval: int | None = None,
        batch_queue: Any = None,
        **kwargs: Any,
    ) -> None:
        interval = interval or 60
        cron = GnrCron(
            sitename=sitename, interval=interval, batch_queue=batch_queue, **kwargs
        )
        time.sleep(1)
        cron.start()

    def monitorCronProcess(self) -> None:
        counter = 0
        while self.monitor_running:
            time.sleep(1)
            counter += 1
            if counter % self.monitor_interval:
                continue
            counter = 0
            if self.cron_process and not self.cron_process.is_alive():
                self.startCronProcess()


class GnrWorkerPool:
    """Pool of :class:`GnrWorker` subprocesses sharing a task queue.

    The pool size defaults to ``cpu_count()`` when *batch_pars* contains
    ``processes="auto"``, or to the numeric value of ``processes`` otherwise.

    :param parent: The parent object.
    :param sitename: Genropy site identifier.
    :param batch_queue: Shared :class:`multiprocessing.Queue`.
    :param batch_pars: Configuration dict; ``processes`` key controls pool size.
    :param monitor_interval: How often (in seconds) dead workers are replaced.
    """

    def __init__(
        self,
        parent: Any,
        sitename: str | None = None,
        workers: int | None = None,
        interval: int | None = None,
        loglevel: Any = None,
        batch_queue: Any = None,
        lock: Any = None,
        execution_dict: Any = None,
        monitor_interval: int | None = None,
        batch_pars: dict | None = None,
        **kwargs: Any,
    ) -> None:
        self.parent = parent
        self.sitename = sitename
        self.batch_queue = batch_queue
        # Create our own Manager so we don't depend on the parent having one
        self._manager = Manager()
        self.lock = self._manager.Lock()
        self.execution_dict = self._manager.dict()
        self.logger = get_logger()
        self.batch_pars = batch_pars
        processes = self.batch_pars.get("processes", "auto")
        if processes == "auto":
            processes = cpu_count()
        else:
            processes = int(processes)
        self.gnrworkers: list[Process | None] = [None for _ in range(processes)]
        self.monitor_interval: int = monitor_interval or 10
        self.monitor_thread: threading.Thread | None = None
        self.monitor_running: bool = False

    def terminate(self) -> None:
        """Stop all worker processes and shut down the shared Manager."""
        self.monitor_running = False
        for p in self.gnrworkers:
            if p and p.is_alive():
                p.terminate()
        self._manager.shutdown()

    def is_alive(self) -> bool:
        """Return ``True`` if at least one worker process is running."""
        for p in self.gnrworkers:
            if p and p.is_alive():
                return True
        return False

    def start(self) -> None:
        """Start all worker slots and the monitor thread."""
        for process_number, process in enumerate(self.gnrworkers):
            if not process or not process.is_alive():
                self.gnrworkers[process_number] = self.startWorker(process_number)
        self.monitor_running = True
        monitor_thread = threading.Thread(target=self.monitorGnrWorkers)
        monitor_thread.daemon = True
        monitor_thread.start()

    def startWorker(self, process_number: int) -> Process:
        process = Process(
            name=f"btc_{self.sitename}_{process_number + 1}",
            target=self.runWorker,
            args=(self.sitename, self.batch_queue, self.lock, self.execution_dict),
        )
        process.daemon = True
        process.start()
        return process

    @staticmethod
    def runWorker(
        sitename: str | None = None,
        batch_queue: Any = None,
        lock: Any = None,
        execution_dict: Any = None,
        **kwargs: Any,
    ) -> None:
        worker = GnrWorker(
            sitename=sitename,
            batch_queue=batch_queue,
            lock=lock,
            execution_dict=execution_dict,
            **kwargs,
        )
        time.sleep(1)
        worker.start()

    def monitorGnrWorkers(self) -> None:
        counter = 0
        while self.monitor_running:
            time.sleep(1)
            counter += 1
            if counter % self.monitor_interval:
                continue
            counter = 0
            running_pids: list[int] = []
            for process_number, process in enumerate(self.gnrworkers):
                if not process or not process.is_alive():
                    process = self.startWorker(process_number)
                    self.gnrworkers[process_number] = process
                running_pids.append(process.pid)
            for task_id, pid in list(self.execution_dict.items()):
                if pid not in running_pids:
                    self.execution_dict.pop(task_id, None)


class GnrRemoteProcess:
    """Base class for processes that need a lazily-created :class:`GnrWsgiSite`.

    The site is created on first access and automatically replaced whenever the
    site signals a restart via the ``RESTART_TS`` global store key.
    """

    def __init__(self, sitename: str | None = None, **kwargs: Any) -> None:
        self.sitename = sitename
        self.logger = get_logger()

    def _makeSite(self) -> None:
        from gnr.web.gnrwsgisite import GnrWsgiSite

        self._site = GnrWsgiSite(self.sitename, noclean=True)
        self._site_ts = datetime.now()
        self.logger.debug(f"Created site for PID {os.getpid()}")

    @property
    def site(self) -> Any:
        if not hasattr(self, "_site"):
            self._makeSite()
        else:
            last_start_ts = self._site.register.globalStore().getItem("RESTART_TS")
            if last_start_ts and last_start_ts > self._site_ts:
                self.logger.debug("Site restarted")
                return None
        return self._site


class GnrDaemonServiceManager:
    """Manages daemon-enabled services declared in the site database.

    Services are loaded from ``sys.service`` rows where ``$daemon IS TRUE``.
    Each service runs in its own subprocess and is restarted if it exits.

    :param parent: The parent daemon object.
    :param sitename: Genropy site identifier.
    :param monitor_interval: How often (in seconds) services are checked and reloaded.
    """

    def __init__(
        self,
        parent: Any = None,
        sitename: str | None = None,
        monitor_interval: int | None = None,
    ) -> None:
        self.parent = parent
        self.sitename = sitename
        self._manager = Manager()
        self.services: dict[str, Process] = dict()
        self.services_info: dict[str, dict] = dict()
        self.services_monitor: dict[str, Any] = dict()
        self.monitor_interval: int = monitor_interval or 10
        self.monitor_running: bool = False

    @property
    def site(self) -> Any:
        if not hasattr(self, "_site"):
            from gnr.web.gnrwsgisite import GnrWsgiSite

            self._site = GnrWsgiSite(self.sitename, noclean=True)
        return self._site

    def terminate(self) -> None:
        self.monitor_running = False
        for p in list(self.services.values()):
            if p and p.is_alive():
                p.terminate()
        self._manager.shutdown()

    def is_alive(self) -> bool:
        for p in list(self.services.values()):
            if p and p.is_alive():
                return True
        return False

    def reloadServices(self, service_identifier: str | None = None) -> None:
        def needReload(service: dict) -> bool:
            service_info = (
                self.services_info.get(service["service_identifier"]) or dict()
            )
            return service["__mod_ts"] != service_info.get("__mod_ts")

        where = "$daemon IS TRUE"
        if service_identifier:
            service_identifier = service_identifier.split(",")
            where = f"{where} AND $service_identifier =:service_identifier"
        service_tbl = self.site.db.table("sys.service")
        services = service_tbl.query(
            "$service_identifier,$service_type,$service_name,$__mod_ts,$disabled",
            where=where,
        ).fetch()
        old_services = list(self.services_info.keys()) or service_identifier or []
        old_services = dict([(o, True) for o in old_services])
        for service in services:
            sid = service["service_identifier"]
            old_services.pop(sid, None)
            if needReload(service):
                self.services_info[sid] = dict(service)
                self.updateService(sid)
        for sid in old_services:
            self.services_info.pop(sid, None)
            self.updateService(sid)

    def updateService(self, service_identifier: str) -> None:
        process = self.services.get(service_identifier)
        if process and process.is_alive():
            self.stopService(service_identifier)

    def start(self) -> None:
        """Start the background service monitor thread."""
        self.monitor_running = True
        monitor_thread = threading.Thread(target=self.monitorServices)
        monitor_thread.daemon = True
        monitor_thread.start()

    def stopService(self, service_identifier: str) -> None:
        stop_thread = threading.Thread(
            target=self._stopService, args=(service_identifier,)
        )
        stop_thread.daemon = True
        stop_thread.start()

    def _stopService(self, service_identifier: str) -> None:
        process = self.services.get(service_identifier)
        if process and process.is_alive():
            running = self.services_monitor.get(service_identifier)
            if running:
                running.value = False
            process.join(30)
            if process.is_alive():
                process.terminate()

    def startService(self, service_identifier: str) -> Process | None:
        service = self.services_info.get(service_identifier)
        if not service:
            return None
        service_type = service["service_type"]
        service_name = service["service_name"]
        _running = self.services_monitor.setdefault(
            service_identifier, self._manager.Value("b", True)
        )
        _running.value = True
        process = Process(
            name=f"svc_{self.sitename}_{service_identifier}",
            target=self.runService,
            args=(service_type, service_name, _running),
        )
        process.daemon = True
        process.start()
        return process

    def runService(
        self, service_type: str, service_name: str, _running: Any, **kwargs: Any
    ) -> None:
        service = GnrDaemonService(
            site=self.site,
            service_type=service_type,
            service_name=service_name,
            _running=_running,
            **kwargs,
        )
        time.sleep(1)
        service.start()

    def monitorServices(self) -> None:
        counter = 0
        while self.monitor_running:
            time.sleep(1)
            counter += 1
            if counter % self.monitor_interval:
                continue
            self.reloadServices()
            counter = 0
            for service_identifier, service in list(self.services_info.items()):
                process = self.services.get(service_identifier)
                if service["disabled"]:
                    continue
                if not process or not process.is_alive():
                    process = self.startService(service_identifier)
                    self.services[service_identifier] = process


class GnrDaemonService:
    """Thin wrapper that resolves and runs a single daemon service.

    :param site: The :class:`GnrWsgiSite` instance.
    :param service_type: Service type identifier (e.g. ``"email"``).
    :param service_name: Service name identifier.
    :param _running: Shared :class:`multiprocessing.Value` flag; the service
        should poll it and exit when set to ``False``.
    """

    def __init__(
        self,
        site: Any = None,
        service_type: str | None = None,
        service_name: str | None = None,
        _running: Any = None,
        **kwargs: Any,
    ) -> None:
        self.site = site
        self.service = self.site.getService(service_type, service_name)
        self._running = _running

    def start(self) -> None:
        if hasattr(self.service, "run"):
            self.service.run(running=self._running)


class GnrWorker(GnrRemoteProcess):
    """Worker subprocess that pops items from a queue and runs them.

    Two item types are supported:

    - ``"batch"`` — runs a table-script batch via ``page.table_script_run``.
    - ``"task"`` — runs a scheduled task via ``sys.task.runTask``.
    """

    def __init__(
        self,
        sitename: str | None = None,
        interval: int | None = None,
        loglevel: Any = None,
        batch_queue: Any = None,
        lock: Any = None,
        execution_dict: Any = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(sitename=sitename)
        self.batch_queue = batch_queue
        self.lock = lock
        self.execution_dict = execution_dict
        self.logger = get_logger()

    def run_batch(self, item_value: dict) -> None:
        page_id = item_value.get("page_id")
        batch_kwargs = item_value.get("batch_kwargs")
        page = self.site.resource_loader.get_page_by_id(page_id)
        self.site.currentPage = page
        page.table_script_run(**batch_kwargs)
        self.site.currentPage = None

    def run_task(self, item_value: dict) -> None:
        task = item_value
        task_id = task["id"]
        page = self.site.dummyPage
        self.site.currentPage = page
        if not task["concurrent"]:
            with self.lock:
                if task_id in self.execution_dict:
                    self.logger.warn(
                        f"Task {task_id} already being executed by PID {self.execution_dict[task_id]} and not marked as concurrent"
                    )
                    return
                else:
                    self.execution_dict[task_id] = os.getpid()
        self.site.db.table("sys.task").runTask(task, page=page)
        self.execution_dict.pop(task_id, None)
        self.site.currentPage = None

    def start(self) -> None:
        queue = self.batch_queue
        self.logger.debug("Starting worker process PID %s", os.getpid())
        while True:
            try:
                item = queue.get()
                if not self.site:
                    queue.put(item)
                    self.logger.debug(f"Worker PID {os.getpid()} will restart")
                    break
                if not item:
                    continue
                item_type = item.get("type")
                item_value = item.get("value")
                handler = getattr(self, f"run_{item_type}", None)
                if handler:
                    handler(item_value)
            except Exception:
                self.logger.exception(
                    "Worker PID %s caught unhandled exception", os.getpid()
                )


class GnrCron(GnrRemoteProcess):
    """Cron subprocess that populates a task queue on a fixed interval.

    :param sitename: Genropy site identifier.
    :param interval: Sleep duration in seconds between tick cycles (default 60).
    :param batch_queue: Shared :class:`multiprocessing.Queue` to push tasks onto.
    :param timespan: Look-ahead window in seconds when fetching next executions.
    """

    def __init__(
        self,
        sitename: str | None = None,
        interval: int | None = None,
        loglevel: Any = None,
        batch_queue: Any = None,
        timespan: int | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(sitename=sitename)
        self.interval = interval
        self.batch_queue = batch_queue
        self._task_queue: list = []
        self.logger = get_logger()
        self.timespan: int = timespan or 60

    def _populateTaskQueue(self) -> None:
        self._task_ts = datetime.now()
        self._task_queue = self.site.db.table("sys.task").getNextExecutions(
            timespan=self.timespan
        )

    @property
    def changesInTask(self) -> bool:
        last_task_ts = self.site.register.globalStore().getItem("TASK_TS")
        return last_task_ts and last_task_ts > self._task_ts

    @property
    def task_queue(self) -> list:
        if not self._task_queue or self.changesInTask:
            self._populateTaskQueue()
        return self._task_queue

    def start(self) -> None:
        self.logger.debug("Starting cron process PID %s", os.getpid())
        while True:
            now = datetime.now()
            if not self.site:
                self.logger.debug(f"Cron PID {os.getpid()} will restart")
                break
            task_queue = self.task_queue
            while task_queue:
                first_task = task_queue[0]
                if first_task["execution_ts"] <= now:
                    first_task = task_queue.pop(0)
                    self.batch_queue.put(dict(type="task", value=first_task["task"]))
                else:
                    break
            time.sleep(self.interval)
