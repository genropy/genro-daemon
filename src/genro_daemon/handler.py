import os.path
from multiprocessing import Process

# to be updated when the namespace rearrange for pathresolver
# hit master/release
from gnr.app.gnrdeploy import PathResolver
from gnr.core.gnrlang import gnrImport
from gnr.web import logger

from . import metrics
from .ars import Ars
from .client import GnrDaemonClient
from .exceptions import GnrDaemonMethodNotFound, GnrDaemonProtoError
from .siteregister import GnrSiteRegister
from .storage import get_backend
from .utils import load_daemon_options


class GnrDaemon(Ars):
    """Single-port daemon that hosts all site registers in-process.

    Every site is identified by its *sitename* namespace.  Clients declare
    which site they belong to by including ``_sitename`` in their request
    kwargs (injected automatically by :class:`~genro_daemon.client.GnrDaemonClient`
    when constructed with ``sitename=…``).

    Request routing
    ---------------
    - If ``_sitename`` is present **and** the named :class:`GnrSiteRegister`
      has a method with that name → dispatched to the register.
    - Otherwise → dispatched to the daemon itself (``ping``, ``stop``,
      ``addSiteRegister``, …).
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._siteregisters = {}  # sitename -> GnrSiteRegister
        self._service_processes = {}  # sitename -> {name -> Process}
        self._options = {}

    # ------------------------------------------------------------------
    # Startup
    # ------------------------------------------------------------------

    def do_start(self, *args, **kwargs):
        self._options = kwargs  # kept for get_backend() calls in addSiteRegister
        metrics_port = int(
            os.environ.get("GNR_DAEMON_METRICS_PORT", kwargs.get("metrics_port") or 0)
        )
        if metrics_port:
            metrics.setup(metrics_port)
        super().do_start(*args, **kwargs)

    # ------------------------------------------------------------------
    # Request routing (overrides Ars._req_parse)
    # ------------------------------------------------------------------

    def _req_parse(self, req):
        if req[0] != self.REQ or len(req) != 5:
            raise GnrDaemonProtoError("Invalid protocol")
        request_type, call_id, method_name, args, kwargs = req
        sitename = kwargs.pop("_sitename", None)

        if method_name.startswith("_"):
            raise GnrDaemonMethodNotFound(
                f"No such method '{method_name}': private methods are not callable"
            )

        # If a sitename is given, check whether the method is explicitly defined on
        # the register class (MRO walk, excluding __getattr__ magic).  This lets
        # lifecycle methods like on_site_stop reach the right register while daemon
        # methods (ping, addSiteRegister, …) still take priority.
        if sitename:
            register = self._siteregisters.get(sitename)
            if not register and method_name != "addSiteRegister":
                # Auto-create the register on the first request that arrives before
                # the explicit addSiteRegister call (e.g. after a daemon restart).
                # With a persistent backend (Redis) this transparently restores all
                # state; with an in-memory backend the register starts empty so the
                # client receives None/empty responses rather than an error.
                logger.info(
                    "Auto-creating site register for %r on first request", sitename
                )
                self.addSiteRegister(sitename)
                register = self._siteregisters.get(sitename)
            if register:
                for cls in type(register).__mro__:
                    if method_name in cls.__dict__:
                        return (
                            call_id,
                            getattr(register, method_name),
                            args,
                            kwargs,
                            method_name,
                            sitename,
                        )

        # Daemon-level methods (addSiteRegister, ping, stop, echo, …)
        daemon_method = getattr(self, method_name, None)
        if daemon_method:
            return call_id, daemon_method, args, kwargs, method_name, ""

        # Last resort: route to register via __getattr__ (handles register_name
        # dispatch for methods like notifyDbEvents that live on a sub-register).
        if sitename:
            register = self._siteregisters.get(sitename)
            if register:
                method = getattr(register, method_name, None)
                if method:
                    return call_id, method, args, kwargs, method_name, sitename

        # Final fallback: no sitename was provided (client did not inject
        # _sitename) and no daemon-level method matched.  Try all registered
        # site registers so that early-init calls like setConfiguration()
        # still reach the right register even when the client omits _sitename.
        if not sitename and self._siteregisters:
            if len(self._siteregisters) > 1:
                logger.warning(
                    "Routing '%s' without sitename across %d site registers; "
                    "using first match",
                    method_name,
                    len(self._siteregisters),
                )
            for register in self._siteregisters.values():
                method = getattr(register, method_name, None)
                if method:
                    return call_id, method, args, kwargs, method_name, ""

        site_ctx = f" (site={sitename!r})" if sitename else ""
        raise GnrDaemonMethodNotFound(
            f"No such method '{method_name}'{site_ctx}: not found on daemon or any site register"
        )

    # ------------------------------------------------------------------
    # Daemon-level protocol methods
    # ------------------------------------------------------------------

    def on_site_stop(self, **kwargs):
        """Fallback for on_site_stop calls that arrive without a sitename."""
        logger.warning("on_site_stop called without sitename; ignoring")

    def echo(self, payload):
        return payload

    def ping(self, **kwargs):
        return "pong"

    def addSiteRegister(self, sitename, storage_path=None, autorestore=False, **kwargs):
        """Create a :class:`GnrSiteRegister` for *sitename* if not already present."""
        if sitename in self._siteregisters:
            register = self._siteregisters[sitename]
            if storage_path and not register.storage_path:
                register.storage_path = storage_path
                if autorestore and os.path.exists(storage_path):
                    register.load()
            logger.debug(f"Site >{sitename}< already registered")
            return
        backend = get_backend(self._options, sitename=sitename)
        logger.info(
            "Site register backend for %r: %s", sitename, type(backend).__name__
        )
        register = GnrSiteRegister(
            self,
            sitename=sitename,
            storage_path=storage_path,
            backend=backend,
        )
        register.setConfiguration()
        if autorestore and storage_path and os.path.exists(storage_path):
            register.load()
        self._siteregisters[sitename] = register
        logger.info(f"Site register created for {sitename!r}")
        m = metrics.get()
        if m:
            m.sites_total.inc()
        self._start_site_processes(sitename)

    def getSite(self, sitename=None, **kwargs):
        """Return status dict for *sitename*, or None if unknown."""
        if sitename in self._siteregisters:
            return {"status": "ready", "sitename": sitename}
        return None

    def siteRegisters(self, **kwargs):
        result = []
        for sitename in self._siteregisters:
            result.append(
                (
                    sitename,
                    {
                        "sitename": sitename,
                        "is_alive": True,
                        "register_uri": sitename,
                    },
                )
            )
        return result

    def stop(self, saveStatus=False, **kwargs):
        self._stop_all_registers(saveStatus=saveStatus)
        super().stop(reason=kwargs.get("reason", "direct call"))

    def restart(self, **kwargs):
        self.stop(saveStatus=True)

    def siteregister_stop(self, sitename=None, saveStatus=False, **kwargs):
        if sitename == "*":
            sitelist = list(self._siteregisters.keys())
        elif isinstance(sitename, str):
            sitelist = sitename.split(",")
        else:
            sitelist = list(sitename)
        m = metrics.get()
        result = {}
        for k in sitelist:
            register = self._siteregisters.pop(k, None)
            if register is None:
                continue
            if saveStatus and register.storage_path:
                try:
                    register.dump()
                except Exception as e:
                    logger.error(f"Failed to dump register {k!r}: {e}")
            for proc in self._service_processes.pop(k, {}).values():
                if proc and proc.is_alive():
                    proc.terminate()
            if m:
                m.sites_total.dec()
            result[k] = {"sitename": k}
        return result

    def restartServiceDaemon(self, sitename=None, service_name=None):
        procs = self._service_processes.get(sitename, {})
        proc = procs.get(service_name)
        if proc and proc.is_alive():
            proc.terminate()
        procs[service_name] = self.startServiceDaemon(
            sitename, service_name=service_name
        )

    def startServiceDaemon(self, sitename, service_name=None):
        p = PathResolver()
        siteconfig = p.get_siteconfig(sitename)
        services = siteconfig.get("services", None)
        service_attr = services.getAttr(service_name)
        pkg, pathlib = service_attr["daemon"].split(":")
        service_path = os.path.join(p.package_name_to_path(pkg), "lib", f"{pathlib}.py")
        m = gnrImport(service_path)
        service_attr.update({"sitename": sitename})
        proc = Process(
            name=f"svc_{sitename}_{service_name}",
            target=m.run,
            kwargs=service_attr,
        )
        proc.daemon = True
        proc.start()
        return proc

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _stop_all_registers(self, saveStatus=False):
        self.siteregister_stop("*", saveStatus=saveStatus)

    def _start_site_processes(self, sitename):
        """Spawn external service daemons declared in the site config."""
        if self._hasSysPackageAndIsPrimary(sitename):
            from .services import createTaskScheduler

            ts = Process(
                name=f"ts_{sitename}",
                target=createTaskScheduler,
                kwargs={"sitename": sitename},
            )
            ts.daemon = True
            ts.start()
            self._service_processes.setdefault(sitename, {})["_task_scheduler"] = ts

        p = PathResolver()
        try:
            siteconfig = p.get_siteconfig(sitename)
        except Exception as e:
            logger.warning(
                "Could not load site config for %r, skipping service startup: %s",
                sitename,
                e,
            )
            return
        services = siteconfig.get("services", None)
        if not services:
            return
        for service in services:
            if service.attr.get("daemon"):
                proc = self.startServiceDaemon(sitename, service.label)
                self._service_processes.setdefault(sitename, {})[service.label] = proc

    def _hasSysPackageAndIsPrimary(self, sitename):
        try:
            p = PathResolver()
            siteconfig = p.get_siteconfig(sitename)
            packages = siteconfig.get("packages") or {}
            return "sys" in packages
        except Exception:
            return False


class GnrDaemonProxy:
    """Synchronous client proxy for :class:`GnrDaemon`.

    Accepts the same constructor keyword arguments as the CLI (host, port, …)
    and forwards all attribute access to :class:`~genro_daemon.client.GnrDaemonClient`.
    """

    def __init__(
        self,
        host=None,
        port=None,
        socket=None,
        hmac_key=None,
        compression=None,
        use_environment=False,
        serializer=None,
        **kwargs,
    ):
        options = dict(host=host, port=port)
        if use_environment:
            options = load_daemon_options(options=options)
        _host = options.get("host") or "localhost"
        _port = int(options.get("port") or 40404)
        self._client = GnrDaemonClient(f"gnr://{_host}:{_port}")

    def __getattr__(self, name):
        return getattr(self._client, name)
