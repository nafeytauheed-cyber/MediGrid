"""Single entrypoint. ROLE=api | worker | migrate | seed | tools."""
from . import config, log


def run_api():
    from gunicorn.app.base import BaseApplication

    from .web.app import create_app

    class _App(BaseApplication):
        def load_config(self):
            for k, v in {"bind": "0.0.0.0:8000", "worker_class": "gthread", "workers": 1, "threads": 128,
                         "timeout": 30, "keepalive": 5, "accesslog": None, "graceful_timeout": 3}.items():
                self.cfg.set(k, v)

        def load(self):
            return create_app()

    log.info("api starting", container_url="http://0.0.0.0:8000", via_nginx="http://<host-lan-ip>:8080")
    _App().run()


def main():
    role = config.ROLE
    if role == "api":
        run_api()
    elif role == "worker":
        from .worker.main import main as wmain
        wmain()
    elif role == "migrate":
        from .migrate import main as m
        m()
    elif role == "seed":
        from .seed import main as s
        s()
    else:
        log.info("tools role: run a module with python -m")


if __name__ == "__main__":
    main()
