import asyncio
import time
from dataclasses import dataclass, field

from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.star import Context, Star, register
from mcstatus import JavaServer

try:
    from astrbot.api import logger
except Exception:  # pragma: no cover - older AstrBot
    import logging

    logger = logging.getLogger("astrbot_plugin_mcstatus")

DEFAULT_ROUTES = (
    "10.10.10.3:25565,"
    "10.10.10.3:25564,"
    "line1.mc.gnetwork.space:25565,"
    "line2.mc.gnetwork.space:25565,"
    "line3.mc.gnetwork.space:25565"
)

DEFAULTS = {
    "routes": DEFAULT_ROUTES,
    "poll_interval": 20,
    "fail_threshold": 2,
    "status_timeout": 2.0,
    "query_timeout": 1.0,
}


@dataclass
class Snapshot:
    online: bool = False
    players: int = 0
    max_players: int = 0
    names: list[str] = field(default_factory=list)
    latency: int = 0
    host: str = ""
    port: int = 0
    error: str = ""
    ts: float = 0.0

    def age(self) -> float:
        return max(0.0, time.time() - self.ts)

    def label(self) -> str:
        return f"{self.host}:{self.port}"


@register("astrbot_plugin_mcstatus", "rokidna", "MC 状态", "0.4.0")
class McStatus(Star):
    def __init__(self, context: Context, config=None):
        super().__init__(context)
        self.config = config or {}
        self.routes = self._routes()
        self.current = None
        self.poll_interval = max(5, int(self._cfg("poll_interval")))
        self.fail_threshold = max(1, int(self._cfg("fail_threshold")))
        self.status_timeout = float(self._cfg("status_timeout"))
        self.query_timeout = float(self._cfg("query_timeout"))
        self.snap = Snapshot()
        self.group = None
        self.silent = False
        self.misses = 0
        self.was_online = None
        self.last_error = ""
        self.task = asyncio.create_task(self.watch())

    def _cfg(self, key):
        try:
            value = self.config.get(key)
        except Exception:
            value = None
        return DEFAULTS[key] if value in (None, "") else value

    @staticmethod
    def _parse_routes(raw, default_port: int = 25565) -> list[tuple[str, int]]:
        items = raw if isinstance(raw, (list, tuple)) else str(raw).split(",")
        routes = []
        for item in items:
            item = str(item).strip()
            if not item:
                continue
            host, _, port = item.partition(":")
            host = host.strip()
            if not host:
                continue
            try:
                port = int(port.strip()) if port.strip() else default_port
            except ValueError:
                continue
            if not 0 < port < 65536:
                continue
            route = (host, port)
            if route not in routes:
                routes.append(route)
        return routes or [("127.0.0.1", default_port)]

    def _routes(self) -> list[tuple[str, int]]:
        try:
            raw = self.config.get("routes")
        except Exception:
            raw = None
        if raw:
            return self._parse_routes(raw)

        # migrate v0.3.0 style config: host + ports (+ fallback_hosts)
        try:
            host = self.config.get("host")
        except Exception:
            host = None
        if not host:
            return self._parse_routes(DEFAULT_ROUTES)

        ports = []
        raw_ports = self.config.get("ports") or 25565
        for token in str(raw_ports).split(","):
            token = token.strip()
            if token.isdigit() and 0 < int(token) < 65536:
                ports.append(int(token))
        ports = ports or [25565]
        routes = [(host, p) for p in ports]

        try:
            fallbacks = self.config.get("fallback_hosts") or ""
        except Exception:
            fallbacks = ""
        for name in str(fallbacks).split(","):
            name = name.strip()
            if name:
                route = (name, ports[0])
                if route not in routes:
                    routes.append(route)
        return routes

    async def terminate(self):
        self.task.cancel()

    async def _read(self, host: str, port: int) -> Snapshot:
        server = JavaServer(host, port, timeout=self.status_timeout)
        status = await asyncio.wait_for(
            server.async_status(), self.status_timeout
        )
        names = [p.name for p in (status.players.sample or [])]
        try:
            query = await asyncio.wait_for(
                server.async_query(), self.query_timeout
            )
            names = list(query.players.names) or names
        except Exception:
            pass
        return Snapshot(
            online=True,
            players=status.players.online,
            max_players=status.players.max,
            names=names,
            latency=round(status.latency),
            host=host,
            port=port,
            ts=time.time(),
        )

    async def _probe(self) -> Snapshot:
        order = list(self.routes)
        if self.current in order:
            order.remove(self.current)
            order.insert(0, self.current)
        errors = []
        for host, port in order:
            try:
                snap = await self._read(host, port)
                if (host, port) != self.current:
                    if self.current is not None:
                        logger.warning(
                            "[mcstatus] 线路切换 "
                            f"{self.current[0]}:{self.current[1]} -> {host}:{port}"
                        )
                    self.current = (host, port)
                return snap
            except Exception as e:
                errors.append(f"{host}:{port}={type(e).__name__}")
        raise ConnectionError("；".join(errors))

    async def watch(self):
        while True:
            try:
                self.snap = await self._probe()
                self.misses = 0
                self.last_error = ""
                if self.was_online is False and self.group and not self.silent:
                    await self._notify("服务器已恢复")
                self.was_online = True
            except Exception as e:
                self.misses += 1
                self.last_error = str(e) or type(e).__name__
                logger.warning(
                    f"[mcstatus] 探测失败（{self.misses}/{self.fail_threshold}）"
                    f" {self.last_error}"
                )
                if self.misses >= self.fail_threshold:
                    host, port = self.current or self.routes[0]
                    self.snap = Snapshot(
                        online=False,
                        host=host,
                        port=port,
                        error=self.last_error,
                        ts=time.time(),
                    )
                    if (
                        self.was_online is True
                        and self.group
                        and not self.silent
                    ):
                        await self._notify("服务器已停止或崩溃")
                    self.was_online = False
            await asyncio.sleep(self.poll_interval)

    async def _notify(self, text: str):
        await self.context.send_message(
            self.group, MessageChain().message(text)
        )

    @staticmethod
    def _brief(error: str, limit: int = 3) -> str:
        parts = [p for p in error.split("；") if p]
        if not parts:
            return "未知"
        if len(parts) > limit:
            return "；".join(parts[:limit]) + f"；+{len(parts) - limit}"
        return "；".join(parts)

    @filter.command("绑定本群")
    async def bind(self, event: AstrMessageEvent):
        self.group = event.unified_msg_origin
        yield event.plain_result("已绑定。离线时会通知这个群。")

    @filter.command("维护")
    async def maint_on(self, event: AstrMessageEvent):
        self.silent = True
        yield event.plain_result("维护模式。重启期间不发崩溃公告。")

    @filter.command("恢复")
    async def maint_off(self, event: AstrMessageEvent):
        self.silent = False
        yield event.plain_result("已退出维护模式。")

    @filter.command("服务器状况")
    async def status_cmd(self, event: AstrMessageEvent):
        s = self.snap
        if not s.ts:
            yield event.plain_result("状态还没采到，稍等几秒。")
            return
        if not s.online:
            yield event.plain_result(
                f"离线\n{int(s.age())}s 前检测\n原因 {self._brief(s.error)}"
            )
            return
        yield event.plain_result(
            f"在线\n人数 {s.players}/{s.max_players}\n"
            f"延迟 {s.latency} ms\n"
            f"线路 {s.label()} · {int(s.age())}s 前采集"
        )

    @filter.command("在线玩家")
    async def players_cmd(self, event: AstrMessageEvent):
        s = self.snap
        if not s.online:
            yield event.plain_result("服务器离线，没有玩家名单")
            return
        body = "、".join(s.names) if s.names else "名单为空"
        yield event.plain_result(f"在线 {s.players}/{s.max_players}\n{body}")

    @filter.command("刷新")
    async def refresh_cmd(self, event: AstrMessageEvent):
        try:
            self.snap = await self._probe()
            self.misses = 0
            yield event.plain_result(f"已刷新（{self.snap.label()}）")
        except Exception as e:
            yield event.plain_result(f"刷新失败：{type(e).__name__}")

    @filter.command("诊断")
    async def diag_cmd(self, event: AstrMessageEvent):
        started = time.time()
        try:
            await self._probe()
            live = f"在线 {int((time.time() - started) * 1000)}ms"
        except Exception as e:
            live = f"失败 {type(e).__name__} {int((time.time() - started) * 1000)}ms"
        s = self.snap
        current = (
            f"{self.current[0]}:{self.current[1]}" if self.current else "未连通"
        )
        yield event.plain_result(
            f"线路 {len(self.routes)} 条\n"
            f"当前 {current}\n"
            f"快照 {'在线' if s.online else '离线'} · "
            f"{int(s.age()) if s.ts else '-'}s 前\n"
            f"连续失败 {self.misses}\n"
            f"最近错误 {self._brief(s.error or self.last_error, 5)}\n"
            f"实时探测 {live}"
        )
