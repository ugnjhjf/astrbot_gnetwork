import asyncio
import time
from dataclasses import dataclass, field

from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.star import Context, Star, register
from mcstatus import JavaServer

HOST = "10.10.10.3"
PORT = 25564
STATUS_TIMEOUT = 2.0
QUERY_TIMEOUT = 1.0
POLL_INTERVAL = 20
FAIL_THRESHOLD = 2


@dataclass
class Snapshot:
    online: bool = False
    players: int = 0
    max_players: int = 0
    names: list[str] = field(default_factory=list)
    latency: int = 0
    ts: float = 0.0

    def age(self) -> float:
        return max(0.0, time.time() - self.ts)


@register("astrbot_plugin_mcstatus", "rokidna", "MC 状态", "0.2.0")
class McStatus(Star):
    def __init__(self, context: Context):
        super().__init__(context)
        self.server = JavaServer(HOST, PORT, timeout=STATUS_TIMEOUT)
        self.snap = Snapshot()
        self.group = None
        self.silent = False
        self.misses = 0
        self.was_online = None
        self.task = asyncio.create_task(self.watch())

    async def terminate(self):
        self.task.cancel()

    async def _read(self) -> Snapshot:
        status = await asyncio.wait_for(
            self.server.async_status(), STATUS_TIMEOUT
        )
        names = []
        try:
            query = await asyncio.wait_for(
                self.server.async_query(), QUERY_TIMEOUT
            )
            names = list(query.players.names)
        except Exception:
            names = [p.name for p in (status.players.sample or [])]
        return Snapshot(
            online=True,
            players=status.players.online,
            max_players=status.players.max,
            names=names,
            latency=round(status.latency),
            ts=time.time(),
        )

    async def _refresh(self):
        self.snap = await self._read()

    async def watch(self):
        while True:
            try:
                await self._refresh()
                self.misses = 0
                if self.was_online is False and self.group and not self.silent:
                    await self._notify("服务器已恢复")
                self.was_online = True
            except Exception:
                self.misses += 1
                if self.misses >= FAIL_THRESHOLD:
                    self.snap = Snapshot(online=False, ts=time.time())
                    if (
                        self.was_online is True
                        and self.group
                        and not self.silent
                    ):
                        await self._notify("服务器已停止或崩溃")
                    self.was_online = False
            await asyncio.sleep(POLL_INTERVAL)

    async def _notify(self, text: str):
        await self.context.send_message(
            self.group, MessageChain().message(text)
        )

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
            yield event.plain_result("离线")
            return
        yield event.plain_result(
            f"在线\n人数 {s.players}/{s.max_players}\n"
            f"延迟 {s.latency} ms\n{int(s.age())}s 前采集"
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
            await self._refresh()
            yield event.plain_result("已刷新")
        except Exception:
            yield event.plain_result("刷新失败，服务器可能离线")
