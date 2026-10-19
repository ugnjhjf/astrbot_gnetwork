import asyncio

from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.star import Context, Star, register
from mcstatus import JavaServer

HOST = "10.10.10.3"
PORT = 25564
TIMEOUT = 4


@register("astrbot_plugin_mcstatus", "rokidna", "MC 状态", "0.1.0")
class McStatus(Star):
    def __init__(self, context: Context):
        super().__init__(context)
        self.server = JavaServer(HOST, PORT, timeout=TIMEOUT)
        self.group = None
        self.online = None
        self.misses = 0
        self.silent = False
        self.task = asyncio.create_task(self.watch())

    async def terminate(self):
        self.task.cancel()

    async def probe(self):
        status = await self.server.async_status()
        try:
            query = await self.server.async_query()
            names = list(query.players.names)
        except Exception:
            names = [p.name for p in (status.players.sample or [])]
        return status.players.online, status.players.max, names, round(status.latency)

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
        try:
            online, max_, _, latency = await self.probe()
            yield event.plain_result(f"在线\n人数 {online}/{max_}\n延迟 {latency} ms")
        except Exception:
            yield event.plain_result("离线")

    @filter.command("在线玩家")
    async def players_cmd(self, event: AstrMessageEvent):
        try:
            online, max_, names, _ = await self.probe()
            body = "、".join(names) if names else "名单为空"
            yield event.plain_result(f"在线 {online}/{max_}\n{body}")
        except Exception:
            yield event.plain_result("服务器离线，没有玩家名单")

    async def watch(self):
        while True:
            try:
                await self.probe()
                self.misses = 0
                if self.online is False and self.group and not self.silent:
                    await self.context.send_message(
                        self.group, MessageChain().message("服务器已恢复")
                    )
                self.online = True
            except Exception:
                self.misses += 1
                if (
                    self.online is True
                    and self.misses >= 2
                    and self.group
                    and not self.silent
                ):
                    await self.context.send_message(
                        self.group, MessageChain().message("服务器已停止或崩溃")
                    )
                    self.online = False
            await asyncio.sleep(30)
