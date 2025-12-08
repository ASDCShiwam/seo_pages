import asyncio
import time
from dataclasses import dataclass, field
from urllib.parse import urlparse

import aiohttp
from loguru import logger

from .config import USER_AGENT


@dataclass
class RobotsRules:
    allows: list[str] = field(default_factory=list)
    disallows: list[str] = field(default_factory=list)
    crawl_delay: float | None = None

    def is_allowed(self, path: str) -> bool:
        def longest_prefix_length(path_value: str, patterns: list[str]) -> int:
            matches = [len(rule) for rule in patterns if rule and path_value.startswith(rule)]
            return max(matches, default=-1)

        allow_len = longest_prefix_length(path, self.allows)
        disallow_len = longest_prefix_length(path, self.disallows)

        if disallow_len == -1:
            return True
        if allow_len == -1:
            return False
        if allow_len > disallow_len:
            return True
        if allow_len < disallow_len:
            return False
        return True


class RobotsManager:
    def __init__(self, user_agent: str = USER_AGENT):
        self.user_agent = user_agent
        self.rules: dict[str, RobotsRules] = {}
        self.fetch_locks: dict[str, asyncio.Lock] = {}
        self.delay_locks: dict[str, asyncio.Lock] = {}
        self.next_allowed: dict[str, float] = {}

    def _domain_key(self, url: str) -> str:
        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.netloc}"

    def _path(self, url: str) -> str:
        parsed = urlparse(url)
        path = parsed.path or "/"
        if parsed.query:
            path = f"{path}?{parsed.query}"
        return path

    def _get_lock(self, lock_map: dict[str, asyncio.Lock], key: str) -> asyncio.Lock:
        if key not in lock_map:
            lock_map[key] = asyncio.Lock()
        return lock_map[key]

    def _parse_robots(self, content: str) -> RobotsRules:
        rules_map: dict[str, RobotsRules] = {}
        current_agents: list[str] = []
        last_key: str | None = None

        for raw_line in content.splitlines():
            line = raw_line.split("#", 1)[0].strip()
            if not line or ":" not in line:
                continue

            key, value = [part.strip() for part in line.split(":", 1)]
            key_lower = key.lower()

            if key_lower == "user-agent":
                agent = value
                if last_key == "user-agent":
                    current_agents.append(agent)
                else:
                    current_agents = [agent]
                rules_map.setdefault(agent, RobotsRules())
            elif key_lower in {"allow", "disallow"}:
                if not current_agents:
                    continue
                for agent in current_agents:
                    rules_map.setdefault(agent, RobotsRules())
                    target_list = (
                        rules_map[agent].allows
                        if key_lower == "allow"
                        else rules_map[agent].disallows
                    )
                    if value:
                        target_list.append(value)
            elif key_lower == "crawl-delay":
                if not current_agents:
                    continue
                try:
                    delay = float(value)
                except ValueError:
                    continue
                for agent in current_agents:
                    rules_map.setdefault(agent, RobotsRules())
                    rules_map[agent].crawl_delay = delay

            last_key = key_lower

        user_agent_lower = self.user_agent.lower()
        for agent, parsed_rules in rules_map.items():
            if agent.lower() == user_agent_lower:
                return parsed_rules

        return rules_map.get("*", RobotsRules())

    async def ensure_rules(self, session: aiohttp.ClientSession, url: str) -> RobotsRules:
        domain = self._domain_key(url)
        if domain in self.rules:
            return self.rules[domain]

        lock = self._get_lock(self.fetch_locks, domain)
        async with lock:
            if domain in self.rules:
                return self.rules[domain]

            robots_url = f"{domain}/robots.txt"
            logger.info(f"Fetching robots.txt for {domain}")
            try:
                async with session.get(robots_url, allow_redirects=True) as resp:
                    if resp.status == 200:
                        content = await resp.text()
                        parsed_rules = self._parse_robots(content)
                        self.rules[domain] = parsed_rules
                        return parsed_rules
                    logger.info(
                        f"robots.txt for {domain} returned status {resp.status}; allowing by default"
                    )
            except Exception as ex:
                logger.warning(f"Failed to fetch robots.txt for {domain}: {ex}; allowing by default")

            self.rules[domain] = RobotsRules()
            return self.rules[domain]

    def is_allowed(self, url: str) -> bool:
        domain = self._domain_key(url)
        rules = self.rules.get(domain, RobotsRules())
        path = self._path(url)
        return rules.is_allowed(path)

    async def wait_for_crawl_delay(self, url: str) -> None:
        domain = self._domain_key(url)
        rules = self.rules.get(domain, RobotsRules())
        delay = rules.crawl_delay or 0
        if delay <= 0:
            return

        lock = self._get_lock(self.delay_locks, domain)
        async with lock:
            now = time.monotonic()
            next_allowed_time = self.next_allowed.get(domain, now)
            wait_time = max(0.0, next_allowed_time - now)
            if wait_time > 0:
                logger.info(
                    f"Waiting {wait_time:.2f}s due to crawl-delay for domain {domain}"
                )
                await asyncio.sleep(wait_time)

            self.next_allowed[domain] = time.monotonic() + delay
