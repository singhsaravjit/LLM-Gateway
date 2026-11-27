"""Locust scenarios (owner: Kanad).

Prompt mix is Zipf-like: a few prompts repeat often, most are rare — the
shape real traffic tends to have, and what makes caching interesting.

    pip install -r loadtest/requirements.txt
    locust -f loadtest/locustfile.py --host http://localhost:8080

Experiment knobs (env vars; defaults give the original mix):
    N_PROMPTS=1000      distinct prompts in the pool
    ZIPF_S=1.1          skew of the mix (higher = more repeats, 0 = uniform)
    NO_CACHE=1          send "no_cache": true on every request (cache-off baseline)
    UNIQUE_PROMPTS=1    every request gets a never-seen prompt (0% hit worst case)
"""
import os
import random
import uuid

from locust import HttpUser, between, task

N_PROMPTS = int(os.getenv("N_PROMPTS", "1000"))
ZIPF_S = float(os.getenv("ZIPF_S", "1.1"))
NO_CACHE = os.getenv("NO_CACHE", "").lower() in ("1", "true", "yes")
UNIQUE_PROMPTS = os.getenv("UNIQUE_PROMPTS", "").lower() in ("1", "true", "yes")
WEIGHTS = [1 / (i ** ZIPF_S) for i in range(1, N_PROMPTS + 1)]
PROMPTS = [f"Question #{i}: explain concept {i} in one paragraph." for i in range(N_PROMPTS)]
LONG_SUFFIX = " Provide detailed reasoning, examples and edge cases." * 20


class GatewayUser(HttpUser):
    wait_time = between(0.1, 0.5)

    def on_start(self):
        self.api_key = f"key-{random.randint(1, 10)}"
        self.team = f"team-{random.randint(1, 3)}"

    @task(9)
    def short_prompt(self):
        self._send(random.choices(PROMPTS, weights=WEIGHTS)[0])

    @task(1)
    def long_prompt(self):
        self._send(random.choices(PROMPTS, weights=WEIGHTS)[0] + LONG_SUFFIX)

    def _send(self, content):
        if UNIQUE_PROMPTS:
            content = f"{uuid.uuid4()} {content}"
        body = {"messages": [{"role": "user", "content": content}]}
        if NO_CACHE:
            body["no_cache"] = True
        with self.client.post(
            "/v1/chat/completions",
            json=body,
            headers={"Authorization": f"Bearer {self.api_key}", "X-Team-ID": self.team},
            name="/v1/chat/completions",
            catch_response=True,
        ) as r:
            if r.status_code == 429:
                r.failure("rate limited")
