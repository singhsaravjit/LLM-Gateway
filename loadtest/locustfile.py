"""Locust scenarios (owner: Kanad).

Prompt mix is Zipf-like: a few prompts repeat often, most are rare — the
shape real traffic tends to have, and what makes caching interesting.

    pip install -r loadtest/requirements.txt
    locust -f loadtest/locustfile.py --host http://localhost:8080
"""
import random

from locust import HttpUser, between, task

N_PROMPTS = 1000
ZIPF_S = 1.1
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
        with self.client.post(
            "/v1/chat/completions",
            json={"messages": [{"role": "user", "content": content}]},
            headers={"Authorization": f"Bearer {self.api_key}", "X-Team-ID": self.team},
            name="/v1/chat/completions",
            catch_response=True,
        ) as r:
            if r.status_code == 429:
                r.failure("rate limited")
