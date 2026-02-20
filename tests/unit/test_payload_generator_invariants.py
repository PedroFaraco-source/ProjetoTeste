import random

from app.core.config.settings import get_settings
from tests.smoke.run_load import build_valid_payload


def test_valid_payload_generator_enforces_views_invariant_and_fixed_window():
    settings = get_settings()
    rng = random.Random(123)

    for _ in range(30):
        payload = build_valid_payload(rng, settings.app_timezone)
        assert payload['time_window_minutes'] == 30
        for message in payload['messages']:
            assert message['views'] >= (message['reactions'] + message['shares'])
