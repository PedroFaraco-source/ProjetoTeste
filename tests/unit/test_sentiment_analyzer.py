from datetime import datetime, timezone

from app.domain.services.sentiment_service import analyze_messages


def test_analyzer_is_deterministic_for_same_payload():
    messages = [
        {
            'user_id': 'user_alpha123',
            'content': 'adorei o suporte #mbras',
            'timestamp': datetime(2026, 2, 20, 10, 0, tzinfo=timezone.utc),
            'hashtags': ['#mbras'],
            'reactions': 2,
            'shares': 1,
            'views': 10,
        },
        {
            'user_id': 'user_beta456',
            'content': 'ruim demais #feedback',
            'timestamp': datetime(2026, 2, 20, 10, 1, tzinfo=timezone.utc),
            'hashtags': ['#feedback'],
            'reactions': 1,
            'shares': 0,
            'views': 10,
        },
    ]

    first = analyze_messages(messages=messages, time_window_minutes=30)
    second = analyze_messages(messages=messages, time_window_minutes=30)

    assert first == second
    assert 'sentiment_distribution' in first
    assert 'engagement_score' in first
    assert 'trending_topics' in first
    assert 'anomaly_detected' in first
    assert 'flags' in first
