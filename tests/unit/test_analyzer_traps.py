from datetime import datetime, timezone

from app.domain.services.sentiment_service import analyze_messages


def _msg(user_id: str, content: str, hashtag: str = '#x', reactions: int = 1, shares: int = 0, views: int = 10):
    return {
        'user_id': user_id,
        'content': content,
        'timestamp': datetime(2026, 2, 20, 10, 0, tzinfo=timezone.utc),
        'hashtags': [hashtag],
        'reactions': reactions,
        'shares': shares,
        'views': views,
    }


def test_intensifier_orphan_results_in_neutral_distribution():
    result = analyze_messages(messages=[_msg('user_alpha_001', 'muito #x')], time_window_minutes=30)
    assert result['sentiment_distribution'] == {'positive': 0.0, 'negative': 0.0, 'neutral': 100.0}


def test_double_negation_turns_positive():
    result = analyze_messages(messages=[_msg('user_alpha_001', 'não não gostei #x')], time_window_minutes=30)
    assert result['sentiment_distribution']['positive'] == 100.0


def test_case_insensitive_mbras_flag():
    result = analyze_messages(messages=[_msg('user_MBRAS_007', 'bom #x')], time_window_minutes=30)
    assert result['flags']['mbras_employee'] is True


def test_special_pattern_with_42_unicode_chars_and_mbras():
    content = 'mbras ' + ('á' * 36)
    assert len(content) == 42
    result = analyze_messages(messages=[_msg('user_alpha_001', content)], time_window_minutes=30)
    assert result['flags']['special_pattern'] is True


def test_unicode_trap_user_cafe_has_special_followers_count():
    result = analyze_messages(messages=[_msg('user_café_001', 'bom #x')], time_window_minutes=30)
    ranking = result['influence_ranking'][0]
    assert ranking['followers'] == 4242


def test_len_13_user_id_trap_followers_count():
    result = analyze_messages(messages=[_msg('user_12345678', 'bom #x')], time_window_minutes=30)
    ranking = result['influence_ranking'][0]
    assert len(ranking['user_id']) == 13
    assert ranking['followers'] == 233


def test_prime_suffix_trap_followers_count():
    result = analyze_messages(messages=[_msg('user_algo_prime', 'bom #x')], time_window_minutes=30)
    ranking = result['influence_ranking'][0]
    assert ranking['followers'] == 7919


def test_golden_ratio_adjustment_applies_when_interactions_multiple_of_seven():
    result = analyze_messages(
        messages=[_msg('user_alpha_001', 'bom #x', reactions=4, shares=3, views=10)],
        time_window_minutes=30,
    )
    ranking = result['influence_ranking'][0]
    assert ranking['engagement_rate'] > 0.7


def test_trending_positive_hashtag_ranks_above_negative():
    messages = [
        _msg('user_alpha_001', 'adorei #pos', hashtag='#pos'),
        _msg('user_alpha_002', 'ruim #neg', hashtag='#neg'),
    ]
    result = analyze_messages(messages=messages, time_window_minutes=30)
    assert result['trending_topics'][0] == '#pos'
