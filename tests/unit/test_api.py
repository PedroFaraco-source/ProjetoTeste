import os

import pytest
from fastapi.testclient import TestClient

from app.main import create_app
from app.api.v1.dependencies.request_context import get_publisher
from app.core.config.settings import reload_settings


class FailingPublisher:
    def publish_event(self, event, routing_key=None, headers=None):
        raise RuntimeError('falha')


@pytest.fixture()
def client(monkeypatch):
    monkeypatch.setenv('DATABASE_URL', 'sqlite+pysqlite:///:memory:')
    monkeypatch.setenv('ENABLE_RABBIT', '0')
    monkeypatch.setenv('BYPASS_ELASTIC_FOR_TESTS', '1')
    reload_settings()

    app = create_app()
    with TestClient(app) as test_client:
        yield test_client


def _valid_payload():
    return {
        'messages': [
            {
                'user_id': 'user_abc123',
                'content': 'adorei o atendimento #mbras',
                'timestamp': '2026-02-20T10:00:00Z',
                'hashtags': ['#mbras'],
                'reactions': 2,
                'shares': 1,
                'views': 10,
            },
            {
                'user_id': 'user_xyz456',
                'content': 'ruim demais #feedback',
                'timestamp': '2026-02-20T10:01:00Z',
                'hashtags': ['#feedback'],
                'reactions': 1,
                'shares': 0,
                'views': 10,
            },
        ],
        'time_window_minutes': 30,
    }


def test_analyze_feed_returns_analysis_object(client):
    response = client.post('/analyze-feed', json=_valid_payload())

    assert response.status_code == 200
    body = response.json()
    assert 'analysis' in body
    assert 'sentiment_distribution' in body['analysis']
    assert 'engagement_score' in body['analysis']
    assert 'trending_topics' in body['analysis']
    assert 'anomaly_detected' in body['analysis']


def test_analyze_feed_time_window_123_returns_expected_error(client):
    payload = _valid_payload()
    payload['time_window_minutes'] = 123

    response = client.post('/analyze-feed', json=payload)

    assert response.status_code == 422
    body = response.json()
    assert body['error'] == 'Valor de janela temporal não suportado na versão atual'
    assert body['code'] == 'UNSUPPORTED_TIME_WINDOW'
    assert 'correlation_id' in body


def test_analyze_feed_invalid_user_id_returns_400(client):
    payload = _valid_payload()
    payload['messages'][0]['user_id'] = 'invalid-user'

    response = client.post('/analyze-feed', json=payload)

    assert response.status_code == 400
    assert response.json()['code'] == 'INVALID_USER_ID'


def test_analyze_feed_invalid_timestamp_returns_400(client):
    payload = _valid_payload()
    payload['messages'][0]['timestamp'] = '2026-02-20T10:00:00'

    response = client.post('/analyze-feed', json=payload)

    assert response.status_code == 400
    assert response.json()['code'] == 'INVALID_TIMESTAMP'


def test_analyze_feed_invalid_hashtag_returns_400(client):
    payload = _valid_payload()
    payload['messages'][0]['hashtags'] = ['invalida']

    response = client.post('/analyze-feed', json=payload)

    assert response.status_code == 400
    assert response.json()['code'] == 'INVALID_HASHTAGS'


def test_analyze_feed_invalid_views_invariant_returns_400(client):
    payload = _valid_payload()
    payload['messages'][0]['views'] = 1
    payload['messages'][0]['reactions'] = 1
    payload['messages'][0]['shares'] = 1

    response = client.post('/analyze-feed', json=payload)

    assert response.status_code == 400
    assert response.json()['code'] == 'INVALID_VIEWS'


def test_analyze_feed_keeps_200_when_publisher_fails(monkeypatch):
    monkeypatch.setenv('DATABASE_URL', 'sqlite+pysqlite:///:memory:')
    monkeypatch.setenv('ENABLE_RABBIT', '1')
    monkeypatch.setenv('BYPASS_ELASTIC_FOR_TESTS', '1')
    reload_settings()

    app = create_app()
    app.dependency_overrides[get_publisher] = lambda: FailingPublisher()

    with TestClient(app) as client:
        response = client.post('/analyze-feed', json=_valid_payload())

    assert response.status_code == 200
    assert 'analysis' in response.json()


def test_health_and_metrics_endpoints(client):
    health_response = client.get('/health')
    metrics_response = client.get('/metrics')
    docs_response = client.get('/docs')

    assert health_response.status_code == 200
    health_body = health_response.json()
    assert health_body['status'] == 'ok'
    assert 'correlation_id' in health_body

    assert metrics_response.status_code == 200
    assert 'text/plain' in metrics_response.headers['content-type']
    assert 'http_requests_total' in metrics_response.text
    assert 'http_request_duration_seconds' in metrics_response.text
    assert 'rabbit_publish_failures_total' in metrics_response.text
    assert 'ingest_processed_total' in metrics_response.text
    assert 'ingest_failed_total' in metrics_response.text
    assert 'analyze_requests_total' in metrics_response.text
    assert docs_response.status_code == 200


def test_list_messages_returns_paginated_payload(client):
    response = client.get('/messages?page=1&page_size=10')

    assert response.status_code == 200
    body = response.json()
    assert body['page'] == 1
    assert body['page_size'] == 10
    assert body['total'] >= 0
    assert isinstance(body['items'], list)


def test_analyze_feed_batch_accepts_and_returns_202(client):
    payload = {
        'items': [
            {
                'user_id': 'user_batch_a',
                'sentiment_distribution': {'positive': 20, 'negative': 10, 'neutral': 70},
                'engagement_score': 11.2,
                'trending_topics': ['#mbras'],
                'influence_ranking': [],
                'anomaly_detected': False,
                'anomaly_type': None,
                'flags': {
                    'mbras_employee': False,
                    'special_pattern': False,
                    'candidate_awareness': False,
                },
            }
        ]
    }

    response = client.post('/analyze-feed', json=payload)

    assert response.status_code == 202
    body = response.json()
    assert 'batch_id' in body
    assert body['accepted'] == 1
