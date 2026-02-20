from fastapi import APIRouter

from app.api.v1.routes.analyze_feed import router as analyze_feed_router
from app.api.v1.routes.health import router as health_router
from app.api.v1.routes.messages import router as messages_router
from app.api.v1.routes.metrics import router as metrics_router

api_v1_router = APIRouter()
api_v1_router.include_router(analyze_feed_router)
api_v1_router.include_router(messages_router)
api_v1_router.include_router(health_router)
api_v1_router.include_router(metrics_router)
