import redis, os, json

_redis = redis.Redis(
    host=os.getenv("REDIS_HOST", "localhost"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    decode_responses=True,
)

def get_cached(category: str):
    data = _redis.get(category)
    return json.loads(data) if data else None

def set_cached(category: str, results: dict, ttl: int = 86400):
    _redis.setex(category, ttl, json.dumps(results, ensure_ascii=False))
