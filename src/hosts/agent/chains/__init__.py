from .classify import classify_chain
from .rag import rag_chain
from .report import report_chain
from .router import router as router_chain

REGISTRY = {
    "router": router_chain,
    "classify": classify_chain,
    "rag": rag_chain,
    "report": report_chain,
}


def get_chain(name: str):
    return REGISTRY[name]
