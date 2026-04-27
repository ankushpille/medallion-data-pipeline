"""
Detector Registry
─────────────────
Central registry for all detectors. Add new detectors here — no other
file needs to change.
"""
from __future__ import annotations

from typing import Dict, List, Type

from engine.detectors.base import BaseDetector
from engine.detectors.framework_detector import FrameworkDetector
from engine.detectors.source_detector import SourceDetector
from engine.detectors.ingestion_detector import IngestionDetector
from engine.detectors.dq_detector import DQDetector

# Ordered list — determines execution order
_REGISTERED: List[Type[BaseDetector]] = [
    FrameworkDetector,
    SourceDetector,
    IngestionDetector,
    DQDetector,
]

# Instantiated singletons (detectors are stateless, so one instance is fine)
_INSTANCES: Dict[str, BaseDetector] = {
    cls.name: cls() for cls in _REGISTERED
}


def get_all_detectors() -> List[BaseDetector]:
    """Return all registered detector instances in order."""
    return [_INSTANCES[cls.name] for cls in _REGISTERED]


def get_detector(name: str) -> BaseDetector | None:
    """Fetch a specific detector by name."""
    return _INSTANCES.get(name)


def register_detector(detector_cls: Type[BaseDetector]) -> None:
    """
    Dynamically register a new detector at runtime.

    Usage:
        from engine.registry import register_detector
        from my_plugin import MyDetector
        register_detector(MyDetector)
    """
    if detector_cls.name in _INSTANCES:
        raise ValueError(f"Detector '{detector_cls.name}' is already registered.")
    _REGISTERED.append(detector_cls)
    _INSTANCES[detector_cls.name] = detector_cls()


def unregister_detector(name: str) -> None:
    """
    Remove a detector from the registry by name.

    Raises KeyError if no detector with that name is registered.
    Useful for test isolation when dynamically registered detectors
    must be cleaned up between test cases.
    """
    if name not in _INSTANCES:
        raise KeyError(f"Detector '{name}' is not registered.")
    del _INSTANCES[name]
    # Remove the corresponding class from _REGISTERED
    for i, cls in enumerate(_REGISTERED):
        if cls.name == name:
            del _REGISTERED[i]
            break
