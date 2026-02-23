"""Ferrite Python SDK — client for Ferrite, a Redis-compatible tiered-storage key-value store."""

from ferrite.client import Ferrite
from ferrite.vector import VectorStore

__all__ = ["Ferrite", "VectorStore"]
__version__ = "0.1.0"
