# =============================================================================
# transforms_v2/registry.py - Primitive Registry
# =============================================================================
# Centralized registry for all transformation primitives.
# Provides lookup, listing, and info retrieval.
# =============================================================================

from __future__ import annotations

from typing import Type

from transforms_v2.types import Primitive, PrimitiveInfo


# Global registry: name -> Primitive class
PRIMITIVE_REGISTRY: dict[str, Type[Primitive]] = {}


def register_primitive(cls: Type[Primitive]) -> Type[Primitive]:
    """
    Decorator to register a primitive class.

    Usage:
        @register_primitive
        class RemoveDuplicates(Primitive):
            ...

    The primitive will be registered under its info().name.
    """
    info = cls.info()
    name = info.name

    if name in PRIMITIVE_REGISTRY:
        raise ValueError(f"Primitive '{name}' is already registered")

    PRIMITIVE_REGISTRY[name] = cls
    return cls


def get_primitive(name: str) -> Type[Primitive] | None:
    """Get a primitive class by name."""
    return PRIMITIVE_REGISTRY.get(name)


def list_primitives(category: str | None = None) -> list[str]:
    """
    List all registered primitive names.

    Args:
        category: If provided, filter by category (e.g., "rows", "columns")

    Returns:
        List of primitive names
    """
    if category is None:
        return list(PRIMITIVE_REGISTRY.keys())

    return [
        name for name, cls in PRIMITIVE_REGISTRY.items()
        if cls.info().category == category
    ]


def get_primitive_info(name: str) -> PrimitiveInfo | None:
    """Get metadata about a primitive."""
    cls = PRIMITIVE_REGISTRY.get(name)
    if cls is None:
        return None
    return cls.info()


def get_all_primitives_info() -> dict[str, PrimitiveInfo]:
    """Get info for all registered primitives."""
    return {name: cls.info() for name, cls in PRIMITIVE_REGISTRY.items()}


def get_test_prompts_for_primitive(name: str) -> list[dict]:
    """
    Get test prompts for a specific primitive.

    Returns list of dicts with prompt, expected_params, description.
    """
    info = get_primitive_info(name)
    if info is None:
        return []

    return [
        {
            "prompt": tp.prompt,
            "expected_params": tp.expected_params,
            "description": tp.description,
        }
        for tp in info.test_prompts
    ]


def get_all_test_prompts() -> list[dict]:
    """
    Get all test prompts from all primitives.

    This is the training dataset for the Strategist.

    Returns:
        List of dicts with primitive_name, prompt, expected_params
    """
    all_prompts = []

    for name, cls in PRIMITIVE_REGISTRY.items():
        info = cls.info()
        for tp in info.test_prompts:
            all_prompts.append({
                "primitive_name": name,
                "category": info.category,
                "prompt": tp.prompt,
                "expected_params": tp.expected_params,
                "description": tp.description,
            })

    return all_prompts


def export_primitives_documentation() -> str:
    """
    Export documentation for all primitives in markdown format.

    Useful for generating docs and for Strategist prompt context.
    """
    lines = ["# ModularData Transform Library v2.0\n"]

    # Group by category
    categories: dict[str, list[str]] = {}
    for name, cls in PRIMITIVE_REGISTRY.items():
        cat = cls.info().category
        if cat not in categories:
            categories[cat] = []
        categories[cat].append(name)

    for category, primitives in sorted(categories.items()):
        lines.append(f"\n## {category.upper()}\n")

        for name in sorted(primitives):
            info = get_primitive_info(name)
            if info is None:
                continue

            lines.append(f"\n### `{name}`\n")
            lines.append(f"{info.description}\n")

            # Parameters
            if info.params:
                lines.append("\n**Parameters:**\n")
                for p in info.params:
                    req = "(required)" if p.required else f"(default: {p.default})"
                    lines.append(f"- `{p.name}`: {p.type} {req} - {p.description}")

            # Test prompts
            if info.test_prompts:
                lines.append("\n**Example prompts:**\n")
                for tp in info.test_prompts[:3]:  # Show first 3
                    lines.append(f"- \"{tp.prompt}\"")

    return "\n".join(lines)
