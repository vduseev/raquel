import dataclasses
from types import UnionType
from typing import Any, get_origin

from typing_extensions import is_typeddict


def to_json(obj: Any) -> Any:
    """Turn a payload object into a value that JSON can serialize.

    Used as the ``default`` hook of ``json.dumps()``, so it is only called
    for objects that JSON can't serialize natively, including nested ones.
    """
    if hasattr(obj, "model_dump"):  # Pydantic model
        return obj.model_dump(mode="json")
    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        return dataclasses.asdict(obj)
    if hasattr(obj, "__dict__"):
        return vars(obj)
    raise TypeError(
        f"Object of type {type(obj).__name__} is not JSON serializable"
    )


def payload_class(payload_type: Any) -> type | None:
    """Get the class that payloads of the given type are built with.

    Type arguments only matter to type checkers, so ``list[int]`` payloads
    are built as a ``list``. Returns ``None`` when the payload should be
    left as is, which is the case for ``None`` and ``Any``.
    """
    if payload_type is None or payload_type is Any:
        return None
    cls = get_origin(payload_type) or payload_type
    if not isinstance(cls, type) or cls is UnionType:
        raise TypeError(
            f"Payload type must be a class, got {payload_type!r} instead"
        )
    return cls


def load_payload(data: Any, cls: type) -> Any:
    """Build a payload object of the given class from its JSON value.

    Pydantic models are validated with ``model_validate()``. Any other
    class is called with the fields of a JSON object as keyword arguments,
    or with any other JSON value as a single argument.
    """
    if is_typeddict(cls) or isinstance(data, cls):
        return data
    model_validate = getattr(cls, "model_validate", None)
    if model_validate:  # Pydantic model
        return model_validate(data)
    if data is None:
        raise ValueError(f"Expected a {cls.__name__} payload, got none")
    if isinstance(data, dict):
        return cls(**data)
    return cls(data)
