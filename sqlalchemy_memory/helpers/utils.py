from itertools import chain

def _dedup_chain(*streams):
    """
    Lazily merge multiple input iterators, yielding unique items only.

    This function performs a streamed union of all provided iterators,
    ensuring that each item is yielded at most once while preserving order
    across the combined streams.
    """
    seen = set()
    for item in chain(*streams):
        if item not in seen:
            seen.add(item)
            yield item


def chunk_generator(results, *a):
    chunk = [(r,) if not isinstance(r, (list, tuple)) else r for r in results]
    yield chunk
