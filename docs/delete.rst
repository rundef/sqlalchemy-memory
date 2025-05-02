Delete
======

Deleting objects from the memory store uses standard SQLAlchemy syntax.

.. code-block:: python

    session.delete(obj) # orm style

    stmt = delete(Item).where(Item.id < 3) # core style
    result = session.execute(stmt)
    print(result.rowcount)
