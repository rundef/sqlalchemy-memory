Commit / Rollback
=================

sqlalchemy-memory fully supports transactional behavior, including commit and rollback operations. Changes are staged until committed, and can be safely reverted using rollback().

Commit
------

.. code-block:: python

  with SessionFactory() as session:
      session.add(Item(id=1, name="foo"))
      session.commit()

      item = session.get(Item, 1)
      print(item.name) # foo
      item.name = "updated"
      session.commit()

      print(item.name) # updated

Rollback
--------

Use `rollback()` to undo uncommitted changes:

.. code-block:: python

  with SessionFactory() as session:
      session.add(Item(id=1, name="foo"))
      session.commit()

      item = session.get(Item, 1)
      print(item.name) # foo
      item.name = "updated"
      session.rollback()

      print(item.name) # foo
