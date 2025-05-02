Update
======

Updates in `sqlalchemy-memory` follow standard SQLAlchemy semantics. You can modify objects directly via the ORM or use `update()` expressions with `session.execute()`.

.. code-block:: python

  with SessionFactory.begin() as session:
      session.add(Item(id=1, name="foo"))
      session.commit()

      item = session.get(Item, 1)
      item.name = "updated"

  with SessionFactory() as session:
      item = session.get(Item, 1)
      print(item.name) # updated

      session.execute(
          update(Item)
          .where(Item.id == 1)
          .values(name="bar")
      )
      session.commit()

      item = session.get(Item, 1)
      print(item.name)  # bar
