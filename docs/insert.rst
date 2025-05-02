Insert / Add
============

Inserting objects into the memory store uses standard SQLAlchemy syntax.

.. code-block:: python

  # orm style
  session.add(Item(id=1, name="Hello"))

  session.add_all([
    Item(id=1, name="Hello"),
    Item(id=2, name="World")
  ])

  # core style
  result = session.execute(insert(Item).values(name="Hello"))
  print(result.rowcount)

  result = session.execute(
      insert(Item),
      [dict(name="Hello"), dict(name="World")]
  )
  print(result.rowcount)

You can also use `session.add_all([...])` for bulk inserts.

Primary Keys
------------

Auto-incrementing primary keys are fully supported for integer columns. You can also manually specify primary keys when needed.

.. code-block:: python

  with SessionFactory() as session:
    session.add(Item(name="foo"))         # Auto-assigned id = 1
    session.add(Item(id=90, name="bar"))  # Manually set id = 90
    session.add(Item(name="foobar"))      # Auto-assigned id = 91
    session.commit()

    items = session.scalars(select(Item)).all()
    results = {item.id: item.name for item in items}
    print(results) # {1: 'foo', 90: 'bar', 91: 'foobar'}

Relationships
-------------

`sqlalchemy-memory` does not implement automatic behavior for relationships. You are responsible for managing relationship fields manually, such as setting foreign keys and related objects.

Note that while the `relationship()` declaration is allowed (and used by the ORM), relationship loading, cascades, and lazy-loading behavior are **not implemented**.

.. code-block:: python

  class Product(Base):
    __tablename__ = "products"

    id = Column(Integer, primary_key=True)
    category_id = Column(Integer, ForeignKey("categories.id"))

    category = relationship("Category")

  with SessionFactory() as session:
    session.add(Product(name="foo", category_id=2, category=category))


Constraints and Validation
--------------------------

Constraints such as unique, nullable, or custom check conditions declared on the model are not enforced by the memory store. No validation errors will be raised for constraint violations.