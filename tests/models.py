from sqlalchemy.orm import declarative_base, mapped_column, Mapped
from sqlalchemy import JSON

Base = declarative_base()

class Item(Base):
    __tablename__ = "items"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column()

    def __repr__(self):
        return f"Item(id={self.id} name={self.name})"


class Product(Base):
    __tablename__ = "products"
    id: Mapped[int] = mapped_column(primary_key=True)
    active: Mapped[bool] = mapped_column(default=True, index=True)
    name: Mapped[str] = mapped_column(nullable=False)
    category: Mapped[str] = mapped_column(nullable=False, index=True)
    data: Mapped[dict] = mapped_column(JSON)

    def __repr__(self):
        return f"Product(id={self.id} name={self.name})"
